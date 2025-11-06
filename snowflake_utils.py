# snowflake_utils.py (MODIFIED SnowflakeManager)

import snowflake.connector
from config import load_snowflake_env_credentials # NEW: Import for dynamic creds
from datetime import datetime
import json

class SnowflakeManager:
    # MODIFIED: __init__ now takes environment_name to load specific creds
    def __init__(self, environment_name):
        self.environment_name = environment_name
        self.conn = None
        self.connect()

    def connect(self):
        """Establishes a connection to Snowflake for the specified environment."""
        try:
            creds = load_snowflake_env_credentials(self.environment_name)
            self.conn = snowflake.connector.connect(
                user=creds["user"],
                password=creds["password"],
                account=creds["account"],
                warehouse=creds["warehouse"],
                database=creds["database"],
                schema=creds["schema"],
                role=creds["role"]
            )
            print(f"Connected to Snowflake environment: {self.environment_name}.")
        except Exception as e:
            print(f"Error connecting to Snowflake environment '{self.environment_name}': {e}")
            raise

    def disconnect(self):
        """Closes the Snowflake connection."""
        if self.conn:
            self.conn.close()
            print(f"Disconnected from Snowflake environment: {self.environment_name}.")

    def _execute_query(self, query, params=None, fetch_results=True):
        """Helper to execute a query and optionally fetch results."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            if fetch_results:
                return cursor.fetchall()
            else:
                return None
        except Exception as e:
            print(f"Error executing Snowflake query in '{self.environment_name}': {query}\nError: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def check_table_existence_and_get_ddl(self, fqdn, object_type='TABLE'): # Added object_type param
        """
        Checks if an object (TABLE or VIEW) exists and extracts its CREATE DDL from Snowflake.
        FQDN format: DATABASE.SCHEMA.TABLE_NAME
        """
        parts = fqdn.split('.')
        if len(parts) != 3:
            raise ValueError(f"FQDN '{fqdn}' is not in the expected DATABASE.SCHEMA.TABLE format.")
        
        db_name, schema_name, object_name = parts[0], parts[1], parts[2]
        
        # Determine table_type for INFORMATION_SCHEMA query
        if object_type.upper() == 'TABLE':
            table_type_clause = "TABLE_TYPE = 'BASE TABLE'"
        elif object_type.upper() == 'VIEW':
            table_type_clause = "TABLE_TYPE = 'VIEW'"
        else:
            # For other custom types, assume BASE TABLE unless you have a way to query them specifically
            table_type_clause = "TABLE_TYPE = 'BASE TABLE'" 

        # Check existence first using INFORMATION_SCHEMA
        existence_query = f"""
        SELECT COUNT(*)
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG ILIKE %s 
          AND TABLE_SCHEMA ILIKE %s 
          AND TABLE_NAME ILIKE %s 
          AND {table_type_clause};
        """
        try:
            exists_result = self._execute_query(existence_query, (db_name, schema_name, object_name), fetch_results=True)
            object_exists = exists_result[0][0] > 0
        except Exception as e:
            print(f"WARNING: Could not check existence of {fqdn} (Type: {object_type}) in INFORMATION_SCHEMA in '{self.environment_name}': {e}")
            return {"exists": False, "ddl": None, "error": f"Existence check failed: {e}"}

        ddl = None
        error_notes = None

        if object_exists:
            print(f"  {object_type} '{fqdn}' exists in Snowflake in '{self.environment_name}'. Attempting to extract DDL...")
            # Use GET_DDL to extract the DDL, specifying object type
            ddl_query = f"SELECT GET_DDL('{object_type.upper()}', '{fqdn}');"
            try:
                ddl_result = self._execute_query(ddl_query, fetch_results=True)
                if ddl_result and ddl_result[0] and ddl_result[0][0]:
                    ddl = ddl_result[0][0]
                    print(f"  Successfully extracted DDL for {fqdn} in '{self.environment_name}'.")
                else:
                    error_notes = f"GET_DDL returned no result for {fqdn} (Type: {object_type}) in '{self.environment_name}'."
                    print(f"  WARNING: {error_notes}")
            except Exception as e:
                error_notes = f"Error extracting DDL for {fqdn} (Type: {object_type}) in '{self.environment_name}': {e}"
                print(f"  ERROR: {error_notes}")
        else:
            print(f"  {object_type} '{fqdn}' does NOT exist in Snowflake in '{self.environment_name}'.")

        return {
            "exists": object_exists,
            "ddl": ddl,
            "error": error_notes,
            "db_name": db_name,
            "schema_name": schema_name,
            "table_name": object_name # Changed from table_name to object_name
        }
    
    def get_all_tables_and_views_in_pattern(self, db_pattern='%', schema_pattern='%'):
        """
        Discovers all tables and views in Snowflake for the connected environment
        that match the given database and schema patterns.
        Returns a list of dicts: {'fqdn', 'db_name', 'schema_name', 'object_name', 'object_type'}
        """
        discovery_query = f"""
        SELECT 
            TABLE_CATALOG AS DB_NAME,
            TABLE_SCHEMA AS SCHEMA_NAME,
            TABLE_NAME AS OBJECT_NAME,
            CASE WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE' ELSE TABLE_TYPE END AS OBJECT_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG ILIKE %s AND TABLE_SCHEMA ILIKE %s;
        """

        print(f"Discovering objects in '{self.environment_name}' for DB pattern '{db_pattern}', Schema pattern '{schema_pattern}'...")
        try:
            results = self._execute_query(discovery_query, (db_pattern, schema_pattern), fetch_results=True)
            discovered_objects = []
            for row in results:
                db = row[0]
                schema = row[1]
                obj_name = row[2]
                obj_type = row[3] # Will be 'TABLE' or 'VIEW'

                discovered_objects.append({
                    "fqdn": f"{db}.{schema}.{obj_name}",
                    "db_name": db,
                    "schema_name": schema,
                    "object_name": obj_name,
                    "object_type": obj_type
                })
            print(f"Found {len(discovered_objects)} objects in '{self.environment_name}'.")
            return discovered_objects
        except Exception as e:
            print(f"ERROR: Failed to discover objects in '{self.environment_name}': {e}")
            return []

# Example usage (for testing this module independently)
if __name__ == "__main__":
    # Ensure your .env has Snowflake credentials set for 'DEV'
    print("Testing SnowflakeManager with DEV environment...")
    sf_manager_dev = None
    try:
        sf_manager_dev = SnowflakeManager(environment_name="DEV")
        
        # Test existence and DDL for a known table/view
        test_fqdn_table = "YOUR_DEV_DB.YOUR_DEV_SCHEMA.YOUR_DEV_TABLE" 
        test_fqdn_view = "YOUR_DEV_DB.YOUR_DEV_SCHEMA.YOUR_DEV_VIEW"
        
        result_table = sf_manager_dev.check_table_existence_and_get_ddl(test_fqdn_table, object_type="TABLE")
        print("\nResult for existing TABLE:")
        for k, v in result_table.items():
            print(f"  {k}: {v}")

        result_view = sf_manager_dev.check_table_existence_and_get_ddl(test_fqdn_view, object_type="VIEW")
        print("\nResult for existing VIEW:")
        for k, v in result_view.items():
            print(f"  {k}: {v}")
        
        # Test get_all_tables_and_views_in_pattern (e.g., all objects in a specific schema)
        all_objects = sf_manager_dev.get_all_tables_and_views_in_pattern(db_pattern='YOUR_DEV_DB', schema_pattern='YOUR_DEV_SCHEMA')
        print(f"\nDiscovered objects in YOUR_DEV_DB.YOUR_DEV_SCHEMA: {len(all_objects)}")
        # for obj in all_objects[:5]: # Print first 5
        #     print(f"  - {obj['fqdn']} ({obj['object_type']})")


    except Exception as e:
        print(f"An error occurred during SnowflakeManager test: {e}")
    finally:
        if sf_manager_dev:
            sf_manager_dev.disconnect()
