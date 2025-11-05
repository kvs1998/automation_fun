# snowflake_utils.py
import snowflake.connector
from config import SnowflakeConfig
from datetime import datetime
import json # For handling JSON outputs if needed

class SnowflakeManager:
    def __init__(self):
        self.conn = None
        self.connect()

    def connect(self):
        """Establishes a connection to Snowflake."""
        try:
            self.conn = snowflake.connector.connect(
                user=SnowflakeConfig.USER,
                password=SnowflakeConfig.PASSWORD,
                account=SnowflakeConfig.ACCOUNT,
                warehouse=SnowflakeConfig.WAREHOUSE,
                database=SnowflakeConfig.DATABASE,
                schema=SnowflakeConfig.SCHEMA,
                role=SnowflakeConfig.ROLE
            )
            print("Connected to Snowflake.")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            raise

    def disconnect(self):
        """Closes the Snowflake connection."""
        if self.conn:
            self.conn.close()
            print("Disconnected from Snowflake.")

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
            print(f"Error executing Snowflake query: {query}\nError: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

    def check_table_existence_and_get_ddl(self, fqdn):
        """
        Checks if a table exists and extracts its CREATE TABLE DDL from Snowflake.
        FQDN format: DATABASE.SCHEMA.TABLE_NAME
        """
        parts = fqdn.split('.')
        if len(parts) != 3:
            raise ValueError(f"FQDN '{fqdn}' is not in the expected DATABASE.SCHEMA.TABLE format.")
        
        db_name, schema_name, table_name = parts[0], parts[1], parts[2]
        
        # Check existence first using INFORMATION_SCHEMA
        existence_query = f"""
        SELECT COUNT(*)
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA ILIKE %s AND TABLE_NAME ILIKE %s AND TABLE_CATALOG ILIKE %s;
        """
        try:
            exists_result = self._execute_query(existence_query, (schema_name, table_name, db_name), fetch_results=True)
            table_exists = exists_result[0][0] > 0
        except Exception as e:
            print(f"WARNING: Could not check existence of {fqdn} in INFORMATION_SCHEMA: {e}")
            return {"exists": False, "ddl": None, "error": f"Existence check failed: {e}"}

        ddl = None
        error_notes = None

        if table_exists:
            print(f"  Table '{fqdn}' exists in Snowflake. Attempting to extract DDL...")
            # Use GET_DDL to extract the DDL
            ddl_query = f"SELECT GET_DDL('TABLE', '{fqdn}');"
            try:
                ddl_result = self._execute_query(ddl_query, fetch_results=True)
                if ddl_result and ddl_result[0] and ddl_result[0][0]:
                    ddl = ddl_result[0][0]
                    print(f"  Successfully extracted DDL for {fqdn}.")
                else:
                    error_notes = f"GET_DDL returned no result for {fqdn}."
                    print(f"  WARNING: {error_notes}")
            except Exception as e:
                error_notes = f"Error extracting DDL for {fqdn}: {e}"
                print(f"  ERROR: {error_notes}")
        else:
            print(f"  Table '{fqdn}' does NOT exist in Snowflake.")

        return {
            "exists": table_exists,
            "ddl": ddl,
            "error": error_notes,
            "db_name": db_name,
            "schema_name": schema_name,
            "table_name": table_name
        }

# Example usage (for testing this module independently)
if __name__ == "__main__":
    # Ensure your .env has Snowflake credentials set!
    # Ensure SnowflakeConfig.DATABASE and SnowflakeConfig.SCHEMA point to valid defaults!
    print("Testing SnowflakeManager...")
    sf_manager = None
    try:
        sf_manager = SnowflakeManager()
        # Replace with an actual FQDN you expect to exist or not exist
        test_fqdn_exists = "SNOWFLAKE_DB.SNOWFLAKE_SCHEMA.YOUR_EXISTING_TABLE" 
        test_fqdn_not_exists = "SNOWFLAKE_DB.SNOWFLAKE_SCHEMA.NON_EXISTENT_TABLE"
        
        # Check an existing table
        result_exists = sf_manager.check_table_existence_and_get_ddl(test_fqdn_exists)
        print("\nResult for existing table:")
        for k, v in result_exists.items():
            print(f"  {k}: {v}")

        # Check a non-existent table
        result_not_exists = sf_manager.check_table_existence_and_get_ddl(test_fqdn_not_exists)
        print("\nResult for non-existent table:")
        for k, v in result_not_exists.items():
            print(f"  {k}: {v}")

    except Exception as e:
        print(f"An error occurred during SnowflakeManager test: {e}")
    finally:
        if sf_manager:
            sf_manager.disconnect()
