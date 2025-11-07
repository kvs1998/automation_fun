# database_manager.py
import sqlite3
import os
from config import FilePaths
from datetime import datetime
import hashlib
import json


class DatabaseManager:
    def __init__(self, db_file=None):
        if db_file is None:
            self.db_file = os.path.join(FilePaths.TABLES_DIR, FilePaths.DB_FILE)
        else:
            self.db_file = db_file
        
        db_dir = os.path.dirname(self.db_file)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
            
        self.conn = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Establishes a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_file)
            self.conn.row_factory = sqlite3.Row
            print(f"Connected to SQLite database: {self.db_file}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def disconnect(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("Disconnected from SQLite database.")

    def create_tables(self):
        sql_create_metadata_table = """
        CREATE TABLE IF NOT EXISTS confluence_page_metadata (
            page_id INTEGER PRIMARY KEY,
            given_title TEXT NOT NULL,
            found_title TEXT,
            page_status TEXT NOT NULL, -- HIT, MISS, ERROR
            user_verified INTEGER NOT NULL DEFAULT 0,
            attempts_made INTEGER,
            api_title TEXT,             -- Actual title from expanded API call
            api_type TEXT,              -- E.g., "page", "blogpost"
            api_status TEXT,            -- E.g., "current"
            last_modified_by_display_name TEXT,
            last_modified_by_username TEXT,
            last_modified_date TEXT,    -- ISO format
            created_by_display_name TEXT,
            created_by_username TEXT,
            created_date TEXT,          -- ISO format
            parent_page_title TEXT,
            parent_page_id INTEGER,
            labels TEXT,                -- JSON string of labels
            first_checked_on TEXT,      -- ISO format (from report)
            last_checked_on TEXT,       -- ISO format (from report)
            extraction_status TEXT,     -- PENDING_METADATA_INGESTION, METADATA_INGESTED, PARSED_OK, PARSE_FAILED, DB_FAILED
            hash_id TEXT,               -- Hash of key metadata fields (current metadata hash)
            last_parsed_content_hash TEXT, -- Hash of metadata when content was last successfully parsed
            structured_data_file TEXT,  -- Link to JSON file (e.g., portfolio_ops.json) - still kept for now
            notes TEXT                  -- From report or new parsing notes
        );
        """
        sql_create_parsed_content_table = """
        CREATE TABLE IF NOT EXISTS confluence_parsed_content (
            page_id INTEGER PRIMARY KEY,
            parsed_json TEXT NOT NULL, -- Stores the entire parsed content as a JSON string
            parsed_date TEXT NOT NULL, -- Timestamp of when the content was parsed
            FOREIGN KEY (page_id) REFERENCES confluence_page_metadata(page_id) ON DELETE CASCADE
        );
        """

        sql_create_snowflake_ml_source_table = f"""
        CREATE TABLE IF NOT EXISTS {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE} (
            fqdn TEXT NOT NULL,
            environment TEXT NOT NULL,          
            object_type TEXT NOT NULL,          
            
            db_name TEXT NOT NULL,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            exists_in_snowflake INTEGER NOT NULL DEFAULT 0,
            
            current_ddl_hash TEXT,
            current_extracted_ddl TEXT,
            last_ddl_extracted_on TEXT,

            previous_ddl_hash TEXT,
            previous_extracted_ddl TEXT,
            ddl_changed_on TEXT,

            last_checked_on TEXT NOT NULL,
            notes TEXT,

            PRIMARY KEY (fqdn, environment, object_type)
        );
        """

        # MODIFIED TABLE: confluence_ml_column_map - added user_override AND is_active
        sql_create_confluence_ml_column_map = """
        CREATE TABLE IF NOT EXISTS confluence_ml_column_map (
            confluence_page_id INTEGER NOT NULL,
            confluence_target_field_name TEXT NOT NULL,
            ml_source_fqdn TEXT NOT NULL,
            ml_env TEXT NOT NULL,
            ml_object_type TEXT NOT NULL,
            
            matched_ml_column_name TEXT,
            match_percentage INTEGER,
            match_strategy TEXT,
            
            mapping_status TEXT NOT NULL,
            ml_source_ddl_hash_at_mapping TEXT,
            last_mapped_on TEXT NOT NULL,
            notes TEXT,
            user_override INTEGER NOT NULL DEFAULT 0,    -- 0 for false, 1 for true. Manual override by user.
            is_active INTEGER NOT NULL DEFAULT 1,        -- NEW: 0 for inactive/orphaned, 1 for active.

            PRIMARY KEY (confluence_page_id, confluence_target_field_name, ml_source_fqdn, ml_env, ml_object_type),
            
            FOREIGN KEY (confluence_page_id) REFERENCES confluence_page_metadata(page_id) ON DELETE CASCADE,
            FOREIGN KEY (ml_source_fqdn, ml_env, ml_object_type) REFERENCES snowflake_ml_source_metadata(fqdn, environment, object_type) ON DELETE CASCADE
        );
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_create_metadata_table)
            cursor.execute(sql_create_parsed_content_table)
            cursor.execute(sql_create_snowflake_ml_source_table)
            cursor.execute(sql_create_confluence_ml_column_map)
            self.conn.commit()
            print(f"Tables 'confluence_page_metadata', 'confluence_parsed_content', '{FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}', and 'confluence_ml_column_map' checked/created.")
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            raise

    def insert_or_update_page_metadata(self, metadata_dict):
        table_name = "confluence_page_metadata"
        all_table_columns = self._get_table_columns(table_name)
        
        pk_name = 'page_id'
        if pk_name not in metadata_dict or metadata_dict[pk_name] is None:
            raise ValueError(f"{pk_name} must be provided for insert/update operations.")

        pk_value = metadata_dict[pk_name]

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT {pk_name} FROM {table_name} WHERE {pk_name} = ?", (pk_value,))
        exists = cursor.fetchone()

        current_timestamp = datetime.now().isoformat()
        if 'last_checked_on' not in metadata_dict or metadata_dict['last_checked_on'] is None:
            metadata_dict['last_checked_on'] = current_timestamp


        if exists:
            non_pk_columns = [col for col in all_table_columns if col != pk_name]
            update_set_clauses = []
            update_values = []
            for col in non_pk_columns:
                if col in metadata_dict:
                    update_set_clauses.append(f"{col} = ?")
                    update_values.append(metadata_dict[col])

            update_values.append(pk_value)
            
            sql = f"UPDATE {table_name} SET {', '.join(update_set_clauses)} WHERE {pk_name} = ?"
            cursor.execute(sql, tuple(update_values))
            print(f"Updated metadata for page_id: {pk_value}")
        else:
            insert_cols = []
            insert_values = []
            for col in all_table_columns:
                insert_cols.append(col)
                insert_values.append(metadata_dict.get(col, None)) 

            placeholders = ", ".join(["?" for _ in insert_cols])
            
            sql = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(insert_values))
            print(f"Inserted metadata for page_id: {pk_value}")
        
        self.conn.commit()

    def insert_or_update_parsed_content(self, page_id, parsed_json_str):
        """
        Inserts or updates the parsed content JSON string for a given page_id.
        """
        cursor = self.conn.cursor()
        parsed_date = datetime.now().isoformat()

        cursor.execute("SELECT page_id FROM confluence_parsed_content WHERE page_id = ?", (page_id,))
        exists = cursor.fetchone()

        if exists:
            sql = "UPDATE confluence_parsed_content SET parsed_json = ?, parsed_date = ? WHERE page_id = ?"
            cursor.execute(sql, (parsed_json_str, parsed_date, page_id))
            print(f"Updated parsed content for page_id: {page_id}")
        else:
            sql = "INSERT INTO confluence_parsed_content (page_id, parsed_json, parsed_date) VALUES (?, ?, ?)"
            cursor.execute(sql, (page_id, parsed_json_str, parsed_date))
            print(f"Inserted parsed content for page_id: {page_id}")
        
        self.conn.commit()

    def get_page_metadata(self, page_id):
        """Retrieves a single page's metadata by page_id."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM confluence_page_metadata WHERE page_id = ?", (page_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def get_parsed_content(self, page_id):
        """Retrieves the parsed content JSON string for a given page_id."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT parsed_json FROM confluence_parsed_content WHERE page_id = ?", (page_id,))
        row = cursor.fetchone()
        return row['parsed_json'] if row else None

    def insert_or_update_snowflake_ml_metadata(self, ml_metadata_dict):
        table_name = FilePaths.SNOWFLAKE_ML_SOURCE_TABLE
        all_table_columns = self._get_table_columns(table_name)
        
        composite_key_names = ['fqdn', 'environment', 'object_type']
        if not all(k in ml_metadata_dict and ml_metadata_dict[k] is not None for k in composite_key_names):
            raise ValueError("FQDN, environment, and object_type must be provided for insert/update operations.")

        composite_key_values = (ml_metadata_dict['fqdn'], ml_metadata_dict['environment'], ml_metadata_dict['object_type'])

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE fqdn = ? AND environment = ? AND object_type = ?", composite_key_values)
        existing_record = cursor.fetchone()
        
        current_timestamp = datetime.now().isoformat()

        if 'last_checked_on' not in ml_metadata_dict or ml_metadata_dict['last_checked_on'] is None:
            ml_metadata_dict['last_checked_on'] = current_timestamp

        if existing_record:
            new_ddl = ml_metadata_dict.get('current_extracted_ddl')
            new_ddl_hash = hashlib.sha256(new_ddl.encode('utf-8')).hexdigest() if new_ddl else None
            
            old_current_ddl_hash = existing_record['current_ddl_hash']
            
            if new_ddl_hash and new_ddl_hash != old_current_ddl_hash:
                print(f"  DDL Change Detected for {ml_metadata_dict['fqdn']} in {ml_metadata_dict['environment']} ({ml_metadata_dict['object_type']})!")
                ml_metadata_dict['previous_ddl_hash'] = old_current_ddl_hash
                ml_metadata_dict['previous_extracted_ddl'] = existing_record['current_extracted_ddl']
                ml_metadata_dict['ddl_changed_on'] = current_timestamp
            elif existing_record['exists_in_snowflake'] == 0 and ml_metadata_dict['exists_in_snowflake'] == 1:
                 print(f"  {ml_metadata_dict['object_type']} {ml_metadata_dict['fqdn']} now exists in Snowflake {ml_metadata_dict['environment']}!")
                 ml_metadata_dict['ddl_changed_on'] = current_timestamp
            else:
                ml_metadata_dict['previous_ddl_hash'] = existing_record['previous_ddl_hash']
                ml_metadata_dict['previous_extracted_ddl'] = existing_record['previous_extracted_ddl']
                ml_metadata_dict['ddl_changed_on'] = existing_record['ddl_changed_on']
            
            ml_metadata_dict['current_ddl_hash'] = new_ddl_hash
            ml_metadata_dict['current_extracted_ddl'] = new_ddl
            ml_metadata_dict['last_ddl_extracted_on'] = current_timestamp if new_ddl else existing_record['last_ddl_extracted_on']
            
            ml_metadata_dict['last_checked_on'] = current_timestamp

            non_pk_columns = [col for col in all_table_columns if col not in composite_key_names]
            update_set_clauses = []
            update_values = []
            for col in non_pk_columns:
                if col in ml_metadata_dict:
                    update_set_clauses.append(f"{col} = ?")
                    update_values.append(ml_metadata_dict[col])

            update_values.extend(composite_key_values)
            
            sql = f"UPDATE {table_name} SET {', '.join(update_set_clauses)} WHERE fqdn = ? AND environment = ? AND object_type = ?"
            cursor.execute(sql, tuple(update_values))
            print(f"Updated ML source metadata for FQDN: {ml_metadata_dict['fqdn']} in {ml_metadata_dict['environment']}")
        else:
            new_ddl = ml_metadata_dict.get('current_extracted_ddl')
            ml_metadata_dict['current_ddl_hash'] = hashlib.sha256(new_ddl.encode('utf-8')).hexdigest() if new_ddl else None
            ml_metadata_dict['last_ddl_extracted_on'] = current_timestamp if new_ddl else None
            ml_metadata_dict['ddl_changed_on'] = current_timestamp

            ml_metadata_dict['previous_ddl_hash'] = None
            ml_metadata_dict['previous_extracted_ddl'] = None
            ml_metadata_dict['last_checked_on'] = current_timestamp

            insert_cols = []
            insert_values = []
            for col in all_table_columns:
                insert_cols.append(col)
                insert_values.append(ml_metadata_dict.get(col, None)) 

            placeholders = ", ".join(["?" for _ in insert_cols])
            
            sql = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(insert_values))
            print(f"Inserted ML source metadata for FQDN: {ml_metadata_dict['fqdn']} in {ml_metadata_dict['environment']}")
        
        self.conn.commit()
    
    def get_snowflake_ml_metadata(self, fqdn, environment, object_type):
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE} WHERE fqdn = ? AND environment = ? AND object_type = ?", (fqdn, environment, object_type))
        row = cursor.fetchone()
        return dict(row) if row else None
    
    # MODIFIED: insert_or_update_confluence_ml_column_map - updated for is_active
    def insert_or_update_confluence_ml_column_map(self, column_map_dict):
        """
        Inserts or updates a column mapping record in confluence_ml_column_map.
        """
        table_name = "confluence_ml_column_map"
        all_table_columns = self._get_table_columns(table_name)

        composite_pk_names = [
            'confluence_page_id', 'confluence_target_field_name',
            'ml_source_fqdn', 'ml_env', 'ml_object_type'
        ]
        if not all(k in column_map_dict and column_map_dict[k] is not None for k in composite_pk_names):
            raise ValueError(f"Composite primary key components {composite_pk_names} must be provided for column map operations.")
        
        composite_pk_values = tuple(column_map_dict[k] for k in composite_pk_names)
        pk_where_clause = " AND ".join([f"{k} = ?" for k in composite_pk_names])

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE {pk_where_clause}", composite_pk_values)
        exists = cursor.fetchone()

        if exists:
            non_pk_columns = [col for col in all_table_columns if col not in composite_pk_names]
            update_set_clauses = []
            update_values = []
            for col in non_pk_columns:
                if col in column_map_dict: 
                    update_set_clauses.append(f"{col} = ?")
                    update_values.append(column_map_dict[col])
            
            update_values.extend(composite_pk_values)
            sql = f"UPDATE {table_name} SET {', '.join(update_set_clauses)} WHERE {pk_where_clause}"
            cursor.execute(sql, tuple(update_values))
            print(f"Updated column map for {column_map_dict['confluence_page_id']} -> {column_map_dict['confluence_target_field_name']} to {column_map_dict['ml_source_fqdn']} in {column_map_dict['ml_env']}.")
        else:
            insert_cols = []
            insert_values = []
            for col in all_table_columns:
                insert_cols.append(col)
                insert_values.append(column_map_dict.get(col, None)) # Use .get with None for safety
            
            placeholders = ", ".join(["?" for _ in insert_cols])
            sql = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(insert_values))
            print(f"Inserted column map for {column_map_dict['confluence_page_id']} -> {column_map_dict['confluence_target_field_name']} to {column_map_dict['ml_source_fqdn']} in {column_map_dict['ml_env']}.")
        self.conn.commit()
    
    def _get_table_columns(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        return columns
