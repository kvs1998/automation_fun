# database_manager.py
import sqlite3
import os
from config import FilePaths
from datetime import datetime

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
        """Creates the necessary tables if they don't exist."""
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
            last_parsed_content_hash TEXT, -- NEW: Hash of metadata when content was last successfully parsed
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
            environment TEXT NOT NULL,          -- NEW: e.g., 'DEV', 'PREPOD', 'PROD'
            object_type TEXT NOT NULL,          -- NEW: e.g., 'TABLE', 'VIEW', 'ML_MODEL_TABLE'
            
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

            PRIMARY KEY (fqdn, environment, object_type) -- NEW: Composite Primary Key
        );
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_create_metadata_table)
            cursor.execute(sql_create_parsed_content_table)
            cursor.execute(sql_create_snowflake_ml_source_table)
            self.conn.commit()
            print(f"Tables 'confluence_page_metadata', 'confluence_parsed_content', and '{FilePaths.SNOWFLAKE_ML_SOURCE_TABLE}' checked/created.")
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
            raise
            

    def insert_or_update_page_metadata(self, metadata_dict):
        """
        Inserts a new page metadata record or updates an existing one.
        Expects a dictionary conforming to the table schema.
        """
        columns = self._get_table_columns("confluence_page_metadata")
        
        if 'page_id' not in metadata_dict or metadata_dict['page_id'] is None:
            raise ValueError("Page ID must be provided for insert/update operations.")

        cursor = self.conn.cursor()
        cursor.execute("SELECT page_id FROM confluence_page_metadata WHERE page_id = ?", (metadata_dict['page_id'],))
        exists = cursor.fetchone()

        if exists:
            update_cols = [col for col in metadata_dict.keys() if col in columns and col != 'page_id']
            set_clause = ", ".join([f"{col} = ?" for col in update_cols])
            update_values = [metadata_dict[col] for col in update_cols]
            update_values.append(metadata_dict['page_id'])
            
            sql = f"UPDATE confluence_page_metadata SET {set_clause} WHERE page_id = ?"
            cursor.execute(sql, tuple(update_values))
            print(f"Updated metadata for page_id: {metadata_dict['page_id']}")
        else:
            insert_cols = [col for col in metadata_dict.keys() if col in columns]
            placeholders = ", ".join(["?" for _ in insert_cols])
            insert_values = [metadata_dict[col] for col in insert_cols]
            
            sql = f"INSERT INTO confluence_page_metadata ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(insert_values))
            print(f"Inserted metadata for page_id: {metadata_dict['page_id']}")
        
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

    def _get_table_columns(self, table_name):
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        return columns


    # MODIFIED METHOD: insert_or_update_snowflake_ml_metadata for composite PK
    def insert_or_update_snowflake_ml_metadata(self, ml_metadata_dict):
        """
        Inserts or updates a Snowflake ML source table metadata record.
        Handles current/previous DDL logic.
        Uses a composite primary key (fqdn, environment, object_type).
        """
        table_name = FilePaths.SNOWFLAKE_ML_SOURCE_TABLE
        columns = self._get_table_columns(table_name)
        
        # Ensure all parts of the composite key are present
        if not all(k in ml_metadata_dict and ml_metadata_dict[k] is not None for k in ['fqdn', 'environment', 'object_type']):
            raise ValueError("FQDN, environment, and object_type must be provided for insert/update operations.")

        composite_key_values = (ml_metadata_dict['fqdn'], ml_metadata_dict['environment'], ml_metadata_dict['object_type'])

        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} WHERE fqdn = ? AND environment = ? AND object_type = ?", composite_key_values)
        existing_record = cursor.fetchone()
        
        current_timestamp = datetime.now().isoformat()

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
            ml_metadata_dict['last_ddl_extracted_on'] = current_timestamp if new_ddl else None
            
            ml_metadata_dict['last_checked_on'] = current_timestamp

            final_update_dict = {k: ml_metadata_dict.get(k) for k in columns if k not in ['fqdn', 'environment', 'object_type']}
            update_cols = ", ".join([f"{col} = ?" for col in final_update_dict.keys()])
            update_values = list(final_update_dict.values())
            update_values.extend(composite_key_values) # Add composite key values for WHERE clause
            
            sql = f"UPDATE {table_name} SET {update_cols} WHERE fqdn = ? AND environment = ? AND object_type = ?"
            cursor.execute(sql, tuple(update_values))
            print(f"Updated ML source metadata for FQDN: {ml_metadata_dict['fqdn']} in {ml_metadata_dict['environment']}")
        else:
            ml_metadata_dict['current_ddl_hash'] = hashlib.sha256(ml_metadata_dict['current_extracted_ddl'].encode('utf-8')).hexdigest() if ml_metadata_dict.get('current_extracted_ddl') else None
            ml_metadata_dict['last_ddl_extracted_on'] = current_timestamp if ml_metadata_dict.get('current_extracted_ddl') else None
            ml_metadata_dict['ddl_changed_on'] = current_timestamp # First seen, so considered changed

            ml_metadata_dict['previous_ddl_hash'] = None
            ml_metadata_dict['previous_extracted_ddl'] = None
            ml_metadata_dict['last_checked_on'] = current_timestamp # Set for new records too

            insert_cols = [col for col in columns] # Insert all columns
            placeholders = ", ".join(["?" for _ in insert_cols])
            insert_values = [ml_metadata_dict.get(col) for col in insert_cols]
            
            sql = f"INSERT INTO {table_name} ({', '.join(insert_cols)}) VALUES ({placeholders})"
            cursor.execute(sql, (ml_metadata_dict['fqdn'],) + tuple(insert_values))
            print(f"Inserted ML source metadata for FQDN: {ml_metadata_dict['fqdn']} in {ml_metadata_dict['environment']}")
        
        self.conn.commit()

    # NEW METHOD: To retrieve existing ML metadata for context
    def get_snowflake_ml_metadata(self, fqdn, environment, object_type):
        """Retrieves a single ML table's metadata by FQDN, environment, and object_type."""
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {FilePaths.SNOWFLAKE_ML_SOURCE_TABLE} WHERE fqdn = ? AND environment = ? AND object_type = ?", (fqdn, environment, object_type))
        row = cursor.fetchone()
        return dict(row) if row else None
        
