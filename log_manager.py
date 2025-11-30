import uuid
from datetime import datetime
import json
from db_helpers import get_db_connection

class DistributedLogManager:
    def __init__(self, node_id, db_connection):
        self.node_id = node_id  # 1 (Central), 2, or 3
        self.db_conn = db_connection
        self._initialize_log_table()

    def _initialize_log_table(self):
        sql = """
        CREATE TABLE IF NOT EXISTS transaction_logs (
            log_id INT AUTO_INCREMENT PRIMARY KEY,
            transaction_id VARCHAR(36) NOT NULL,
            log_timestamp DATETIME NOT NULL,
            operation_type VARCHAR(20), 
            record_key VARCHAR(50), 
            new_value JSON, 
            replication_target INT, 
            status VARCHAR(30) 
            -- Note: 'old_value' omitted here for true Deferred (NO-UNDO), 
            -- but recommended for validation (as discussed previously).
        );
        """
       

    def log_local_commit(self, txn_id, op_type, key, new_data):
        sql = """
        INSERT INTO transaction_logs 
        (transaction_id, log_timestamp, operation_type, record_key, new_value, replication_target, status)
        VALUES (%s, %s, %s, %s, %s, NULL, 'LOCAL_COMMIT');
        """
        params = (
            txn_id,
            datetime.now(),
            op_type,
            key,
            json.dumps(new_data)
        )
        
        # Cursor is for tracking the log transactions
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit() 
            cursor.close()
            print(f"Log: Transaction {txn_id} committed successfully on Node {self.node_id} (LOG SAVED).")
        except Exception as e:
            print(f"FATAL LOGGING ERROR for {txn_id}: {e}")

    # --- Step 2: Logging Replication Attempts (Handles Case #1 and #3) ---

    def log_replication_attempt(self, txn_id, target_node):
        """Logs the start of a replication attempt to a remote node."""
        sql = """
        INSERT INTO transaction_logs 
        (transaction_id, log_timestamp, operation_type, record_key, new_value, replication_target, status)
        VALUES (%s, %s, 'REPLICATE', NULL, NULL, %s, 'REPLICATION_PENDING');
        """
        # Fetch the original new_value if needed, but here we just log the intent.
        
        params = (txn_id, datetime.now(), target_node)
        cursor = self.db_conn.cursor()
        cursor.execute(sql, params)
        self.db_conn.commit()
        cursor.close()
        print(f"Log: Transaction {txn_id} replication PENDING to Node {target_node}.")
    
   
    
    def update_replication_status(self, txn_id, target_node, success=True):
        """Updates the status after a replication attempt (success or failure)."""
        new_status = 'REPLICATION_SUCCESS' if success else 'REPLICATION_FAILED'
        sql = """
        UPDATE transaction_logs 
        SET status = %s 
        WHERE transaction_id = %s AND replication_target = %s AND status = 'REPLICATION_PENDING';
        """
        params = (new_status, txn_id, target_node)
        cursor = self.db_conn.cursor()
        cursor.execute(sql, params)
        self.db_conn.commit()
        cursor.close()
        print(f"Log: Transaction {txn_id} replication status updated to {new_status} for Node {target_node}.")

    # --- Step 3: Global Failure Recovery Logic (Handles Case #2 and #4) ---



    def log_prepare_start(self, txn_id):
        """Logs the coordinator's initiation of the 2PC protocol (Phase 1)."""
        sql = "INSERT INTO transaction_logs (transaction_id, log_timestamp, status) VALUES (%s, %s, 'PREPARE_SENT');"
        params = (txn_id, datetime.now())
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit() 
            cursor.close()
            print(f"Log: Coordinator on Node {self.node_id} sent PREPARE for {txn_id} (LOG SAVED).")
        except Exception as e:
            print(f"FATAL LOGGING ERROR (PREPARE) for {txn_id}: {e}")
            raise e # Re-raise to ensure transaction failure is handled

    def log_ready_status(self, txn_id, op_type, key, new_data):
        """Logs the participant's readiness to commit (Phase 1 response), including REDO image."""
        sql = """
        INSERT INTO transaction_logs 
        (transaction_id, log_timestamp, operation_type, record_key, new_value, status)
        VALUES (%s, %s, %s, %s, %s, 'READY_COMMIT');
        """
        params = (
            txn_id,
            datetime.now(),
            op_type,
            key,
            json.dumps(new_data)
        )
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit() 
            cursor.close()
            print(f"Log: Participant on Node {self.node_id} is READY for {txn_id} (LOG SAVED).")
        except Exception as e:
            print(f"FATAL LOGGING ERROR (READY) for {txn_id}: {e}")
            raise e

    def log_global_commit(self, txn_id, commit=True):
        """Logs the coordinator's final, irrevocable decision (Phase 2)."""
        status = 'GLOBAL_COMMIT' if commit else 'GLOBAL_ABORT'
        sql = "INSERT INTO transaction_logs (transaction_id, log_timestamp, status) VALUES (%s, %s, %s);"
        params = (txn_id, datetime.now(), status)
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            self.db_conn.commit() 
            cursor.close()
            print(f"Log: Coordinator on Node {self.node_id} recorded {status} for {txn_id} (IRREVOCABLE).")
            return {'success': True}
        except Exception as e:
            print(f"FATAL LOGGING ERROR (GLOBAL_COMMIT/ABORT) for {txn_id}: {e}")
            return {'success': False, 'error': str(e)}

    # NOTE: The original log_local_commit is now redundant and should be removed. 
    # The replication-related methods can stay as they track the commit outcome.

    def _apply_redo_to_main_db(self, log_entry):
        """
        Applies the 'After Image' (new_value) from a log entry to the main 
        database of the local node (REDO operation).
        
        Args:
            log_entry (dict): A single row fetched from the transaction_logs table.
        """
        op_type = log_entry['operation_type']
        key = log_entry['record_key']
        
        # Load the JSON data that represents the state AFTER the change
        # Note: new_value is stored as a JSON string in the log table
        new_data = json.loads(log_entry['new_value']) 

        cursor = self.db_conn.cursor()

        try:
            if op_type == 'INSERT':
                # --- REDO INSERT ---
                columns = ['titleId', 'ordering', 'title', 'region', 'language', 'types', 'attributes', 'isOriginalTitle']
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)

                # Ensure params are ordered according to the 'columns' list
                params = tuple(new_data.get(col) for col in columns)

                query = f"""
                    INSERT INTO movies 
                    ({column_names}) 
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE title=VALUES(title) 
                    -- ^ Ensures idempotent operation: if the row already exists (e.g., due to crash), it updates instead of failing.
                """
                cursor.execute(query, params)

            # In log_manager.py: Inside _apply_redo_to_main_db
            # In log_manager.py: Inside _apply_redo_to_main_db
            elif op_type == 'UPDATE':
                # --- REDO UPDATE ---
                
                # Filter out the primary key (titleId) and build the list of columns to update
                columns_to_update = [col for col in new_data.keys() if col != 'titleId']
                
                # 1. Prepare the SET clause: "column1 = %s, column2 = %s, ..."
                set_clauses = [f"{col} = %s" for col in columns_to_update] 
                
                # 2. Prepare the parameters: (value1, value2, ..., title_id)
                params = tuple(new_data[col] for col in columns_to_update) + (key,) # 'key' is the titleId from the log
                
                # 3. Construct the final query
                query = f"UPDATE movies SET {', '.join(set_clauses)} WHERE titleId = %s"
                
                # Execute the query
                cursor.execute(query, params)

            elif op_type == 'DELETE':
                # --- REDO DELETE ---
                query = "DELETE FROM movies WHERE titleId = %s"
                cursor.execute(query, (key,))

            self.db_conn.commit()
            print(f"REDO Success: {op_type} for record {key} applied to main DB.")
            return True
            
        except Exception as e:
            self.db_conn.rollback()
            print(f"REDO FAILURE: {op_type} for record {key} failed: {e}")
            return False
        finally:
            cursor.close()
# --- Example Usage and Simulation ---


# Initialize the log managers (assuming they connect to their respective DBs)
log_manager_N1 = DistributedLogManager(node_id=1, db_connection=get_db_connection('node1'))
log_manager_N2 = DistributedLogManager(node_id=2, db_connection=get_db_connection('node2'))

# Run the simulation
