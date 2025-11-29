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

    def recover_missed_writes(self, last_known_commit_time):
        missed_logs = self._simulate_fetch_missed_logs(last_known_commit_time)

        for log in missed_logs:
            print(f"   -> REDO: Applying missed {log['operation_type']} for record {log['record_key']}...")
            
            # --- CALLING THE NEW REDO HELPER ---
            if self._apply_redo_to_main_db(log):
                print("      -> Successfully applied. Must acknowledge back to Node 1.")
            
        print(f"--- Recovery for Node {self.node_id} Complete ---")

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
            
    def _simulate_fetch_missed_logs(self, last_time):
        """
        Placeholder: In a real distributed system, this would be a network call
        to the Central Node (Node 1) or other active Fabric Nodes (Node 2/3).
        
        For simulation, you will likely query the log table of the active node(s).
        """
        # For simplicity in the example, we'll return a sample missed log entry
        if self.node_id in [2, 3]:
            # Simulate a missed log that was committed on Node 1 while Node 2/3 was down
            return [{
                'transaction_id': str(uuid.uuid4()),
                'operation_type': 'UPDATE',
                'record_key': 'A101',
                'new_value': json.dumps({"column": "value_after_recovery"}),
                'status': 'LOCAL_COMMIT',
                'log_timestamp': datetime.now()
            }]
        return []
    
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

            elif op_type == 'UPDATE':
                # --- REDO UPDATE ---
                # Dynamically build SET clause for all updated fields
                set_clauses = [f"{col} = %s" for col in new_data.keys() if col != 'titleId']
                
                # Parameters: values for SET clause, followed by the WHERE clause value (key)
                params = tuple(new_data[col] for col in new_data.keys() if col != 'titleId') + (key,)
                
                query = f"UPDATE movies SET {', '.join(set_clauses)} WHERE titleId = %s"
                
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

def simulate_failure_recovery(log_manager_central, log_manager_fabric):
    """Simulates Case #1 and Case #2/4 using the Log Manager."""
    
    # 1. Simulate a successful local transaction on a Fabric Node (N2)
    txn1_id = str(uuid.uuid4())
    log_manager_fabric.log_local_commit(txn1_id, 'UPDATE', 'R42', {"price": 150.0})
    
    # 2. Simulate **Case #1** (Replication from Node 2 to Central Node Fails)
    log_manager_fabric.log_replication_attempt(txn1_id, 1)
    
    # Simulate a network or DB failure on Node 1
    # Node 2 logs the failure and holds the log for retry
    log_manager_fabric.update_replication_status(txn1_id, 1, success=False)
    
    print("\n--- Simulating Node 1 (Central Node) Crash ---")
    last_commit_time_N1 = datetime.now()
    
    # A successful transaction occurs on Node 2 while Node 1 is down
    txn2_id = str(uuid.uuid4())
    log_manager_fabric.log_local_commit(txn2_id, 'INSERT', 'R50', {"name": "New Item"})
    log_manager_fabric.update_replication_status(txn2_id, 1, success=False) # Replication fails because N1 is down

    # 3. Simulate **Case #2** (Central Node Recovers and missed transactions)
    print(f"\n--- Simulating Central Node (Node 1) Recovering ---")
    log_manager_central.recover_missed_writes(last_commit_time_N1)
    # The recovery logic here would look at the logs of N2 and N3 to find txn2_id and re-apply it.

# Initialize the log managers (assuming they connect to their respective DBs)
log_manager_N1 = DistributedLogManager(node_id=1, db_connection=get_db_connection('node1'))
log_manager_N2 = DistributedLogManager(node_id=2, db_connection=get_db_connection('node2'))

# Run the simulation
simulate_failure_recovery(log_manager_N1, log_manager_N2)