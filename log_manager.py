import uuid
from datetime import datetime
import json
# Import necessary function for the simulation block at the bottom
from db_helpers import get_db_connection 

class DistributedLogManager:
    """
    Manages transaction logs for the local node (Node 1, 2, or 3).
    Implements Deferred (NO-UNDO/REDO) logging protocol tailored for
    distributed updates from the coordinator (Node 1).
    """
    def __init__(self, node_id, db_connection, auto_commit_log=True):
        self.node_id = node_id  # 1 (Central), 2, or 3
        self.db_conn = db_connection
        # Controls whether log operations themselves are immediately committed.
        # Typically, this should be True for durability, but made configurable.
        self.auto_commit_log = auto_commit_log 
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
            -- Note: 'old_value' omitted here for true Deferred (NO-UNDO)
        );
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql)
            cursor.close()
            # Ensure table creation is committed, regardless of self.auto_commit_log
            self.db_conn.commit() 
            print(f"Transaction log table ensured on Node {self.node_id}.")
        except Exception as e:
            print(f"Error initializing log table on Node {self.node_id}: {e}")

    def _commit_if_needed(self):
        """Commits the log operation if auto_commit_log is True."""
        if self.auto_commit_log:
            try:
                self.db_conn.commit()
            except Exception as e:
                print(f"Error committing log record on Node {self.node_id}: {e}")
                # Log commit failed, but application continues (potentially losing the log record)

    def log_local_commit(self, txn_id, op_type, key, new_data):
        """Logs the final successful commit event for a transaction."""
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
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            cursor.close()
            self._commit_if_needed() # Use the conditional commit helper
            print(f"Log: Transaction {txn_id} recorded on Node {self.node_id}.")
        except Exception as e:
            print(f"Error logging LOCAL_COMMIT for {txn_id}: {e}")

    def log_replication_attempt(self, txn_id, target_node):
        """Logs that replication is being attempted for a specific target node."""
        sql = """
        INSERT INTO transaction_logs 
        (transaction_id, log_timestamp, operation_type, record_key, new_value, replication_target, status)
        VALUES (%s, %s, 'REPLICATE', NULL, NULL, %s, 'REPLICATION_PENDING');
        """
        params = (
            txn_id, 
            datetime.now(), 
            target_node
        )
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(sql, params)
            cursor.close()
            self._commit_if_needed() # Use the conditional commit helper
            print(f"Log: Transaction {txn_id} replication PENDING to Node {target_node}.")
        except Exception as e:
            print(f"Error logging REPLICATION_ATTEMPT for {txn_id} to {target_node}: {e}")

    def update_replication_status(self, txn_id, target_node, success=True):
        """Updates the status after a replication attempt (success or failure)."""
        new_status = 'REPLICATION_SUCCESS' if success else 'REPLICATION_FAILED'
        
        # 1. Update existing attempt log
        update_sql = """
        UPDATE transaction_logs
        SET status = %s, log_timestamp = %s
        WHERE transaction_id = %s AND replication_target = %s AND status = 'REPLICATION_PENDING'
        ORDER BY log_id DESC 
        LIMIT 1;
        """
        update_params = (new_status, datetime.now(), txn_id, target_node)

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(update_sql, update_params)
            
            if cursor.rowcount == 0:
                # 2. If no PENDING log was found to update, insert the final status
                insert_sql = """
                INSERT INTO transaction_logs 
                (transaction_id, log_timestamp, operation_type, record_key, new_value, replication_target, status)
                VALUES (%s, %s, 'REPLICATE', NULL, NULL, %s, %s);
                """
                insert_params = (txn_id, datetime.now(), target_node, new_status)
                cursor.execute(insert_sql, insert_params)
            
            cursor.close()
            self._commit_if_needed() # Use the conditional commit helper
            print(f"Log: Transaction {txn_id} replication status updated to {new_status} for Node {target_node}.")
        except Exception as e:
            print(f"Error updating REPLICATION_STATUS for {txn_id} to {target_node}: {e}")
            
    # --- Step 3: Global Failure Recovery Logic (Handles Case #2 and #4) ---

    def recover_missed_writes(self, last_known_commit_time):
        """
        Runs the REDO recovery procedure for the local node.
        """
        print(f"--- Running Recovery Check for Node {self.node_id} since: {last_known_commit_time} ---")
        missed_logs = self._simulate_fetch_missed_logs(last_known_commit_time)

        if not missed_logs:
            print("No missed transactions found to REDO.")
            
        for log in missed_logs:
            print(f" -> REDO: Applying missed {log['operation_type']} for record {log['record_key']} (Txn: {log['transaction_id'][:8]}...)")
            
            # --- CALLING THE REDO HELPER ---
            if self._apply_redo_to_main_db(log):
                # In a real DDM, the node would now acknowledge success back to the coordinator (Node 1)
                pass
            
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
        Placeholder: Simulates fetching missed transactions from the coordinator.
        
        If this is Node 2 or 3, it returns a simulated missed transaction.
        """
        if self.node_id in [2, 3]:
            # Simulate a missed log that was committed on Node 1 while Node 2/3 was down
            return [{
                'transaction_id': str(uuid.uuid4()),
                'operation_type': 'UPDATE', # Use UPDATE for recovery simulation
                'record_key': 'tt0099685',  # A real IMDb key for plausibility
                'new_value': json.dumps({"title": "The Big Lebowski (Restored)", "region": "US"}),
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
        new_data = json.loads(log_entry['new_value']) 

        cursor = self.db_conn.cursor()

        try:
            if op_type == 'INSERT':
                # --- REDO INSERT ---
                columns = list(new_data.keys()) 
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)

                params = tuple(new_data[col] for col in columns)

                query = f"""
                    INSERT INTO movies 
                    ({column_names}) 
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE title=VALUES(title); 
                    -- ^ Ensures idempotent operation (REDO principle)
                """
                cursor.execute(query, params)

            elif op_type == 'UPDATE':
                # --- REDO UPDATE ---
                # Dynamically build SET clause for all updated fields
                set_clauses = [f"{col} = %s" for col in new_data.keys() if col != 'titleId']
                
                # Parameters: values for SET clause, followed by the WHERE clause value (key)
                # Assumes 'key' (record_key) maps to the primary key, 'titleId'
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

# --- Example Usage and Simulation (Requires db_helpers) ---

def simulate_failure_recovery(log_manager_central, log_manager_fabric):
    """Simulates a multi-node transaction and subsequent crash recovery."""
    
    print("\n" + "="*50)
    print("SIMULATION START: LOCAL COMMIT & REPLICATION FAILURE")
    print("="*50)

    # 1. Simulate a successful local transaction on a Fabric Node (N2)
    txn1_id = str(uuid.uuid4())
    log_manager_fabric.log_local_commit(txn1_id, 'UPDATE', 'tt0099685', {"title": "The Big Lebowski (Local Edit)"})
    
    # 2. Simulate **Case #1** (Replication from Node 2 to Central Node Fails)
    log_manager_fabric.log_replication_attempt(txn1_id, 1)
    log_manager_fabric.update_replication_status(txn1_id, 1, success=False)
    
    print("\n" + "="*50)
    print("SIMULATION PHASE 2: CENTRAL NODE CRASH")
    print("="*50)
    
    # Set the crash time marker
    last_commit_time_N1 = datetime.now()
    
    # A successful transaction occurs on Node 2 while Node 1 is down
    txn2_id = str(uuid.uuid4())
    log_manager_fabric.log_local_commit(txn2_id, 'INSERT', 'tt0000001', {"titleId": "tt0000001", "ordering": 1, "title": "Crash Test Movie", "region": "ZZ", "language": None, "types": None, "attributes": None, "isOriginalTitle": 0})
    log_manager_fabric.update_replication_status(txn2_id, 1, success=False) # Replication fails because N1 is logically 'down'

    # 3. Simulate **Case #2** (Central Node Recovers and missed transactions)
    print("\n" + "="*50)
    print(f"SIMULATION PHASE 3: CENTRAL NODE (N1) RECOVERY ({datetime.now().strftime('%H:%M:%S')})")
    print("="*50)
    
    # When Node 1 runs recover_missed_writes, it triggers the _simulate_fetch_missed_logs 
    # which returns a dummy transaction for REDO.
    log_manager_central.recover_missed_writes(last_commit_time_N1)

# Note: The actual simulation block below requires a running database connection 
# from the main environment to execute successfully.