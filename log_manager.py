import uuid
from datetime import datetime
import json

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

    def recover_missed_writes(self, last_known_commit_time):
        """
        Simulates the recovery process for a node that missed transactions 
        (Node 1 in Case #2, Node 2/3 in Case #4).
        
        It would typically query the *surviving* node(s) for all committed 
        logs after `last_known_commit_time`.
        """
        print(f"\n--- Recovery Operation Started for Node {self.node_id} ---")
        
        # 1. Redo Local Changes (If node crashed after log but before main DB write)
        # In deferred, we just check logs with status 'LOCAL_COMMIT' but not yet applied
        # and re-apply the changes (REDO).
        print("1. Performing Local REDO (applying committed changes from log to main DB)...")
        # SQL to find: logs where LOCAL_COMMIT but no corresponding entry in main DB...
        
        # 2. Synchronize Missed Remote Changes (The core of Case #2 & #4)
        print(f"2. Requesting missed replicated transactions from active nodes since {last_known_commit_time}...")
        
        # **SIMULATION:** In a real implementation, this would involve a network call.
        # Here we simulate fetching the required log entries (e.g., from Node 1's log).
        
        # A successful transaction on Node X needs to be applied on Node Y.
        # The logs from active nodes will contain entries where 'status' is 'LOCAL_COMMIT' 
        # (or 'REPLICATION_SUCCESS') and 'log_timestamp' > last_known_commit_time.
        
        missed_logs = self._simulate_fetch_missed_logs(last_known_commit_time)
        
        if not missed_logs:
            print("   -> No missed transactions found. System is up to date.")
            return

        for log in missed_logs:
            # Re-apply the transaction's new_value to the main DB
            # The 'new_value' is the After Image, used for REDO.
            print(f"   -> REDO: Applying missed {log['operation_type']} for record {log['record_key']} (Txn ID: {log['transaction_id']})")
            # self._apply_redo_to_main_db(log) 
            
        print(f"--- Recovery for Node {self.node_id} Complete ---")

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
log_manager_N1 = DistributedLogManager(node_id=1, db_connection=None) 
log_manager_N2 = DistributedLogManager(node_id=2, db_connection=None)

# Run the simulation
simulate_failure_recovery(log_manager_N1, log_manager_N2)