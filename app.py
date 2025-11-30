from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector
from datetime import datetime

import uuid
from datetime import datetime
import json
from dotenv import load_dotenv
import os
from log_manager import DistributedLogManager
from db_helpers import get_db_connection, DB_CONFIG

# Global Configuration Dictionary
GLOBAL_SETTINGS = {
    'isolation_level': 'READ COMMITTED', # Default Isolation Level
    'auto_commit': True,             # Default Log Autocommit
    'auto_commit_log': True         # Default Log Autocommit
}

# Stores active connection objects for transactions waiting for manual commit.
# Structure: {txn_id: {'node1': conn_obj, 'node2': conn_obj, 'node3': conn_obj}}
ACTIVE_TXN_CONNECTIONS = {}

load_dotenv()
try:
    LOCAL_NODE_KEY = os.environ.get('LOCAL_NODE_KEY', 'node1') 
    LOCAL_NODE_ID = int(LOCAL_NODE_KEY.replace('node', ''))
    print(f"Local Node Key: {LOCAL_NODE_KEY}, ID: {LOCAL_NODE_ID}")
except Exception as e:
    print(f"Error determining local node from environment: {e}")
    LOCAL_NODE_KEY = 'node3'
    LOCAL_NODE_ID = 3
# Initialize Log Manager for Local Node
try:
    LOCAL_DB_CONN = mysql.connector.connect(**DB_CONFIG[LOCAL_NODE_KEY])
    LOG_MANAGER = DistributedLogManager(
        LOCAL_NODE_ID, 
        LOCAL_DB_CONN,
        auto_commit_log=GLOBAL_SETTINGS['auto_commit']
        )
    print(f"Log Manager initialized for {LOCAL_NODE_KEY}. Recovery startup complete.")
except Exception as e:
    print(f"Could not initialize Log Manager or connect to {LOCAL_NODE_KEY}: {e}")
    LOG_MANAGER = None

# Initialize the Flask application
app = Flask(__name__)
CORS(app)

# Database configuration
DB_CONFIG = {
    'node1': {
        'host': '10.2.14.84', 
        'user': 'admin',
        'password': 'poginiallen',     
        'database': 'mco2_ddb'      
    },
    'node2': {
        'host': '10.2.14.85',  
        'user': 'admin',
        'password': 'poginiallen',
        'database': 'mco2_ddb'
    },
    'node3': {
        'host': '10.2.14.86',  
        'user': 'admin',
        'password': 'poginiallen',
        'database': 'mco2_ddb'
    }
}

# --- HELPER FUNCTION: Connect to DB --- commented out because db_helpers also has definition for get_db_connection
def get_db_connection(node_key, isolation_level = None, autocommit_conn = True):
    try:
        config = DB_CONFIG[node_key]
        conn = mysql.connector.connect(**config)

        # --- set mySQL isolation level ---
        if isolation_level:
            # Set the desired isolation level using the connection property
            conn.isolation_level = isolation_level 
        # --- ---

        conn.autocommit = autocommit_conn

        return conn
    except Exception as e:
        print(f"Error connecting to {node_key}: {e}")
        return None
    
def execute_query(node_key, query, params=None):
    # Retrieve the current isolation level from global settings
    current_isolation_level = GLOBAL_SETTINGS['isolation_level']
    auto_commit_enabled = GLOBAL_SETTINGS['auto_commit']

    conn = get_db_connection(
        node_key, 
        isolation_level=current_isolation_level, 
        autocommit_conn=auto_commit_enabled
    )
    
    if not conn:
        return {"success": False, "error": "Connection failed"}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        rows_affected = cursor.rowcount
        cursor.close()

        # --- CONDITIONAL COMMIT AND CLOSE ---
        if auto_commit_enabled:
            conn.commit()
            conn.close()
            # Standard return in auto-commit mode
            return {"success": True, "rows_affected": rows_affected}
        else:
            # MANUAL MODE: Do not commit, do not close.
            # Return the connection object for manual management.
            return {"success": True, "rows_affected": rows_affected, "conn_obj": conn}
        # ------------------------------------
    except Exception as e:
        if conn:
            # Rollback the uncommitted transaction before closing
            conn.rollback() 
            conn.close()
        return {"success": False, "error": str(e), "rows_affected": 0}

def get_row_count(node_key):
    """Get the total number of rows in a node"""
    # Force autocommit for simple read
    conn = get_db_connection(node_key, autocommit_conn=True) 
    if not conn:
        return 0
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM movies")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Error counting rows in {node_key}: {e}")
        return 0

def get_last_update(node_key):
    """Get the timestamp of the last update in a node"""
    # TODO: Implement actual last update tracking
    # For now, return current timestamp
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
# Frontend / Homepage
@app.route('/')
def index(): 
    return render_template('index.html')

# ROUTE: Status with detailed information
@app.route('/status', methods=['GET'])
def node_status():
    status_report = {}
    for key in DB_CONFIG:
        # Force autocommit = true for connection status check
        conn = get_db_connection(key, autocommit_conn=True)
        if conn:
            # TODO: Implement real-time status monitoring
            # - Check node health
            # - Monitor active connections
            # - Track transaction logs
            row_count = get_row_count(key)
            last_update = get_last_update(key)
            status_report[key] = {
                "status": "ONLINE",
                "rows": row_count,
                "lastUpdate": last_update
            }
            conn.close()
        else:
            status_report[key] = {
                "status": "OFFLINE",
                "rows": 0,
                "lastUpdate": "N/A"
            }
    # Also report the currently active transactions
    status_report['active_transactions'] = list(ACTIVE_TXN_CONNECTIONS.keys())
    return jsonify(status_report)

# ROUTE: Read / Search with filters and pagination
@app.route('/movies', methods=['GET'])
def get_movies():
    # Get query parameters
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 100))
    
    # Filter parameters
    title_id = request.args.get('titleId', '')
    title = request.args.get('title', '')
    region = request.args.get('region', '')

    # Node selection
    requested_node = request.args.get('node', 'node1')
    if requested_node not in DB_CONFIG:
        requested_node = 'node1'

    # Build Query
    where_clause = " WHERE 1=1" 
    params = []
    if title_id:
        where_clause += " AND titleId LIKE %s"
        params.append(f"%{title_id}%")
    if title:
        where_clause += " AND title LIKE %s"
        params.append(f"%{title}%")
    if region:
        where_clause += " AND region LIKE %s"
        params.append(f"%{region}%")

    # 2. STRATEGY: Check Local Node First
    target_node = requested_node
    conn = get_db_connection(target_node)
    
    rows = []
    total_count = 0
    source = target_node

    # If connection works, try to fetch
    if conn:
        cursor = conn.cursor(dictionary=True)
        # Count
        cursor.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
        total_count = cursor.fetchone()['total']
        
        # If local node has data OR if no filters are applied (browsing mode), use local
        # If local has 0 results BUT filters are applied, we might be looking for data in another node
        if total_count > 0 or (not title_id and not title and not region):
            cursor.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
            rows = cursor.fetchall()
            conn.close()
        else:
            # Local returned 0 results, but we are searching. 
            # 3. STRATEGY: Fallback to Central (Node 1) if we are on a fragment
            conn.close()
            if requested_node != 'node1':
                print(f"Search on {requested_node} yielded 0 results. Checking Central...")
                conn_central = get_db_connection('node1')
                if conn_central:
                    cursor_central = conn_central.cursor(dictionary=True)
                    cursor_central.execute(f"SELECT COUNT(*) as total FROM movies {where_clause}", params)
                    total_count = cursor_central.fetchone()['total']
                    cursor_central.execute(f"SELECT * FROM movies {where_clause} LIMIT %s OFFSET %s", params + [limit, offset])
                    rows = cursor_central.fetchall()
                    conn_central.close()
                    source = 'node1 (Fallback)'

    return jsonify({
        "data": rows,
        "total": total_count,
        "source_node": source
    })

# ROUTE: Insert
@app.route('/insert', methods=['POST'])
def insert_movie():
    data = request.json
    txn_id = str(uuid.uuid4()) # Generate a new transaction ID
    auto_commit = GLOBAL_SETTINGS['auto_commit']

    # Determine current node
    current_node = request.args.get('node', data.get('node', 'node1'))

    params = (
        data.get('titleId'),
        data.get('ordering'),
        data.get('title'),
        data.get('region'),
        data.get('language'),
        data.get('types'),
        data.get('attributes'),
        data.get('isOriginalTitle')
    )

    query = """
        INSERT INTO movies 
        (titleId, ordering, title, region, language, types, attributes, isOriginalTitle) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Determine Correct Partition
    target_region = data.get('region')
    correct_fragment = 'node3' 
    if target_region in ['US', 'JP']: 
        correct_fragment = 'node2'
    
    logs = []

    # Initialize connection storage for this transaction if needed
    if not auto_commit:
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'INSERT', 'status': 'PENDING'}
    
    def store_conn(node_key, res):
        """Helper to store connection object if successful and in manual mode."""
        if not auto_commit and res['success']:
            ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
    
    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # "Update central node first, then identify which fragmented node to modify"
        res_central = execute_query('node1', query, params)
        store_conn('node1', res_central)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")

        # Then update fragment
        res_frag = execute_query(correct_fragment, query, params)
        store_conn(correct_fragment, res_frag)
        logs.append(f"{correct_fragment} (Fragment): {'Success' if res_frag['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")

    # SCENARIO 2 & 3: User is on a Fragment (Node 2 or 3)
    else:
        # Check if the data belongs here
        if current_node == correct_fragment:
            # SCENARIO 2: Data belongs to current node
            # "Update current node first then update central node"
            res_local = execute_query(current_node, query, params)
            store_conn(current_node, res_local)
            logs.append(f"{current_node} (Local): {'Success' if res_local['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
            
            
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
        else:
            # SCENARIO 3: Data belongs to OTHER fragment
            # "Fetch from other fragmented node" (In insert terms: Insert to other fragment)
            logs.append(f"{current_node} (Local): Skipped (Data belongs to {correct_fragment})")
            
            res_other = execute_query(correct_fragment, query, params)
            store_conn(correct_fragment, res_other)
            logs.append(f"{correct_fragment} (Remote): {'Success' if res_other['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
            
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
            
    return jsonify({"message": "Insert Processed", "txn_id": txn_id, "logs": logs})

# ROUTE: Update
@app.route('/update', methods=['POST'])
def update_movie():
    data = request.json
    txn_id = str(uuid.uuid4()) # Generate a new transaction ID
    auto_commit = GLOBAL_SETTINGS['auto_commit']
    current_node = request.args.get('node', data.get('node', 'node1'))
    
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    logs = []

    # Initialize connection storage for this transaction if needed
    if not auto_commit:
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'UPDATE', 'status': 'PENDING'}

    def store_conn(node_key, res):
        """Helper to store connection object if successful and in manual mode."""
        if not auto_commit and res['success']:
            ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
    
    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # "Update central node first"
        res_central = execute_query('node1', query, params)
        store_conn('node1', res_central)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
        
        # "Identify which fragmented node to modify"
        # Strategy: We try Node 2. If rows_affected > 0, we found it. If not, try Node 3.
        res_node2 = execute_query('node2', query, params)
        if res_node2['success'] and res_node2['rows_affected'] > 0:
            store_conn('node2', res_node2)
            logs.append(f"Node 2 (Fragment): Updated (Found target) ({'Active' if not auto_commit else 'Committed'})")
        else:
            # Wasn't in Node 2, must be Node 3
            res_node3 = execute_query('node3', query, params)
            store_conn('node3', res_node3)
            logs.append(f"Node 3 (Fragment): {'Updated' if res_node3['rows_affected'] > 0 else 'Target Not Found'} ({'Active' if not auto_commit else 'Committed'})")
            
    # SCENARIO 2 & 3: User is on Fragment
    else:
        # "Update current node first"
        res_local = execute_query(current_node, query, params)
        
        if res_local['success'] and res_local['rows_affected'] > 0:
            # SCENARIO 2: Data was local
            store_conn(current_node, res_local)
            logs.append(f"{current_node} (Local): Updated ({'Active' if not auto_commit else 'Committed'})")
            
            # "Then update central node"
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): Updated ({'Active' if not auto_commit else 'Committed'})")
            
        else:
            # SCENARIO 3: Data to be updated is NOT in current node
            logs.append(f"{current_node} (Local): 0 Rows Affected (Data is Remote)")
            
            # "Fetch from other fragmented node" -> Update the other fragment
            other_node = 'node3' if current_node == 'node2' else 'node2'
            res_other = execute_query(other_node, query, params)
            store_conn(other_node, res_other)
            logs.append(f"{other_node} (Remote): {'Updated' if res_other['rows_affected'] > 0 else 'Not Found'} ({'Active' if not auto_commit else 'Committed'})")
            
            # "Then update central node"
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): Updated ({'Active' if not auto_commit else 'Committed'})")
            
    return jsonify({"message": "Update Processed", "logs": logs})

# ROUTE: Delete
@app.route('/delete', methods=['POST'])
def delete_movie():
    data = request.json
    txn_id = str(uuid.uuid4()) # Generate a new transaction ID
    auto_commit = GLOBAL_SETTINGS['auto_commit']
    current_node = request.args.get('node', data.get('node', 'node1'))
    title_id = data.get('titleId')
    
    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    logs = []

    if not auto_commit:
        ACTIVE_TXN_CONNECTIONS[txn_id] = {'type': 'DELETE', 'status': 'PENDING'}

    def store_conn(node_key, res):
        """Helper to store connection object if successful and in manual mode."""
        # Only store the connection if the query successfully ran AND affected rows
        if not auto_commit and res['success'] and res.get('rows_affected', 0) > 0:
             ACTIVE_TXN_CONNECTIONS[txn_id][node_key] = res['conn_obj']
        # If autocommit is off and the operation failed (e.g. data not found), close the conn immediately
        elif not auto_commit and res['success'] and res.get('rows_affected', 0) == 0 and res.get('conn_obj'):
             res['conn_obj'].close()
    
    # Logic mirrors update exactly
    if current_node == 'node1':
        res_central = execute_query('node1', query, params)
        store_conn('node1', res_central)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'} ({'Active' if not auto_commit else 'Committed'})")
        
        # Try Node 2
        res_node2 = execute_query('node2', query, params)
        store_conn('node2', res_node2)
        logs.append(f"Node 2 (Fragment): {'Deleted' if res_node2['rows_affected'] > 0 else 'Not Found'} ({'Active' if not auto_commit else 'Committed'})")
        
        if res_node2['rows_affected'] == 0:
            # Try Node 3
            res_node3 = execute_query('node3', query, params)
            store_conn('node3', res_node3)
            logs.append(f"Node 3 (Fragment): {'Deleted' if res_node3['rows_affected'] > 0 else 'Not Found'} ({'Active' if not auto_commit else 'Committed'})")

    else:
        res_local = execute_query(current_node, query, params)
        if res_local['rows_affected'] > 0:
            store_conn(current_node, res_local)
            logs.append(f"{current_node} (Local): Deleted ({'Active' if not auto_commit else 'Committed'})")
            
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): Deleted ({'Active' if not auto_commit else 'Committed'})")
        else:
            logs.append(f"{current_node} (Local): 0 Rows (Remote Data)")
            other_node = 'node3' if current_node == 'node2' else 'node2'
            
            res_other = execute_query(other_node, query, params)
            store_conn(other_node, res_other)
            logs.append(f"{other_node} (Remote): {'Deleted' if res_other['rows_affected'] > 0 else 'Not Found'} ({'Active' if not auto_commit else 'Committed'})")
            
            res_central = execute_query('node1', query, params)
            store_conn('node1', res_central)
            logs.append(f"Node 1 (Central): Deleted ({'Active' if not auto_commit else 'Committed'})")

    # --- Return txn_id in response ---
    return jsonify({"message": "Delete Processed", "txn_id": txn_id, "logs": logs})

# ROUTE: Get active transactions
@app.route('/active-transactions', methods=['GET'])
def get_active_transactions():
    """Returns a list of currently active transaction IDs."""
    
    # Prepare a cleaner report for the frontend
    report = {}
    for txn_id, txn_data in ACTIVE_TXN_CONNECTIONS.items():
        # Filter out metadata keys ('type', 'status') to count only connections
        conn_count = len([k for k in txn_data.keys() if k not in ['type', 'status']])
        
        report[txn_id] = {
            'type': txn_data.get('type', 'Unknown'),
            'status': txn_data.get('status', 'PENDING'),
            'connection_count': conn_count
        }

    return jsonify(report)

# ROUTE: Manually Commit or Rollback a Transaction
@app.route('/resolve-transaction', methods=['POST'])
def resolve_transaction():
    global ACTIVE_TXN_CONNECTIONS
    data = request.json
    txn_id = data.get('txnId')
    action = data.get('action') # 'COMMIT' or 'ROLLBACK'

    if txn_id not in ACTIVE_TXN_CONNECTIONS:
        return jsonify({"message": f"Transaction ID {txn_id} not found or already resolved.", "success": False}), 404

    txn_data = ACTIVE_TXN_CONNECTIONS[txn_id]
    logs = [f"Attempting {action} for Transaction {txn_id} ({txn_data['type']})..."]
    success_count = 0
    failure_count = 0

    # Iterate over all stored connection objects for this transaction
    for node_key, conn in list(txn_data.items()):
        # Skip metadata keys
        if node_key in ['type', 'status']:
            continue
            
        try:
            if action == 'COMMIT':
                conn.commit()
                logs.append(f"{node_key}: Committed successfully.")
                success_count += 1
            elif action == 'ROLLBACK':
                conn.rollback()
                logs.append(f"{node_key}: Rolled back successfully.")
                success_count += 1
            else:
                logs.append(f"Invalid action specified: {action}. Connection for {node_key} left open.")
                failure_count += 1
                continue
            
            # Always close the connection after the final action
            conn.close()

        except Exception as e:
            logs.append(f"{node_key}: Failed to {action}. Error: {e}")
            # Try to close the connection even if commit/rollback failed
            try:
                conn.close()
            except:
                pass 
            failure_count += 1

    # Update status and remove the transaction from the active list
    txn_data['status'] = f"{action}ED"
    del ACTIVE_TXN_CONNECTIONS[txn_id]

    final_message = f"Transaction {txn_id} resolved: {success_count} successful actions, {failure_count} failures."
    
    return jsonify({
        "message": final_message, 
        "logs": logs,
        "success": failure_count == 0
    })

# ROUTE: Simulate Concurrency
@app.route('/simulate-concurrency', methods=['POST'])
def simulate_concurrency():
    """
    TODO: Implement concurrency simulation
    
    This endpoint should:
    1. Create multiple concurrent transactions
    2. Test different isolation levels
    3. Simulate race conditions
    4. Test deadlock scenarios
    5. Monitor transaction conflicts
    6. Return detailed logs of concurrent operations
    
    Example implementation:
    - Spawn multiple threads/processes
    - Execute simultaneous reads/writes
    - Track transaction timing and conflicts
    - Return results showing concurrency behavior
    """
    
    return jsonify({
        "message": "Concurrency simulation not yet implemented",
        "status": "TODO"
    })

# ROUTE: Settings
@app.route('/settings', methods=['POST'])
def update_settings():
    global GLOBAL_SETTINGS
    data = request.json
    logs = []
    
    # --- Update Isolation Level Setting ---
    isolation_level = data.get('isolationLevel')
    if isolation_level and isolation_level in ['READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE']:
        GLOBAL_SETTINGS['isolation_level'] = isolation_level
        logs.append(f"Set Global Isolation Level to: {isolation_level}")

    # --- Update Transaction Autocommit Setting ---
    auto_commit_str = data.get('autoCommit')
    if auto_commit_str is not None:
        auto_commit = auto_commit_str.lower() == 'true'
        GLOBAL_SETTINGS['auto_commit'] = auto_commit
        logs.append(f"Set Global Transaction Autocommit to: {auto_commit}")
        
    # --- Update Log Autocommit Setting ---
    auto_commit_log_str = data.get('autoCommitLog')
    if auto_commit_log_str is not None:
        auto_commit_log = auto_commit_log_str.lower() == 'true'
        GLOBAL_SETTINGS['auto_commit_log'] = auto_commit_log
        
        if LOG_MANAGER:
            LOG_MANAGER.auto_commit_log = auto_commit_log
            logs.append(f"Updated LOG_MANAGER Autocommit to: {auto_commit_log}")
        
    return jsonify({
        "status": "Settings updated successfully", 
        "current_settings": GLOBAL_SETTINGS,
        "logs": logs
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)