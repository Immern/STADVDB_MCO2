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
    LOG_MANAGER = DistributedLogManager(LOCAL_NODE_ID, LOCAL_DB_CONN)
    print(f"Log Manager initialized for {LOCAL_NODE_KEY}. Recovery startup complete.")
except Exception as e:
    print(f"Could not initialize Log Manager or connect to {LOCAL_NODE_KEY}: {e}")
    LOG_MANAGER = None

# Initialize the Flask application
app = Flask(__name__)
CORS(app)


# --- HELPER FUNCTION: Connect to DB ---
def get_db_connection(node_key):
    try:
        config = DB_CONFIG[node_key]
        conn = mysql.connector.connect(**config)
        return conn
    except Exception as e:
        print(f"Error connecting to {node_key}: {e}")
        return None
    
def execute_query(node_key, query, params=None):
    conn = get_db_connection(node_key)
    if not conn:
        return {"success": False, "error": "Connection failed"}
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        rows_affected = cursor.rowcount
        cursor.close()
        conn.close()
        return {"success": True, "rows_affected": rows_affected}
    except Exception as e:
        return {"success": False, "error": str(e), "rows_affected": 0}

def get_row_count(node_key):
    """Get the total number of rows in a node"""
    conn = get_db_connection(node_key)
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
        conn = get_db_connection(key)
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
    # Ensure the LOG_MANAGER is available (from global instantiation)
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json

    txn_id = str(uuid.uuid4())
    record_key = data.get('titleId')

    # Prepare the 'new_value' payload for the log (After Image)
    new_value = {
        'titleId': record_key,
        'ordering': data.get('ordering'),
        'title': data.get('title'),
        'region': data.get('region'),
        'language': data.get('language'),
        'types': data.get('types'),
        'attributes': data.get('attributes'),
        'isOriginalTitle': data.get('isOriginalTitle')
    }
    # ------------------------------------------------------------------

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
    correct_fragment_key = 'node3' 
    correct_fragment_id = 3
    if target_region in ['US', 'JP']: 
        correct_fragment_key = 'node2'
        correct_fragment_id = 2

    # logs for CONSOLE output
    logs = []

    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # 1. "Update central node first"
        res_central = execute_query('node1', query, params)
        
        # LOG LOCAL COMMIT on Node 1 (Central)
        if res_central['success']:
            LOG_MANAGER.log_local_commit(txn_id, 'INSERT', record_key, new_value)
            logs.append("Node 1 (Local): Success & Logged")

            # 2. "Then identify which fragmented node to modify" (Replication)
            LOG_MANAGER.log_replication_attempt(txn_id, correct_fragment_id)
            res_frag = execute_query(correct_fragment_key, query, params)
            
            # Update Replication Status on Node 1
            LOG_MANAGER.update_replication_status(txn_id, correct_fragment_id, res_frag['success'])
            logs.append(f"{correct_fragment_key} (Fragment): {'Success' if res_frag['success'] else 'Failed (Log updated)'}")
        else:
            # Central commit failed. No successful log entry.
            logs.append(f"Node 1 (Local): Failed - {res_central.get('error', '')}")


    # SCENARIO 2 & 3: User is on a Fragment (Node 2 or 3)
    else:
        current_node_id = int(current_node.replace('node', ''))
        
        # Check if the data belongs here
        if current_node == correct_fragment_key:
            # SCENARIO 2: Data belongs to current node
            
            # 1. "Update current node first"
            res_local = execute_query(current_node, query, params)
            
            # LOG LOCAL COMMIT on the Fragment Node
            if res_local['success']:
                LOG_MANAGER.log_local_commit(txn_id, 'INSERT', record_key, new_value)
                logs.append(f"{current_node} (Local): Success & Logged")
                
                # 2. "Then update central node" (Replication)
                LOG_MANAGER.log_replication_attempt(txn_id, 1) # Target is Central Node (ID 1)
                res_central = execute_query('node1', query, params)
                
                # Update Replication Status on the Fragment Node
                LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
                logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed (Log updated)'}")
            else:
                logs.append(f"{current_node} (Local): Failed - {res_local.get('error', '')}")
            
        else:
            # SCENARIO 3: Data belongs to OTHER fragment
            # "Fetch from other fragmented node" (In insert terms: Insert to other fragment)
            logs.append(f"{current_node} (Local): Skipped (Data belongs to {correct_fragment_key})")
            
            res_other = execute_query(correct_fragment_key, query, params)
            
            if res_other['success']:
                LOG_MANAGER.log_local_commit(txn_id, 'INSERT', record_key, new_value)
                logs.append(f"{correct_fragment_key} (Remote): Success & Commit Logged on {current_node}")
                
                LOG_MANAGER.log_replication_attempt(txn_id, 1) 
                res_central = execute_query('node1', query, params)
                
                LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
                logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed (Log updated)'}")
            else:
                 logs.append(f"{correct_fragment_key} (Remote): Failed - No Log Commit")

    return jsonify({
        "message": "Insert Processed", 
        "logs": logs,
        "txn_id": txn_id
    })
    
    
# ROUTE: Update
@app.route('/update', methods=['POST'])
def update_movie():
    # Ensure the LOG_MANAGER is available (from global instantiation)
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    
    # 1. Transaction Setup & Log Data Preparation
    txn_id = str(uuid.uuid4()) 
    record_key = data.get('titleId')
    
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')

    # Prepare the 'new_value' payload for the log (After Image)
    # NOTE: You must include all columns being changed for a complete REDO image.
    new_value = {
        'titleId': record_key, # Key used for WHERE clause
        'ordering': new_ordering,
        'title': new_title,
        # Other potentially updated fields should also be included if they exist in `data`
    }
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    logs = []

    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # 1. "Update central node first"
        res_central = execute_query('node1', query, params)
        
        if res_central['success'] and res_central['rows_affected'] > 0:
            # Log Local Commit on Node 1 (Central)
            LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', record_key, new_value)
            logs.append("Node 1 (Local): Success & Logged")

            # 2. "Identify which fragmented node to modify" (Replication)
            
            # --- Attempt Node 2 Replication ---
            LOG_MANAGER.log_replication_attempt(txn_id, 2)
            res_node2 = execute_query('node2', query, params)
            LOG_MANAGER.update_replication_status(txn_id, 2, res_node2['success'])
            
            if res_node2['success'] and res_node2['rows_affected'] > 0:
                logs.append(f"Node 2 (Fragment): Updated (Found target, Log updated)")
            else:
                logs.append(f"Node 2 (Fragment): Skipped/Not Found (Log updated)")
                
                # --- Attempt Node 3 Replication ---
                LOG_MANAGER.log_replication_attempt(txn_id, 3)
                res_node3 = execute_query('node3', query, params)
                LOG_MANAGER.update_replication_status(txn_id, 3, res_node3['success'])
                
                if res_node3['rows_affected'] > 0:
                     logs.append(f"Node 3 (Fragment): Updated (Found target, Log updated)")
                else:
                     logs.append(f"Node 3 (Fragment): Target Not Found (Log updated)")
        else:
            logs.append(f"Node 1 (Local): Failed/No Rows Affected - {res_central.get('error', '')}")

    # SCENARIO 2 & 3: User is on Fragment
    else:
        # 1. "Update current node first"
        res_local = execute_query(current_node, query, params)
        current_node_id = int(current_node.replace('node', ''))
        
        if res_local['success'] and res_local['rows_affected'] > 0:
            # SCENARIO 2: Data was local (Log commit on Fragment Node)
            LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', record_key, new_value)
            logs.append(f"{current_node} (Local): Updated & Logged")
            
            # 2. "Then update central node" (Replication)
            LOG_MANAGER.log_replication_attempt(txn_id, 1) # Target is Central Node (ID 1)
            res_central = execute_query('node1', query, params)
            
            # Update Replication Status on the Fragment Node
            LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
            logs.append(f"Node 1 (Central): {'Updated' if res_central['success'] else 'Failed (Log updated)'}")
        else:
            # SCENARIO 3: Data to be updated is NOT in current node
            logs.append(f"{current_node} (Local): 0 Rows Affected (Data is Remote)")
            
            # 1. "Update the other fragment"
            other_node_key = 'node3' if current_node == 'node2' else 'node2'
            other_node_id = int(other_node_key.replace('node', ''))
            res_other = execute_query(other_node_key, query, params)

            if res_other['success'] and res_other['rows_affected'] > 0:
                # If the remote update succeeds, log the commit on the *current node* (acting as coordinator)
                LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', record_key, new_value)
                logs.append(f"{other_node_key} (Remote): Updated & Commit Logged on {current_node}")

                # 2. "Then update central node" (Replication)
                LOG_MANAGER.log_replication_attempt(txn_id, 1) # Target is Central Node (ID 1)
                res_central = execute_query('node1', query, params)
                
                # Update Replication Status on the calling node (current_node)
                LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
                logs.append(f"Node 1 (Central): {'Updated' if res_central['success'] else 'Failed (Log updated)'}")
            else:
                logs.append(f"{other_node_key} (Remote): Not Found/Failed")
                # No successful local commit log written if the remote update failed.

    return jsonify({
        "message": "Update Processed", 
        "logs": logs,
        "txn_id": txn_id
    })
    
    
# ROUTE: Delete
@app.route('/delete', methods=['POST'])
def delete_movie():
    # Ensure the LOG_MANAGER is available
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    title_id = data.get('titleId')
    
    # 1. Transaction Setup & Log Data Preparation
    txn_id = str(uuid.uuid4())
    record_key = title_id
    
    # For a DELETE operation (REDO only), we log the key and the operation type.
    new_value = {"action": "DELETE", "titleId": title_id}

    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    logs = []

    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # 1. "Delete central node first"
        res_central = execute_query('node1', query, params)
        
        if res_central['success'] and res_central['rows_affected'] > 0:
            # Log Local Commit on Node 1 (Central)
            LOG_MANAGER.log_local_commit(txn_id, 'DELETE', record_key, new_value)
            logs.append("Node 1 (Local): Success & Logged")

            # 2. Replicate to Node 2
            LOG_MANAGER.log_replication_attempt(txn_id, 2)
            res_node2 = execute_query('node2', query, params)
            LOG_MANAGER.update_replication_status(txn_id, 2, res_node2['success'])
            
            if res_node2['rows_affected'] > 0:
                logs.append("Node 2 (Fragment): Deleted (Log updated)")
            else:
                logs.append("Node 2 (Fragment): Not Found/Failed (Log updated)")

            # 3. Replicate to Node 3 (Only if not found in Node 2, in the original logic)
            # We follow the original logic and only try Node 3 if Node 2 didn't delete anything.
            if res_node2['rows_affected'] == 0:
                 LOG_MANAGER.log_replication_attempt(txn_id, 3)
                 res_node3 = execute_query('node3', query, params)
                 LOG_MANAGER.update_replication_status(txn_id, 3, res_node3['success'])
                 
                 if res_node3['rows_affected'] > 0:
                     logs.append(f"Node 3 (Fragment): Deleted (Log updated)")
                 else:
                     logs.append(f"Node 3 (Fragment): Target Not Found (Log updated)")
            
        else:
            logs.append(f"Node 1 (Local): Failed/No Rows Affected - {res_central.get('error', '')}")

    # SCENARIO 2 & 3: User is on Fragment
    else:
        current_node_id = int(current_node.replace('node', ''))
        
        # 1. "Update current node first"
        res_local = execute_query(current_node, query, params)
        
        if res_local['rows_affected'] > 0:
            # SCENARIO 2: Data was local
            # Log Local Commit on Fragment Node
            LOG_MANAGER.log_local_commit(txn_id, 'DELETE', record_key, new_value)
            logs.append(f"{current_node} (Local): Deleted & Logged")
            
            # 2. Replicate to Central Node
            LOG_MANAGER.log_replication_attempt(txn_id, 1) # Target is Central Node (ID 1)
            res_central = execute_query('node1', query, params)
            
            # Update Replication Status on the Fragment Node
            LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
            logs.append(f"Node 1 (Central): {'Deleted' if res_central['success'] else 'Failed (Log updated)'}")
            
        else:
            # SCENARIO 3: Data to be deleted is NOT in current node
            logs.append(f"{current_node} (Local): 0 Rows (Remote Data)")
            
            # 1. Update the other fragment
            other_node_key = 'node3' if current_node == 'node2' else 'node2'
            other_node_id = int(other_node_key.replace('node', ''))
            res_other = execute_query(other_node_key, query, params)

            if res_other['rows_affected'] > 0:
                # If the remote delete succeeds, log the commit on the *current node* (acting as coordinator)
                LOG_MANAGER.log_local_commit(txn_id, 'DELETE', record_key, new_value)
                logs.append(f"{other_node_key} (Remote): Deleted & Commit Logged on {current_node}")
                
                # 2. Then delete central node (Replication)
                LOG_MANAGER.log_replication_attempt(txn_id, 1) # Target is Central Node (ID 1)
                res_central = execute_query('node1', query, params)
                
                # Update Replication Status on the calling node (current_node)
                LOG_MANAGER.update_replication_status(txn_id, 1, res_central['success'])
                logs.append(f"Node 1 (Central): {'Deleted' if res_central['success'] else 'Failed (Log updated)'}")
            else:
                logs.append(f"{other_node_key} (Remote): Not Found/Failed")
                # No successful local commit log written if the remote delete failed.

    return jsonify({
        "message": "Delete Processed", 
        "logs": logs,
        "txn_id": txn_id
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)