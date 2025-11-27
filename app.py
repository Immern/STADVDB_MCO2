from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector

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
CORS(app) # This allows nodes/browsers to talk to this API



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
        cursor.close()
        conn.close()
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

# Frontend / Homepage
@app.route('/')
def index(): 
    return render_template('index.html')

# ROUTE: Status
@app.route('/status', methods=['GET'])
def node_status():
    status_report = {}
    for key in DB_CONFIG:
        conn = get_db_connection(key)
        if conn:
            status_report[key] = "ONLINE"
            conn.close()
        else:
            status_report[key] = "OFFLINE"
    return jsonify(status_report)

# ROUTE: Read/Search
@app.route('/movies', methods=['GET'])
def get_movies():
    # 1. Check if the frontend sent a search term
    # usage: /movies?q=something
    search_term = request.args.get('q')
    
    conn = get_db_connection('node1')
    if not conn:
        return jsonify({"error": "Central Node Offline"}), 500
    
    cursor = conn.cursor(dictionary=True)
    
    if search_term:
        # 2. If search term exists, filter by ID, Title, OR Region
        # We use wildcards (%) to match partial text (e.g. "Avat" matches "Avatar")
        query = """
            SELECT * FROM movies 
            WHERE titleId LIKE %s 
               OR title LIKE %s 
               OR region LIKE %s 
            LIMIT 100
        """
        # Add wildcards to the search term
        wildcard_term = f"%{search_term}%"
        params = (wildcard_term, wildcard_term, wildcard_term)
        cursor.execute(query, params)
    else:
        # 3. No search term? Return default list
        cursor.execute("SELECT * FROM movies")
        
    rows = cursor.fetchall()
    conn.close()
    
    return jsonify(rows)

# ROUTE: Insert
@app.route('/insert', methods=['POST'])
def insert_movie():
    # Ensure the LOG_MANAGER is available (from global instantiation)
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json

    txn_id = str(uuid.uuid4()) 
    record_key = data.get('titleId')
    
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

    # Determine Partition (Node 2 vs Node 3)
    target_region = data.get('region')
    target_node_key = 'node3' 
    target_node_id = 3 # Use integer ID for the log
    
    if target_region in ['US', 'JP']: 
        target_node_key = 'node2'
        target_node_id = 2
    
    # logs for CONSOLE output
    logs = []
    res_central = execute_query('node1', query, params) 
    
    # Performed operation in the local was a success,
    # therefore we proceed to performing the replication
    if res_central['success']:
        LOG_MANAGER.log_local_commit(txn_id, 'INSERT', record_key, new_value)
        logs.append(f"Node 1 (Central): Success & Logged")
        LOG_MANAGER.log_replication_attempt(txn_id, target_node_id)
        res_fragment = execute_query(target_node_key, query, params)
        LOG_MANAGER.update_replication_status(txn_id, target_node_id, res_fragment['success'])
        logs.append(f"{target_node_key} (Fragment): {'Success' if res_fragment['success'] else 'Failed (Log updated)'}")
        
    else:
        # Local commit failed. Transaction is considered aborted. No log entry for success is written.
        logs.append(f"Node 1 (Central): Failed - {res_central.get('error', '')}")
        
    return jsonify({
        "message": "Transaction Processed",
        "logs": logs,
        "target_node": target_node_key, # Corrected to use target_node_key
        "txn_id": txn_id # Include txn_id in the response for testing/debugging
    })

# ROUTE: Update
@app.route('/update', methods=['POST'])
def update_movie():
    # Ensure the LOG_MANAGER is available (from global instantiation)
    if not LOG_MANAGER:
        return jsonify({"error": "Distributed Log Manager not initialized."}), 500

    data = request.json
    
    # 1. Transaction Setup & Log Data Preparation
    txn_id = str(uuid.uuid4()) 
    record_key = data.get('titleId')
    
    # NOTE: In a real system, you would fetch the 'Before Image' (old_value) 
    # from the database before the update for UNDO operations.
    # For this system, we only create the 'After Image' for REDO.
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
    
    # 2. Prepare Query and Parameters
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    logs = []

    # 3. Update Central Node (Node 1) - Always
    res_central = execute_query('node1', query, params)
    
    if res_central['success']:
        # Log the successful local commit (Redo log)
        LOG_MANAGER.log_local_commit(txn_id, 'UPDATE', record_key, new_value)
        logs.append(f"Node 1 (Central): Success & Logged")

        # 4. Update Fragments (Replication)
        
        # Log and attempt update on Node 2
        LOG_MANAGER.log_replication_attempt(txn_id, 2)
        res_node2 = execute_query('node2', query, params)
        LOG_MANAGER.update_replication_status(txn_id, 2, res_node2['success'])
        logs.append(f"Node 2 Update: {'Success' if res_node2['success'] else 'Failed (Log updated)'}")
        
        # Log and attempt update on Node 3
        LOG_MANAGER.log_replication_attempt(txn_id, 3)
        res_node3 = execute_query('node3', query, params)
        LOG_MANAGER.update_replication_status(txn_id, 3, res_node3['success'])
        logs.append(f"Node 3 Update: {'Success' if res_node3['success'] else 'Failed (Log updated)'}")

    else:
        # Local commit failed. Transaction is considered aborted.
        logs.append(f"Node 1 (Central): Failed - {res_central.get('error', '')}")
        
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
    title_id = data.get('titleId')
    
    # 1. Transaction Setup
    txn_id = str(uuid.uuid4())
    
    # For a DELETE operation, the "After Image" (new_value) is essentially empty 
    # because the record no longer exists. We pass an empty dict or a marker.
    new_value = {"action": "DELETE", "titleId": title_id}

    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    logs = []

    # 2. Delete from Central (Node 1) - Always First
    res_central = execute_query('node1', query, params)
    
    if res_central['success']:
        # --- LOGGING STEP A: Local Commit ---
        # Log that the Primary Node (Node 1) successfully performed the action.
        # This is the "Point of No Return" for the transaction.
        LOG_MANAGER.log_local_commit(txn_id, 'DELETE', title_id, new_value)
        logs.append(f"Node 1 (Central): Success & Logged")

        # --- LOGGING STEP B: Replication to Node 2 ---
        LOG_MANAGER.log_replication_attempt(txn_id, 2)
        res_node2 = execute_query('node2', query, params)
        LOG_MANAGER.update_replication_status(txn_id, 2, res_node2['success'])
        logs.append(f"Node 2 Delete: {'Success' if res_node2['success'] else 'Failed (Log updated)'}")

        # --- LOGGING STEP C: Replication to Node 3 ---
        LOG_MANAGER.log_replication_attempt(txn_id, 3)
        res_node3 = execute_query('node3', query, params)
        LOG_MANAGER.update_replication_status(txn_id, 3, res_node3['success'])
        logs.append(f"Node 3 Delete: {'Success' if res_node3['success'] else 'Failed (Log updated)'}")

    else:
        # If Central Node fails, the whole transaction is aborted.
        logs.append(f"Node 1 (Central): Failed - {res_central.get('error', '')}")

    return jsonify({
        "message": "Delete Processed",
        "logs": logs,
        "txn_id": txn_id
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)