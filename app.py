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
    data = request.json

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

    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # "Update central node first, then identify which fragmented node to modify"
        res_central = execute_query('node1', query, params)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'}")
        
        # Then update fragment
        res_frag = execute_query(correct_fragment, query, params)
        logs.append(f"{correct_fragment} (Fragment): {'Success' if res_frag['success'] else 'Failed'}")

    # SCENARIO 2 & 3: User is on a Fragment (Node 2 or 3)
    else:
        # Check if the data belongs here
        if current_node == correct_fragment:
            # SCENARIO 2: Data belongs to current node
            # "Update current node first then update central node"
            res_local = execute_query(current_node, query, params)
            logs.append(f"{current_node} (Local): {'Success' if res_local['success'] else 'Failed'}")
            
            res_central = execute_query('node1', query, params)
            logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed'}")
        else:
            # SCENARIO 3: Data belongs to OTHER fragment
            # "Fetch from other fragmented node" (In insert terms: Insert to other fragment)
            logs.append(f"{current_node} (Local): Skipped (Data belongs to {correct_fragment})")
            
            res_other = execute_query(correct_fragment, query, params)
            logs.append(f"{correct_fragment} (Remote): {'Success' if res_other['success'] else 'Failed'}")
            
            res_central = execute_query('node1', query, params)
            logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed'}")

    return jsonify({"message": "Insert Processed", "logs": logs})

# ROUTE: Update
@app.route('/update', methods=['POST'])
def update_movie():
    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    
    title_id = data.get('titleId')
    new_title = data.get('title')
    new_ordering = data.get('ordering')
    
    query = "UPDATE movies SET title = %s, ordering = %s WHERE titleId = %s"
    params = (new_title, new_ordering, title_id)
    
    logs = []

    # SCENARIO 1: User is on Node 1 (Central)
    if current_node == 'node1':
        # "Update central node first"
        res_central = execute_query('node1', query, params)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'}")
        
        # "Identify which fragmented node to modify"
        # Strategy: We try Node 2. If rows_affected > 0, we found it. If not, try Node 3.
        res_node2 = execute_query('node2', query, params)
        if res_node2['success'] and res_node2['rows_affected'] > 0:
            logs.append(f"Node 2 (Fragment): Updated (Found target)")
        else:
            # Wasn't in Node 2, must be Node 3
            res_node3 = execute_query('node3', query, params)
            logs.append(f"Node 3 (Fragment): {'Updated' if res_node3['rows_affected'] > 0 else 'Target Not Found'}")

    # SCENARIO 2 & 3: User is on Fragment
    else:
        # "Update current node first"
        res_local = execute_query(current_node, query, params)
        
        if res_local['success'] and res_local['rows_affected'] > 0:
            # SCENARIO 2: Data was local
            logs.append(f"{current_node} (Local): Updated")
            # "Then update central node"
            execute_query('node1', query, params)
            logs.append("Node 1 (Central): Updated")
        else:
            # SCENARIO 3: Data to be updated is NOT in current node
            logs.append(f"{current_node} (Local): 0 Rows Affected (Data is Remote)")
            
            # "Fetch from other fragmented node" -> Update the other fragment
            other_node = 'node3' if current_node == 'node2' else 'node2'
            res_other = execute_query(other_node, query, params)
            logs.append(f"{other_node} (Remote): {'Updated' if res_other['rows_affected'] > 0 else 'Not Found'}")
            
            # "Then update central node"
            execute_query('node1', query, params)
            logs.append("Node 1 (Central): Updated")

    return jsonify({"message": "Update Processed", "logs": logs})

# ROUTE: Delete
@app.route('/delete', methods=['POST'])
def delete_movie():
    data = request.json
    current_node = request.args.get('node', data.get('node', 'node1'))
    title_id = data.get('titleId')
    
    query = "DELETE FROM movies WHERE titleId = %s"
    params = (title_id,)
    
    logs = []

    # Logic mirrors update exactly
    if current_node == 'node1':
        res_central = execute_query('node1', query, params)
        logs.append(f"Node 1 (Local): {'Success' if res_central['success'] else 'Failed'}")
        
        # Try Node 2
        res_node2 = execute_query('node2', query, params)
        if res_node2['rows_affected'] > 0:
            logs.append("Node 2 (Fragment): Deleted")
        else:
            res_node3 = execute_query('node3', query, params)
            logs.append(f"Node 3 (Fragment): {'Deleted' if res_node3['rows_affected'] > 0 else 'Not Found'}")

    else:
        res_local = execute_query(current_node, query, params)
        if res_local['rows_affected'] > 0:
            logs.append(f"{current_node} (Local): Deleted")
            execute_query('node1', query, params)
            logs.append("Node 1 (Central): Deleted")
        else:
            logs.append(f"{current_node} (Local): 0 Rows (Remote Data)")
            other_node = 'node3' if current_node == 'node2' else 'node2'
            res_other = execute_query(other_node, query, params)
            logs.append(f"{other_node} (Remote): {'Deleted' if res_other['rows_affected'] > 0 else 'Not Found'}")
            execute_query('node1', query, params)
            logs.append("Node 1 (Central): Deleted")

    return jsonify({"message": "Delete Processed", "logs": logs})

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