from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import mysql.connector

# Initialize the Flask application
app = Flask(__name__)
CORS(app) # This allows your other nodes/browsers to talk to this API

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

# ROUTE: Read / Search
@app.route('/movies', methods=['GET'])
def get_movies():
    # Read from Node 1 (Central)
    conn = get_db_connection('node1')
    if not conn:
        return jsonify({"error": "Central Node Offline"}), 500
    
    cursor = conn.cursor(dictionary=True)
    # Limit to 100 so we don't crash the browser if DB is huge
    cursor.execute("SELECT * FROM movies LIMIT 100") 
    rows = cursor.fetchall()
    conn.close()
    return jsonify(rows)

# ROUTE: Insert
@app.route('/insert', methods=['POST'])
def insert_movie():
    data = request.json

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
    # US and JP go to Node 2, the rest to Node 3
    target_region = data.get('region')
    target_node = 'node3' 
    
    if target_region in ['US', 'JP']: 
        target_node = 'node2'
    
    logs = []

    res_central = execute_query('node1', query, params)
    logs.append(f"Node 1 (Central): {'Success' if res_central['success'] else 'Failed ' + res_central.get('error', '')}")

    res_fragment = execute_query(target_node, query, params)
    logs.append(f"{target_node} (Fragment): {'Success' if res_fragment['success'] else 'Failed ' + res_fragment.get('error', '')}")

    return jsonify({
        "message": "Transaction Processed",
        "logs": logs,
        "target_node": target_node
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)