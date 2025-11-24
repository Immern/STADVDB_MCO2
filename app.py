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

# Define the route for the homepage ("/")
@app.route('/')
def index():
    # Flask looks inside the 'templates' folder for index.html
    return render_template('index.html')

# --- ROUTE 2: API Status (The Logic) ---
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

# --- ROUTE 3: API Insert (The Transaction) ---
@app.route('/insert', methods=['POST'])
def insert_data():
    data = request.json
    # We will implement the complex SQL logic here later
    return jsonify({"message": "Data received by Backend", "data": data})

# This block allows you to run the app directly
if __name__ == '__main__':
    # Set host to '0.0.0.0' for external access (needed for Proxmox/VMs)
    app.run(host='0.0.0.0', port=80)