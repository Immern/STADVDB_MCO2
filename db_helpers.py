import mysql.connector
# Note: You may need to load_dotenv() and define DB_CONFIG here

# Configuration dictionary for connecting to the three MySQL nodes.
# These details are typically loaded from environment variables in a real application.
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

def get_db_connection(node_key, isolation_level=None, autocommit_conn=True):
    """
    Establishes a connection to the specified database node.

    Args:
        node_key (str): The key ('node1', 'node2', or 'node3') corresponding
                        to the configuration in DB_CONFIG.
        isolation_level (str, optional): The transaction isolation level 
                                         (e.g., 'READ COMMITTED', 'SERIALIZABLE'). 
                                         Defaults to None (uses MySQL default).
        autocommit_conn (bool, optional): Whether autocommit should be enabled
                                          on the connection. Defaults to True.
    Returns:
        mysql.connector.connection.MySQLConnection or None: The database 
        connection object or None if connection failed.
    """
    try:
        config = DB_CONFIG[node_key]
        # Establish the connection
        conn = mysql.connector.connect(**config)

        # Set isolation level if specified
        if isolation_level:
            conn.isolation_level = isolation_level
            
        # Set autocommit behavior
        conn.autocommit = autocommit_conn

        return conn
    except Exception as e:
        # In a production environment, this should log the full traceback
        print(f"Error connecting to {node_key}: {e}")
        return None