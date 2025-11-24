from flask import Flask, render_template

# Initialize the Flask application
app = Flask(__name__)

# Define the route for the homepage ("/")
@app.route('/')
def index():
    # Flask looks inside the 'templates' folder for index.html
    return render_template('index.html')

# This block allows you to run the app directly
if __name__ == '__main__':
    # Set host to '0.0.0.0' for external access (needed for Proxmox/VMs)
    app.run(host='0.0.0.0', port=80)