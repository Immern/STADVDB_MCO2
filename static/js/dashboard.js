let currentNode = null;

// Load node status on page load
document.addEventListener('DOMContentLoaded', function() {
    updateNodeStatus();
    // Refresh status every 5 seconds
    setInterval(updateNodeStatus, 5000);
});

async function updateNodeStatus() {
    try {
        const response = await fetch('/status');
        const statusData = await response.json();
        
        // Update Node 1
        updateNodeCard('node1', statusData.node1);
        
        // Update Node 2
        updateNodeCard('node2', statusData.node2);
        
        // Update Node 3
        updateNodeCard('node3', statusData.node3);
        
    } catch (error) {
        console.error('Error fetching node status:', error);
    }
}

function updateNodeCard(nodeKey, status) {
    const statusElement = document.getElementById(`${nodeKey}-status`);
    const updateElement = document.getElementById(`${nodeKey}-update`);
    
    if (status === 'ONLINE') {
        statusElement.textContent = 'ONLINE';
        statusElement.className = 'status-indicator status-online';
        updateElement.textContent = new Date().toLocaleString();
    } else {
        statusElement.textContent = 'OFFLINE';
        statusElement.className = 'status-indicator status-offline';
        updateElement.textContent = 'N/A';
    }
}

function navigateToNode(nodeNumber) {
    currentNode = nodeNumber;
    document.getElementById('dashboard-view').style.display = 'none';
    document.getElementById('node-view').classList.add('active');
    document.getElementById('current-node-title').textContent = 
        nodeNumber === 1 ? 'Node 1 (Central)' : `Node ${nodeNumber} (Regional)`;
    
    // Reset page counter
    currentPage = 1;
    
    // Load data for the selected node
    loadNodeData(nodeNumber);
}

function navigateToDashboard() {
    document.getElementById('node-view').classList.remove('active');
    document.getElementById('dashboard-view').style.display = 'block';
    currentNode = null;
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const failureSimulation = document.getElementById('failure-simulation').value;
    
    console.log('Applying settings:', { isolationLevel, failureSimulation });
    alert(`Settings applied:\nIsolation Level: ${isolationLevel}\nFailure Simulation: ${failureSimulation}`);
    
    // TODO: Send settings to backend
}