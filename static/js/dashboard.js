let currentNode = null;

function navigateToNode(nodeNumber) {
    currentNode = nodeNumber;
    document.getElementById('dashboard-view').style.display = 'none';
    document.getElementById('node-view').classList.add('active');
    document.getElementById('current-node-title').textContent = 
        nodeNumber === 1 ? 'Node 1 (Central)' : `Node ${nodeNumber} (Regional)`;
    
    // Load data for the selected node
    loadNodeData(nodeNumber);
}

function navigateToDashboard() {
    document.getElementById('node-view').classList.remove('active');
    document.getElementById('dashboard-view').style.display = 'block';
    currentNode = null;
    updateNodeStatus();
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const failureSimulation = document.getElementById('failure-simulation').value;
    
    console.log('Applying settings:', { isolationLevel, failureSimulation });
    alert(`Settings applied:\nIsolation Level: ${isolationLevel}\nFailure Simulation: ${failureSimulation}`);
    
    // TODO: Send settings to backend
}

async function updateNodeStatus() {
    try {
        const response = await fetch('/status');
        const data = await response.json();
        
        // Update Node 1
        updateCard('node1', data.node1);
        // Update Node 2
        updateCard('node2', data.node2);
        // Update Node 3
        updateCard('node3', data.node3);
        
    } catch (error) {
        console.error("Error fetching status:", error);
    }
}

function updateCard(nodeId, status) {
    const badge = document.getElementById(`${nodeId}-status`);
    if (status === 'ONLINE') {
        badge.className = 'status-indicator status-online';
        badge.textContent = 'ONLINE';
    } else {
        badge.className = 'status-indicator status-offline';
        badge.textContent = 'OFFLINE';
    }
}

updateNodeStatus();
setInterval(updateNodeStatus, 5000);