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
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const failureSimulation = document.getElementById('failure-simulation').value;
    
    console.log('Applying settings:', { isolationLevel, failureSimulation });
    alert(`Settings applied:\nIsolation Level: ${isolationLevel}\nFailure Simulation: ${failureSimulation}`);
    
    // TODO: Send settings to backend
}