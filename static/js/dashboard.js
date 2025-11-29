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
    
    // Refresh node status when returning to dashboard
    loadNodeStatus();
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const failureSimulation = document.getElementById('failure-simulation').value;
    const autoCommit = document.getElementById('auto-commit').value // will require an auto-commit element from the frontend
    
    console.log('Applying settings:', { isolationLevel, failureSimulation });
    
    // TODO: Send settings to backend
    // - Apply isolation level to transactions
    // - Disable/Enable autocommit
    // - Configure failure simulation scenarios
    // - Update backend transaction handling
    
    const settingsPayload = {
        isolationLevel: isolationLevel,
        failureSimulation: failureSimulation,
        autoCommit: autoCommit // if implemented
    };
    
    // Send settings to backend
    fetch('/settings', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(settingsPayload),
    })
    .then(response => response.json())
    .then(data => console.log('Backend response:', data))
    .catch((error) => console.error('Error sending settings:', error));
    
    alert(`Settings applied:\nIsolation Level: ${isolationLevel}\nFailure Simulation: ${failureSimulation}\nAuto Commit: ${autoCommit}`);
}

// Load node status from backend
async function loadNodeStatus() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        console.log('Node status:', status);
        
        // Update each node card with real-time data
        for (const [nodeKey, nodeData] of Object.entries(status)) {
            updateNodeCard(nodeKey, nodeData);
        }
        
    } catch (error) {
        console.error('Error loading node status:', error);
        
        // Show error state for all nodes
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            updateNodeCard(nodeKey, {
                status: 'ERROR',
                rows: 0,
                lastUpdate: 'N/A'
            });
        });
    }
}

function updateNodeCard(nodeKey, nodeData) {
    const statusElement = document.getElementById(`${nodeKey}-status`);
    const rowsElement = document.getElementById(`${nodeKey}-rows`);
    const updateElement = document.getElementById(`${nodeKey}-update`);
    
    if (statusElement) {
        statusElement.textContent = nodeData.status;
        
        if (nodeData.status === 'ONLINE') {
            statusElement.className = 'status-indicator status-online';
        } else {
            statusElement.className = 'status-indicator status-offline';
        }
    }
    
    if (rowsElement) {
        rowsElement.textContent = nodeData.rows.toLocaleString();
    }
    
    if (updateElement) {
        updateElement.textContent = nodeData.lastUpdate;
    }
}

// TODO: Implement real-time monitoring
// - Auto-refresh node status every 5-10 seconds
// - Monitor node health metrics
// - Display connection status
// - Show transaction logs
// - Track replication lag
// - Alert on node failures

// Auto-refresh node status (optional)
function startStatusMonitoring() {
    // Initial load
    loadNodeStatus();
    
    // TODO: Uncomment to enable auto-refresh every 10 seconds
    // setInterval(loadNodeStatus, 10000);
}

// Start monitoring when page loads
document.addEventListener('DOMContentLoaded', function() {
    startStatusMonitoring();
});
