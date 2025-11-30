let currentNode = null;

// Determine the local node from the backend
async function detectLocalNode() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        // Check if backend provides local node information
        if (status.local_node_id) {
            currentNode = status.local_node_id;
        } else {
            // Default to Node 1 if not specified
            currentNode = 1;
        }
        
        // Update the page title
        const nodeTitle = currentNode === 1 ? 'Node 1 (Central)' : `Node ${currentNode} (Regional)`;
        document.getElementById('current-node-title').textContent = nodeTitle;
        
        console.log(`Local Node detected: ${currentNode}`);
        
        return currentNode;
        
    } catch (error) {
        console.error('Error detecting local node:', error);
        currentNode = 1; // Default to Node 1
        document.getElementById('current-node-title').textContent = 'Node 1 (Central)';
        return 1;
    }
}

function applySettings() {
    const isolationLevel = document.getElementById('isolation-level').value;
    const autoCommitToggle = document.getElementById('autocommit-toggle').checked;
    
    console.log('Applying settings:', { isolationLevel, autoCommit: autoCommitToggle });
    
    // Map frontend values to backend expected format
    const isolationLevelMap = {
        'read-uncommitted': 'READ UNCOMMITTED',
        'read-committed': 'READ COMMITTED',
        'repeatable-read': 'REPEATABLE READ',
        'serializable': 'SERIALIZABLE'
    };
    
    const settingsPayload = {
        isolationLevel: isolationLevelMap[isolationLevel],
        autoCommit: autoCommitToggle.toString()
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
    .then(data => {
        console.log('Backend response:', data);
        const mode = autoCommitToggle ? 'Auto Commit' : 'Manual 2PC';
        alert(`Settings applied successfully!\n\nIsolation Level: ${isolationLevelMap[isolationLevel]}\nTransaction Mode: ${mode}\n\n${data.logs ? data.logs.join('\n') : ''}`);
        
        // Update commit button state
        updateCommitButtonState();
    })
    .catch((error) => {
        console.error('Error sending settings:', error);
        alert('Failed to apply settings. Check console for details.');
    });
}

// Update the state of the Commit Changes button based on autocommit toggle
function updateCommitButtonState() {
    const autoCommitToggle = document.getElementById('autocommit-toggle');
    const commitButton = document.getElementById('commit-changes-btn');
    
    if (autoCommitToggle && commitButton) {
        if (autoCommitToggle.checked) {
            // Auto Commit ON: Disable button
            commitButton.disabled = true;
            commitButton.style.opacity = '0.5';
            commitButton.style.cursor = 'not-allowed';
            commitButton.title = 'Disabled in Auto Commit mode';
        } else {
            // Manual 2PC Mode: Enable button
            commitButton.disabled = false;
            commitButton.style.opacity = '1';
            commitButton.style.cursor = 'pointer';
            commitButton.title = 'View and resolve pending transactions';
        }
    }
}

// Add event listener to toggle for real-time button state update
document.addEventListener('DOMContentLoaded', async function() {
    // Detect local node first
    await detectLocalNode();
    
    // Start status monitoring
    startStatusMonitoring();
    
    // Load initial data for the current node
    loadNodeData(currentNode);
    
    // Load current settings from backend
    await loadCurrentSettings();
    
    // Set up toggle event listener
    const autoCommitToggle = document.getElementById('autocommit-toggle');
    if (autoCommitToggle) {
        autoCommitToggle.addEventListener('change', updateCommitButtonState);
    }
    
    // Initial button state update
    updateCommitButtonState();
});

// Load node status from backend
async function loadNodeStatus() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        console.log('Node status:', status);
        
        // Update each node status card
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            if (status[nodeKey]) {
                updateNodeStatus(nodeKey, status[nodeKey]);
            }
        });
        
    } catch (error) {
        console.error('Error loading node status:', error);
        
        // Show error state for all nodes
        ['node1', 'node2', 'node3'].forEach(nodeKey => {
            updateNodeStatus(nodeKey, {
                status: 'ERROR',
                rows: 0,
                lastUpdate: 'N/A'
            });
        });
    }
}

function updateNodeStatus(nodeKey, nodeData) {
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

// Auto-refresh node status
function startStatusMonitoring() {
    // Initial load
    loadNodeStatus();
    
    // Auto-refresh every 10 seconds
    setInterval(loadNodeStatus, 10000);
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async function() {
    // Detect local node first
    await detectLocalNode();
    
    // Start status monitoring
    startStatusMonitoring();
    
    // Load initial data for the current node
    loadNodeData(currentNode);
    
    // Load current settings from backend
    loadCurrentSettings();
});

// Load current settings from backend and update UI
async function loadCurrentSettings() {
    try {
        const response = await fetch('/status');
        const status = await response.json();
        
        if (status.current_settings) {
            const settings = status.current_settings;
            
            // Update autocommit toggle
            const autoCommitToggle = document.getElementById('autocommit-toggle');
            if (autoCommitToggle && settings.auto_commit !== undefined) {
                autoCommitToggle.checked = settings.auto_commit;
            }
            
            // Update isolation level dropdown
            const isolationSelect = document.getElementById('isolation-level');
            if (isolationSelect && settings.isolation_level) {
                const levelMap = {
                    'READ UNCOMMITTED': 'read-uncommitted',
                    'READ COMMITTED': 'read-committed',
                    'REPEATABLE READ': 'repeatable-read',
                    'SERIALIZABLE': 'serializable'
                };
                isolationSelect.value = levelMap[settings.isolation_level] || 'read-committed';
            }
        }
    } catch (error) {
        console.error('Error loading current settings:', error);
    }
}