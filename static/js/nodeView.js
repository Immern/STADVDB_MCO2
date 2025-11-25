function loadNodeData(nodeNumber) {
    // TODO: Fetch actual data from backend
    console.log('Loading data for node:', nodeNumber);
}

function filterTable() {
    const searchValue = document.getElementById('search-bar').value.toLowerCase();
    const table = document.getElementById('data-table');
    const rows = table.getElementsByTagName('tr');

    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const cells = row.getElementsByTagName('td');
        let found = false;

        for (let j = 0; j < cells.length; j++) {
            const cell = cells[j];
            if (cell.textContent.toLowerCase().includes(searchValue)) {
                found = true;
                break;
            }
        }

        row.style.display = found ? '' : 'none';
    }
}

function openInsertModal() {
    document.getElementById('insert-modal').classList.add('active');
}

function closeInsertModal() {
    document.getElementById('insert-modal').classList.remove('active');
    document.getElementById('title-name').value = '';
    document.getElementById('title-region').value = '';
}

async function submitInsert() {
    const titleName = document.getElementById('title-name').value;
    const titleRegion = document.getElementById('title-region').value;

    if (!titleName || !titleRegion) {
        alert('Please fill in all fields');
        return;
    }

    console.log('Inserting:', { titleName, titleRegion, node: currentNode });
    alert(`Inserting record:\nTitle: ${titleName}\nRegion: ${titleRegion}\nNode: ${currentNode}`);
    
    // 1. Prepare the Payload matching your Schema
    // Since the UI only asks for Name/Region, we generate the rest.
    const payload = {
        titleId: "tt" + Math.floor(Math.random() * 1000000), // Random ID
        ordering: 1,
        title: titleName,
        region: titleRegion,
        language: "en",      // Default
        types: "movie",      // Default
        attributes: "N/A",   // Default
        isOriginalTitle: 1   // Default
    };

    console.log('Sending to backend:', payload);

    try {
        const response = await fetch('/insert', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const result = await response.json();

        // Show Feedback
        alert(`Transaction Status:\n${result.logs.join('\n')}`);
        
        // Refresh Data
        closeInsertModal();
        loadNodeData(currentNode);

    } catch (error) {
        console.error("Insert failed:", error);
        alert("Failed to insert record. Check console for details.");
    }
    
    closeInsertModal();
}

function editRow(id) {
    console.log('Editing row:', id);
    alert(`Edit functionality for row ${id} - TODO: Implement`);
    // TODO: Implement edit functionality
}

// Close modal when clicking outside
document.getElementById('insert-modal').addEventListener('click', function(e) {
    if (e.target === this) {
        closeInsertModal();
    }
});