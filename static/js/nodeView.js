async function loadNodeData(nodeNumber) {
    try {
        // Fetch movies from backend
        const response = await fetch('/movies');
        const data = await response.json();
        
        console.log('Loaded data for node:', nodeNumber, data);
        
        // Clear existing table rows
        const tableBody = document.getElementById('table-body');
        tableBody.innerHTML = '';
        
        // Check if data is empty
        if (!data || data.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center;">No data available</td></tr>';
            return;
        }
        
        // Populate table with data
        data.forEach(movie => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td title="${movie.titleId || 'N/A'}">${movie.titleId || 'N/A'}</td>
                <td>${movie.ordering || 'N/A'}</td>
                <td title="${movie.title || 'N/A'}">${movie.title || 'N/A'}</td>
                <td>${movie.region || 'N/A'}</td>
                <td>${movie.language || 'N/A'}</td>
                <td title="${movie.types || 'N/A'}">${movie.types || 'N/A'}</td>
                <td title="${movie.attributes || 'N/A'}">${movie.attributes || 'N/A'}</td>
                <td>${movie.isOriginalTitle == 1 ? 'Yes' : 'No'}</td>
                <td>
                    <button onclick="editRow('${movie.titleId}')">Edit</button>
                    <button class="delete-button" onclick="deleteRow('${movie.titleId}')">Delete</button>
                </td>
            `;
            tableBody.appendChild(row);
        });
        
    } catch (error) {
        console.error('Error loading data:', error);
        const tableBody = document.getElementById('table-body');
        tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: red;">Error loading data</td></tr>';
    }
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
    
    // Clear all form fields
    document.getElementById('ordering').value = '';
    document.getElementById('title-name').value = '';
    document.getElementById('title-region').value = '';
    document.getElementById('language').value = '';
    document.getElementById('types').value = '';
    document.getElementById('attributes').value = '';
    document.getElementById('is-original').value = '1';
}

<<<<<<< Updated upstream
function submitInsert() {
=======
async function submitInsert() {
    // Get all form values
    const ordering = document.getElementById('ordering').value;
>>>>>>> Stashed changes
    const titleName = document.getElementById('title-name').value;
    const titleRegion = document.getElementById('title-region').value;
    const language = document.getElementById('language').value;
    const types = document.getElementById('types').value;
    const attributes = document.getElementById('attributes').value;
    const isOriginal = document.getElementById('is-original').value;

    // Validate required fields
    if (!ordering || !titleName || !titleRegion || !language || !types) {
        alert('Please fill in all required fields (marked with *)');
        return;
    }

<<<<<<< Updated upstream
    console.log('Inserting:', { titleName, titleRegion, node: currentNode });
    alert(`Inserting record:\nTitle: ${titleName}\nRegion: ${titleRegion}\nNode: ${currentNode}`);
    
    // TODO: Send insert request to backend
    
    closeInsertModal();
=======
    // Prepare the Payload
    const payload = {
        titleId: "tt" + Math.floor(Math.random() * 10000000), // Random ID
        ordering: parseInt(ordering),
        title: titleName,
        region: titleRegion,
        language: language,
        types: types,
        attributes: attributes || 'N/A',
        isOriginalTitle: parseInt(isOriginal)
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
>>>>>>> Stashed changes
}

function editRow(titleId) {
    console.log('Editing row:', titleId);
    alert(`Edit functionality for row ${titleId} - TODO: Implement`);
    // TODO: Implement edit functionality
}

async function deleteRow(titleId) {
    if (!confirm(`Are you sure you want to delete ${titleId}?`)) {
        return;
    }

    try {
        const response = await fetch('/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ titleId: titleId })
        });

        const result = await response.json();
        
        // Show Feedback
        alert(`Delete Status:\n${result.logs.join('\n')}`);
        
        // Refresh Data
        loadNodeData(currentNode);

    } catch (error) {
        console.error("Delete failed:", error);
        alert("Failed to delete record. Check console for details.");
    }
}

// Close modal when clicking outside - wait for DOM to load
document.addEventListener('DOMContentLoaded', function() {
    const modal = document.getElementById('insert-modal');
    if (modal) {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeInsertModal();
            }
        });
    }
});