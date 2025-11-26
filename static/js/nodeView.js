let currentPage = 1;
let totalRows = 0;
const ROWS_PER_PAGE = 100;

async function loadNodeData(nodeNumber, page = 1) {
    try {
        // Get filter values
        const titleIdFilter = document.getElementById('filter-titleid').value;
        const titleFilter = document.getElementById('filter-title').value;
        const regionFilter = document.getElementById('filter-region').value;

        // Build query parameters
        const params = new URLSearchParams({
            page: page,
            limit: ROWS_PER_PAGE
        });

        if (titleIdFilter) params.append('titleId', titleIdFilter);
        if (titleFilter) params.append('title', titleFilter);
        if (regionFilter) params.append('region', regionFilter);

        // Fetch movies from backend
        const response = await fetch(`/movies?${params.toString()}`);
        const result = await response.json();
        
        console.log('Loaded data for node:', nodeNumber, result);
        
        // Update total rows count
        totalRows = result.total || result.data?.length || 0;
        document.getElementById('total-rows').textContent = `Total Rows: ${totalRows}`;
        
        const data = result.data || result;
        
        // Clear existing table rows
        const tableBody = document.getElementById('table-body');
        tableBody.innerHTML = '';
        
        // Check if data is empty
        if (!data || data.length === 0) {
            tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center;">No data available</td></tr>';
            document.getElementById('load-more-btn').style.display = 'none';
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
                    <button onclick="openEditModal('${movie.titleId}', '${escapeHtml(movie.title)}')">Edit</button>
                    <button class="delete-button" onclick="deleteRow('${movie.titleId}')">Delete</button>
                </td>
            `;
            tableBody.appendChild(row);
        });

        // Show/hide load more button
        const loadMoreBtn = document.getElementById('load-more-btn');
        if (data.length >= ROWS_PER_PAGE) {
            loadMoreBtn.style.display = 'block';
        } else {
            loadMoreBtn.style.display = 'none';
        }
        
    } catch (error) {
        console.error('Error loading data:', error);
        const tableBody = document.getElementById('table-body');
        tableBody.innerHTML = '<tr><td colspan="9" style="text-align: center; color: red;">Error loading data</td></tr>';
    }
}

function escapeHtml(text) {
    if (!text) return '';
    return text.replace(/'/g, "\\'").replace(/"/g, '&quot;');
}

function loadMoreRows() {
    currentPage++;
    loadNodeData(currentNode, currentPage);
}

function applyFilters() {
    currentPage = 1;
    loadNodeData(currentNode, currentPage);
}

function openInsertModal() {
    document.getElementById('insert-modal').classList.add('active');
}

function closeInsertModal() {
    document.getElementById('insert-modal').classList.remove('active');
    
    // Clear all form fields
    document.getElementById('insert-titleid').value = '';
    document.getElementById('ordering').value = '';
    document.getElementById('title-name').value = '';
    document.getElementById('title-region').value = '';
    document.getElementById('language').value = '';
    document.getElementById('types').value = '';
    document.getElementById('attributes').value = '';
    document.getElementById('is-original').value = '1';
}

async function submitInsert() {
    // Get all form values
    const titleId = document.getElementById('insert-titleid').value;
    const ordering = document.getElementById('ordering').value;
    const titleName = document.getElementById('title-name').value;
    const titleRegion = document.getElementById('title-region').value;
    const language = document.getElementById('language').value;
    const types = document.getElementById('types').value;
    const attributes = document.getElementById('attributes').value;
    const isOriginal = document.getElementById('is-original').value;

    // Validate required fields
    if (!titleId || !ordering || !titleName || !titleRegion || !language || !types) {
        alert('Please fill in all required fields (marked with *)');
        return;
    }

    // Prepare the Payload
    const payload = {
        titleId: titleId,
        ordering: parseInt(ordering),
        title: titleName,
        region: titleRegion,
        language: language,
        types: types,
        attributes: attributes || null,
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
        currentPage = 1;
        loadNodeData(currentNode);

    } catch (error) {
        console.error("Insert failed:", error);
        alert("Failed to insert record. Check console for details.");
    }
}

function openEditModal(titleId, currentTitle) {
    // Unescape the title
    currentTitle = currentTitle.replace(/\\'/g, "'");
    
    document.getElementById('edit-modal').classList.add('active');
    document.getElementById('edit-titleid').value = titleId;
    document.getElementById('edit-title').value = currentTitle;
}

function closeEditModal() {
    document.getElementById('edit-modal').classList.remove('active');
    document.getElementById('edit-titleid').value = '';
    document.getElementById('edit-title').value = '';
}

async function submitEdit() {
    const titleId = document.getElementById('edit-titleid').value;
    const newTitle = document.getElementById('edit-title').value;

    if (!newTitle) {
        alert('Please enter a title');
        return;
    }

    const payload = {
        titleId: titleId,
        title: newTitle,
        ordering: 1 // Required by backend but not changed
    };

    console.log('Updating:', payload);

    try {
        const response = await fetch('/update', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const result = await response.json();
        
        // Show Feedback
        alert(`Update Status:\n${result.logs.join('\n')}`);
        
        // Refresh Data
        closeEditModal();
        loadNodeData(currentNode);

    } catch (error) {
        console.error("Update failed:", error);
        alert("Failed to update record. Check console for details.");
    }
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

function simulateConcurrency() {
    alert('Simulate Concurrency - Feature coming soon!\nThis will test concurrent transactions across nodes.');
    // TODO: Implement concurrency simulation
}

// Close modals when clicking outside - wait for DOM to load
document.addEventListener('DOMContentLoaded', function() {
    const insertModal = document.getElementById('insert-modal');
    if (insertModal) {
        insertModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeInsertModal();
            }
        });
    }

    const editModal = document.getElementById('edit-modal');
    if (editModal) {
        editModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeEditModal();
            }
        });
    }
});