document.addEventListener('DOMContentLoaded', () => {
    // Initialize Bootstrap toast
    const toastEl = document.getElementById('toast');
    const toast = new bootstrap.Toast(toastEl, {
        autohide: true,
        delay: 3000
    });

    // Initialize Dropzone
    Dropzone.autoDiscover = false;
    const myDropzone = new Dropzone("#uploadDropzone", {
        url: "/upload",
        autoProcessQueue: false,
        addRemoveLinks: true,
        parallelUploads: 5,
        maxFilesize: 50, // MB
        acceptedFiles: ".csv,.json,.txt,.xlsx,.xls,.parquet",
        dictDefaultMessage: "Drop files here or click to upload",
        dictFileTooBig: "File is too big ({{filesize}}MB). Max filesize: {{maxFilesize}}MB.",
        dictInvalidFileType: "Invalid file type. Allowed types: CSV, JSON, TXT, Excel, Parquet"
    });

    // Handle file addition
    myDropzone.on("addedfile", file => {
        console.log("File added:", file.name);
    });

    // Handle upload success
    myDropzone.on("success", (file, response) => {
        const uploadItem = createUploadListItem(file, response.location, true);
        document.getElementById('uploadList').prepend(uploadItem);
        showToast('File uploaded successfully', 'success');
    });

    // Handle upload error
    myDropzone.on("error", (file, errorMessage) => {
        file.previewElement.classList.add('dz-error');
        showToast(errorMessage.error || 'Upload failed', 'danger');
    });

    // Handle form submission
    myDropzone.on("sending", (file, xhr, formData) => {
        const dataset = document.getElementById('datasetName').value.trim();
        if (!dataset) {
            showToast('Please enter a dataset name', 'danger');
            myDropzone.removeFile(file);
            return;
        }
        formData.append('dataset', dataset);
    });

    // Process uploads when files are added
    myDropzone.on("addedfile", file => {
        const dataset = document.getElementById('datasetName').value.trim();
        if (dataset) {
            myDropzone.processQueue();
        } else {
            showToast('Please enter a dataset name', 'danger');
            myDropzone.removeFile(file);
        }
    });
});

function createUploadListItem(file, location, success) {
    const item = document.createElement('div');
    item.className = 'list-group-item';
    item.innerHTML = `
        <div class="d-flex align-items-center">
            <div class="flex-grow-1">
                <h6 class="mb-0">${file.name}</h6>
                <small class="text-muted">${formatFileSize(file.size)}</small>
            </div>
            <div class="ms-3">
                ${success 
                    ? '<i class="fas fa-check text-success"></i>'
                    : '<i class="fas fa-times text-danger"></i>'}
            </div>
        </div>
    `;
    return item;
}

function getSchema() {
    const datasetInput = document.getElementById('schemaDataset');
    const resultsContainer = document.getElementById('schemaResults');
    const dataset = datasetInput.value.trim();

    if (!dataset) {
        showToast('Please enter a dataset name', 'danger');
        return;
    }

    resultsContainer.innerHTML = `
        <div class="text-center py-3">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
        </div>
    `;

    fetch(`/get-schema?dataset=${encodeURIComponent(dataset)}`)
        .then(response => response.json())
        .then(data => {
            if (data.error) {
                throw new Error(data.error);
            }

            resultsContainer.innerHTML = '';
            if (data.files.length === 0) {
                resultsContainer.innerHTML = `
                    <div class="text-center py-3 text-muted">
                        <i class="fas fa-folder-open fa-3x mb-3"></i>
                        <p>No files found in this dataset</p>
                    </div>
                `;
                return;
            }

            data.files.forEach(file => {
                const fileItem = document.createElement('div');
                fileItem.className = 'list-group-item';
                fileItem.innerHTML = `
                    <h6 class="mb-1">${file.key}</h6>
                    <small class="text-muted">
                        <i class="fas fa-hdd me-1"></i> ${formatFileSize(file.size)}
                        <span class="mx-2">|</span>
                        <i class="fas fa-clock me-1"></i> ${new Date(file.last_modified).toLocaleString()}
                    </small>
                `;
                resultsContainer.appendChild(fileItem);
            });
        })
        .catch(error => {
            resultsContainer.innerHTML = `
                <div class="text-center py-3 text-danger">
                    <i class="fas fa-exclamation-circle fa-3x mb-3"></i>
                    <p>${error.message}</p>
                </div>
            `;
            showToast(error.message, 'danger');
        });
}

function showToast(message, type = 'success') {
    const toastEl = document.getElementById('toast');
    toastEl.className = `toast align-items-center text-white bg-${type} border-0`;
    toastEl.querySelector('.toast-body').textContent = message;
    
    const toast = new bootstrap.Toast(toastEl);
    toast.show();
}

function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
} 