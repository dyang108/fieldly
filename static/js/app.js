document.addEventListener('DOMContentLoaded', () => {
    // Initialize Bootstrap toast
    const toastEl = document.getElementById('toast');
    const toast = new bootstrap.Toast(toastEl, {
        autohide: true,
        delay: 3000
    });

    const submitButton = document.getElementById('submitButton');
    const clearButton = document.getElementById('clearButton');
    const clearHistoryButton = document.getElementById('clearHistoryButton');
    const datasetInput = document.getElementById('datasetName');

    // Initialize Dropzone
    Dropzone.autoDiscover = false;
    const myDropzone = new Dropzone("#uploadDropzone", {
        url: "/upload",
        autoProcessQueue: true, // Enable auto upload
        uploadMultiple: false,
        addRemoveLinks: true,
        parallelUploads: 5,
        maxFilesize: 50, // MB
        acceptedFiles: ".csv,.json,.txt,.xlsx,.xls,.parquet,.pdf",
        dictDefaultMessage: "Set dataset name, then drop files here or click to browse",
        dictFileTooBig: "File is too big ({{filesize}}MB). Max filesize: {{maxFilesize}}MB.",
        dictInvalidFileType: "Invalid file type. Allowed types: CSV, JSON, TXT, Excel, Parquet, PDF",
        init: function() {
            const dz = this;

            // Check dataset name before accepting files
            this.on("addedfile", function(file) {
                const dataset = datasetInput.value.trim();
                if (!dataset) {
                    this.removeFile(file);
                    showToast('Please enter a dataset name before uploading files', 'danger');
                    return;
                }
                console.log("File added:", file.name);
                // Lock dataset name once upload starts
                datasetInput.disabled = true;
            });

            this.on("removedfile", function(file) {
                console.log("File removed:", file.name);
                // If no more files, unlock dataset name
                if (this.files.length === 0) {
                    datasetInput.disabled = false;
                }
            });

            this.on("sending", function(file, xhr, formData) {
                console.log("Sending file:", file.name);
                // Add the dataset parameter to the formData
                formData.append("dataset", datasetInput.value.trim());
            });

            this.on("success", function(file, response) {
                console.log("File uploaded successfully:", file.name);
                const uploadItem = createUploadListItem(file, response.location, true);
                document.getElementById('uploadList').prepend(uploadItem);
                showToast('File uploaded successfully', 'success');
            });

            this.on("error", function(file, errorMessage) {
                console.log("Upload error:", file.name, errorMessage);
                file.previewElement.classList.add('dz-error');
                const error = errorMessage.error || (typeof errorMessage === 'string' ? errorMessage : 'Upload failed');
                showToast(error, 'danger');
                // If error occurs, allow changing dataset name
                if (this.files.length === 0) {
                    datasetInput.disabled = false;
                }
            });

            this.on("queuecomplete", function() {
                console.log("Queue complete");
            });
        }
    });

    // Clear button now also unlocks dataset name
    clearButton.addEventListener('click', () => {
        myDropzone.removeAllFiles(true);
        datasetInput.disabled = false;
    });

    // Handle the clear history button click
    clearHistoryButton.addEventListener('click', () => {
        const uploadList = document.getElementById('uploadList');
        uploadList.innerHTML = '';
    });
});

function createUploadListItem(file, location, success) {
    const item = document.createElement('div');
    item.className = 'list-group-item';
    item.innerHTML = `
        <div class="d-flex align-items-center">
            <div class="flex-grow-1">
                <h6 class="mb-0">${file.name}</h6>
                <small class="text-muted">
                    ${formatFileSize(file.size)} | Location: ${location}
                </small>
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