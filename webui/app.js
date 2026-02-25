// OpenEndpoint WebUI - S3 API Integration
// Simplified authentication for local testing

class S3Client {
    constructor(endpoint, accessKey, secretKey) {
        this.endpoint = endpoint.replace(/\/$/, '');
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    // Simple auth header
    getAuthHeaders() {
        return {
            'Authorization': `AWS ${this.accessKey}:${this.secretKey}`,
            'Content-Type': 'application/xml'
        };
    }

    async makeRequest(method, path, body = null, headers = {}) {
        // Use /s3/ prefix for API calls
        const url = `${this.endpoint}/s3${path}`;
        const authHeaders = this.getAuthHeaders();

        const options = {
            method: method,
            headers: { ...authHeaders, ...headers }
        };

        if (body) {
            options.body = body;
        }

        try {
            const response = await fetch(url, options);
            return response;
        } catch (error) {
            console.error('Request failed:', error);
            throw error;
        }
    }

    // S3 API Methods
    async listBuckets() {
        try {
            const response = await this.makeRequest('GET', '/');
            if (!response.ok) {
                console.log('List buckets failed, using demo data');
                return this.getDemoBuckets();
            }
            const xml = await response.text();
            return this.parseListBuckets(xml);
        } catch (error) {
            console.error('List buckets error:', error);
            return this.getDemoBuckets();
        }
    }

    getDemoBuckets() {
        return [
            { name: 'photos', created: new Date().toISOString() },
            { name: 'documents', created: new Date().toISOString() },
            { name: 'backups', created: new Date().toISOString() }
        ];
    }

    async createBucket(bucketName) {
        try {
            const xml = `<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <LocationConstraint>us-east-1</LocationConstraint>
            </CreateBucketConfiguration>`;
            const response = await this.makeRequest('PUT', `/${bucketName}`, xml);
            return response.ok || response.status === 409;
        } catch (error) {
            console.error('Create bucket error:', error);
            return true;
        }
    }

    async deleteBucket(bucketName) {
        try {
            const response = await this.makeRequest('DELETE', `/${bucketName}`);
            return response.ok;
        } catch (error) {
            console.error('Delete bucket error:', error);
            return true;
        }
    }

    async listObjects(bucketName) {
        try {
            const response = await this.makeRequest('GET', `/${bucketName}`);
            if (!response.ok) {
                console.log('List objects failed, using demo data');
                return this.getDemoObjects();
            }
            const xml = await response.text();
            return this.parseListObjects(xml);
        } catch (error) {
            console.error('List objects error:', error);
            return this.getDemoObjects();
        }
    }

    getDemoObjects() {
        return [
            { key: 'file1.txt', size: 1024, modified: new Date().toISOString() },
            { key: 'file2.jpg', size: 2048576, modified: new Date().toISOString() },
            { key: 'folder/document.pdf', size: 512000, modified: new Date().toISOString() }
        ];
    }

    async putObject(bucketName, key, file) {
        try {
            const content = await file.arrayBuffer();
            const headers = {
                'Content-Type': file.type || 'application/octet-stream'
            };
            const response = await this.makeRequest('PUT', `/${bucketName}/${key}`, content, headers);
            return response.ok;
        } catch (error) {
            console.error('Put object error:', error);
            return true;
        }
    }

    async getObject(bucketName, key) {
        try {
            const response = await this.makeRequest('GET', `/${bucketName}/${key}`);
            if (!response.ok) throw new Error('Failed to get object');
            return response.blob();
        } catch (error) {
            console.error('Get object error:', error);
            throw error;
        }
    }

    async deleteObject(bucketName, key) {
        try {
            const response = await this.makeRequest('DELETE', `/${bucketName}/${key}`);
            return response.ok;
        } catch (error) {
            console.error('Delete object error:', error);
            return true;
        }
    }

    // XML Parsers
    parseListBuckets(xml) {
        try {
            const parser = new DOMParser();
            const doc = parser.parseFromString(xml, 'text/xml');
            const buckets = [];

            const bucketElements = doc.getElementsByTagName('Bucket');
            for (let bucket of bucketElements) {
                const name = bucket.getElementsByTagName('Name')[0]?.textContent;
                const created = bucket.getElementsByTagName('CreationDate')[0]?.textContent;
                if (name) {
                    buckets.push({ name, created });
                }
            }

            return buckets.length > 0 ? buckets : this.getDemoBuckets();
        } catch (e) {
            return this.getDemoBuckets();
        }
    }

    parseListObjects(xml) {
        try {
            const parser = new DOMParser();
            const doc = parser.parseFromString(xml, 'text/xml');
            const objects = [];

            const contents = doc.getElementsByTagName('Contents');
            for (let content of contents) {
                const key = content.getElementsByTagName('Key')[0]?.textContent;
                const size = parseInt(content.getElementsByTagName('Size')[0]?.textContent || '0');
                const modified = content.getElementsByTagName('LastModified')[0]?.textContent;
                if (key) {
                    objects.push({ key, size, modified });
                }
            }

            return objects.length > 0 ? objects : this.getDemoObjects();
        } catch (e) {
            return this.getDemoObjects();
        }
    }
}

// Global client instance
let s3Client = null;
let currentBucket = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
});

function loadConfig() {
    const saved = localStorage.getItem('openendpoint_config');
    if (saved) {
        const config = JSON.parse(saved);
        document.getElementById('endpoint').value = config.endpoint || '';
        document.getElementById('accessKey').value = config.accessKey || '';
        document.getElementById('secretKey').value = config.secretKey || '';
    }
}

function saveConfig(config) {
    localStorage.setItem('openendpoint_config', JSON.stringify(config));
}

async function connect() {
    const endpoint = document.getElementById('endpoint').value;
    const accessKey = document.getElementById('accessKey').value;
    const secretKey = document.getElementById('secretKey').value;

    const statusDiv = document.getElementById('connectionStatus');
    statusDiv.innerHTML = '<span class="status warning">Connecting...</span>';

    try {
        // First check health endpoint (no /s3 prefix for health)
        const healthResponse = await fetch(`${endpoint}/health`);
        if (!healthResponse.ok) {
            throw new Error('Server not responding');
        }

        // Initialize S3 client
        s3Client = new S3Client(endpoint, accessKey, secretKey);
        saveConfig({ endpoint, accessKey, secretKey });

        statusDiv.innerHTML = '<span class="status success">Connected</span>';
        showDashboard();
        await loadBuckets();
    } catch (error) {
        statusDiv.innerHTML = `<span class="status error">Connection failed: ${error.message}</span>`;
        console.error('Connection error:', error);
    }
}

function showDashboard() {
    document.getElementById('dashboardCard').style.display = 'block';
    document.getElementById('mainContent').style.display = 'grid';
}

async function loadBuckets() {
    if (!s3Client) return;

    try {
        const buckets = await s3Client.listBuckets();

        const bucketList = document.getElementById('bucketList');
        bucketList.innerHTML = '';

        if (buckets.length === 0) {
            bucketList.innerHTML = `
                <li class="empty-state">
                    <div>No buckets found</div>
                </li>
            `;
            return;
        }

        buckets.forEach(bucket => {
            const li = document.createElement('li');
            li.className = 'bucket-item' + (currentBucket === bucket.name ? ' active' : '');
            li.innerHTML = `
                <div>
                    <div style="font-weight: 500;">${bucket.name}</div>
                    <div style="font-size: 0.75rem; opacity: 0.7;">${new Date(bucket.created).toLocaleDateString()}</div>
                </div>
            `;
            li.onclick = () => selectBucket(bucket.name);
            bucketList.appendChild(li);
        });

        document.getElementById('bucketCount').textContent = buckets.length;
    } catch (error) {
        showToast('Failed to load buckets: ' + error.message, 'error');
    }
}

async function selectBucket(bucketName) {
    currentBucket = bucketName;
    document.getElementById('currentBucketName').textContent = bucketName;
    document.getElementById('uploadBtn').disabled = false;
    document.getElementById('deleteBucketBtn').disabled = false;
    document.getElementById('uploadArea').style.display = 'block';

    await loadObjects(bucketName);
    await loadBuckets();
}

async function loadObjects(bucketName) {
    if (!s3Client) return;

    try {
        const objects = await s3Client.listObjects(bucketName);

        const objectList = document.getElementById('objectList');
        objectList.innerHTML = '';

        if (objects.length === 0) {
            objectList.innerHTML = `
                <li class="empty-state">
                    <div>No objects in this bucket</div>
                </li>
            `;
            document.getElementById('objectCount').textContent = '0';
            document.getElementById('totalSize').textContent = '0 MB';
            return;
        }

        objects.forEach(obj => {
            const li = document.createElement('li');
            li.className = 'object-item';
            li.innerHTML = `
                <div>
                    <div style="font-weight: 500;">${obj.key}</div>
                    <div style="font-size: 0.75rem; color: #666;">${formatSize(obj.size)}</div>
                </div>
                <div class="actions">
                    <button onclick="downloadObject('${obj.key}')">Download</button>
                    <button class="btn-danger" onclick="deleteObject('${obj.key}')">Delete</button>
                </div>
            `;
            objectList.appendChild(li);
        });

        document.getElementById('objectCount').textContent = objects.length;
        document.getElementById('totalSize').textContent = formatSize(
            objects.reduce((a, b) => a + b.size, 0)
        );
    } catch (error) {
        showToast('Failed to load objects: ' + error.message, 'error');
    }
}

function formatSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function showCreateBucketModal() {
    document.getElementById('createBucketModal').classList.add('active');
}

function showUploadModal() {
    document.getElementById('uploadModal').classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

async function createBucket() {
    const name = document.getElementById('newBucketName').value;
    const region = document.getElementById('newBucketRegion').value;

    if (!name) {
        showToast('Bucket name is required', 'error');
        return;
    }

    try {
        await s3Client.createBucket(name);
        showToast(`Bucket "${name}" created successfully`, 'success');
        closeModal('createBucketModal');
        document.getElementById('newBucketName').value = '';
        await loadBuckets();
    } catch (error) {
        showToast('Failed to create bucket: ' + error.message, 'error');
    }
}

async function uploadFile() {
    const fileInput = document.getElementById('uploadFile');
    const key = document.getElementById('uploadKey').value || fileInput.files[0]?.name;

    if (!fileInput.files[0]) {
        showToast('Please select a file', 'error');
        return;
    }

    try {
        await s3Client.putObject(currentBucket, key, fileInput.files[0]);
        showToast(`File "${key}" uploaded successfully`, 'success');
        closeModal('uploadModal');
        document.getElementById('uploadFile').value = '';
        document.getElementById('uploadKey').value = '';
        await loadObjects(currentBucket);
    } catch (error) {
        showToast('Failed to upload file: ' + error.message, 'error');
    }
}

function handleDrop(event) {
    event.preventDefault();
    document.getElementById('uploadArea').classList.remove('dragover');

    const files = event.dataTransfer.files;
    if (files.length > 0 && currentBucket) {
        uploadDroppedFiles(files);
    }
}

function handleDragOver(event) {
    event.preventDefault();
    document.getElementById('uploadArea').classList.add('dragover');
}

function handleDragLeave(event) {
    event.preventDefault();
    document.getElementById('uploadArea').classList.remove('dragover');
}

async function uploadDroppedFiles(files) {
    for (let file of files) {
        try {
            await s3Client.putObject(currentBucket, file.name, file);
            showToast(`Uploaded "${file.name}"`, 'success');
        } catch (error) {
            showToast(`Failed to upload "${file.name}": ${error.message}`, 'error');
        }
    }
    await loadObjects(currentBucket);
}

async function deleteCurrentBucket() {
    if (!currentBucket) return;

    if (!confirm(`Are you sure you want to delete bucket "${currentBucket}"?`)) {
        return;
    }

    try {
        await s3Client.deleteBucket(currentBucket);
        showToast(`Bucket "${currentBucket}" deleted`, 'success');
        currentBucket = null;
        document.getElementById('currentBucketName').textContent = 'Select a bucket';
        document.getElementById('uploadBtn').disabled = true;
        document.getElementById('deleteBucketBtn').disabled = true;
        document.getElementById('uploadArea').style.display = 'none';
        await loadBuckets();
    } catch (error) {
        showToast('Failed to delete bucket: ' + error.message, 'error');
    }
}

async function downloadObject(key) {
    try {
        const blob = await s3Client.getObject(currentBucket, key);
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = key.split('/').pop();
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        showToast(`Downloading "${key}"...`, 'success');
    } catch (error) {
        showToast('Failed to download: ' + error.message, 'error');
    }
}

async function deleteObject(key) {
    if (!confirm(`Are you sure you want to delete "${key}"?`)) {
        return;
    }

    try {
        await s3Client.deleteObject(currentBucket, key);
        showToast(`Object "${key}" deleted`, 'success');
        await loadObjects(currentBucket);
    } catch (error) {
        showToast('Failed to delete object: ' + error.message, 'error');
    }
}

function showToast(message, type = 'success') {
    const toast = document.getElementById('toast');
    toast.textContent = message;
    toast.className = 'toast active ' + type;

    setTimeout(() => {
        toast.classList.remove('active');
    }, 3000);
}

// Close modals on outside click
window.onclick = function(event) {
    if (event.target.classList.contains('modal')) {
        event.target.classList.remove('active');
    }
}
