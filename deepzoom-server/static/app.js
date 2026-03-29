// DeepZoom Viewer Application

// Path utilities
const PathUtil = {
    encode(path) {
        if (path.match(/^https?:\/\//i) || path.match(/^s3:\/\//i)) {
            return 'base64:' + btoa(unescape(encodeURIComponent(path)));
        }
        return path.split('/').map(segment => encodeURIComponent(segment)).join('/');
    },
    decode(encoded) {
        if (encoded.startsWith('base64:')) {
            return decodeURIComponent(escape(atob(encoded.slice(7))));
        }
        return decodeURIComponent(encoded);
    }
};

// Viewer Manager
class ViewerManager {
    constructor() {
        this.viewerLocal = null;
        this.viewerRemote = null;
        this.syncEnabled = true;
        this.statsVisible = false;
        this.ws = null;
        this.stats = { local: null, remote: null };
        
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.connectWebSocket();
        this.makeDraggable(document.getElementById('statsPanel'), document.getElementById('statsPanelHeader'));
    }

    setupEventListeners() {
        document.getElementById('loadBtn').addEventListener('click', () => this.loadSlides());
        document.getElementById('syncToggle').addEventListener('click', () => this.toggleSync());
        document.getElementById('statsToggle').addEventListener('click', () => this.toggleStats());
        document.getElementById('closeStats').addEventListener('click', () => this.toggleStats());

        // Enter key to load
        ['localPath', 'remotePath'].forEach(id => {
            document.getElementById(id).addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.loadSlides();
            });
        });

        // Example buttons
        document.querySelectorAll('.example-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const localPath = btn.dataset.local || '';
                const remotePath = btn.dataset.remote || '';
                if (localPath) document.getElementById('localPath').value = localPath;
                if (remotePath) document.getElementById('remotePath').value = remotePath;
                this.loadSlides();
            });
        });
    }

    async loadSlides() {
        const localPath = document.getElementById('localPath').value.trim();
        const remotePath = document.getElementById('remotePath').value.trim();

        if (localPath) {
            await this.loadViewer('local', localPath);
        }
        if (remotePath) {
            await this.loadViewer('remote', remotePath);
        }
    }

    async loadViewer(type, path) {
        const containerId = type === 'local' ? 'viewerLocal' : 'viewerRemote';
        const encodedPath = PathUtil.encode(path);
        const dziUrl = `/dzi/${encodedPath}`;

        // Destroy existing viewer
        if (type === 'local' && this.viewerLocal) {
            this.viewerLocal.destroy();
        } else if (type === 'remote' && this.viewerRemote) {
            this.viewerRemote.destroy();
        }

        const viewer = OpenSeadragon({
            id: containerId,
            prefixUrl: 'https://cdnjs.cloudflare.com/ajax/libs/openseadragon/4.1.0/images/',
            tileSources: dziUrl,
            showNavigator: true,
            navigatorPosition: 'BOTTOM_RIGHT',
            showNavigationControl: true,
            navigationControlAnchor: OpenSeadragon.ControlAnchor.TOP_LEFT,
            animationTime: 0.5,
            blendTime: 0.1,
            constrainDuringPan: true,
            maxZoomPixelRatio: 2,
            minZoomImageRatio: 0.8,
            visibilityRatio: 0.5,
            zoomPerScroll: 1.2,
            crossOriginPolicy: 'Anonymous',
        });

        if (type === 'local') {
            this.viewerLocal = viewer;
        } else {
            this.viewerRemote = viewer;
        }

        // Setup sync handlers
        this.setupSyncHandlers(viewer, type);

        // Fetch and display metadata
        this.fetchMetadata(encodedPath, type);
    }

    setupSyncHandlers(viewer, type) {
        const other = type === 'local' ? this.viewerRemote : this.viewerLocal;
        
        let syncing = false;

        viewer.addHandler('viewport-change', () => {
            if (!this.syncEnabled || !other || syncing) return;
            
            syncing = true;
            const bounds = viewer.viewport.getBounds();
            const zoom = viewer.viewport.getZoom();
            const center = viewer.viewport.getCenter();
            
            other.viewport.zoomTo(zoom, null, true);
            other.viewport.panTo(center, true);
            syncing = false;
        });
    }

    async fetchMetadata(encodedPath, type) {
        try {
            const response = await fetch(`/api/metadata/${encodedPath}`);
            if (response.ok) {
                const metadata = await response.json();
                console.log(`${type} metadata:`, metadata);
            }
        } catch (err) {
            console.error(`Failed to fetch ${type} metadata:`, err);
        }
    }

    toggleSync() {
        this.syncEnabled = !this.syncEnabled;
        const btn = document.getElementById('syncToggle');
        btn.textContent = `Sync: ${this.syncEnabled ? 'ON' : 'OFF'}`;
        btn.classList.toggle('bg-primary', this.syncEnabled);
        btn.classList.toggle('bg-muted', !this.syncEnabled);
    }

    toggleStats() {
        this.statsVisible = !this.statsVisible;
        document.getElementById('statsPanel').classList.toggle('hidden', !this.statsVisible);
    }

    connectWebSocket() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws/stats`;
        
        this.ws = new WebSocket(wsUrl);
        
        this.ws.onopen = () => {
            document.getElementById('wsStatus').classList.remove('bg-error');
            document.getElementById('wsStatus').classList.add('bg-success');
        };

        this.ws.onclose = () => {
            document.getElementById('wsStatus').classList.remove('bg-success');
            document.getElementById('wsStatus').classList.add('bg-error');
            // Reconnect after 3 seconds
            setTimeout(() => this.connectWebSocket(), 3000);
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            this.handleWebSocketMessage(data);
        };
    }

    handleWebSocketMessage(data) {
        switch (data.type) {
            case 'stats':
                this.updateStats(data.stats);
                break;
            case 'request':
                this.addRecentRequest(data.request);
                this.updateLatencyDisplay(data.request);
                break;
            case 'recent':
                data.recent.forEach(req => this.addRecentRequest(req));
                break;
        }
    }

    updateStats(stats) {
        this.stats = stats;
        
        // Local stats
        if (stats.local) {
            document.getElementById('localRequests').textContent = `${stats.local.requests} req`;
            document.getElementById('localAvg').textContent = this.formatDuration(stats.local.avg_time_ns);
            document.getElementById('localMin').textContent = this.formatDuration(stats.local.min_time_ns);
            document.getElementById('localMax').textContent = this.formatDuration(stats.local.max_time_ns);
        }

        // Remote stats
        if (stats.remote) {
            document.getElementById('remoteRequests').textContent = `${stats.remote.requests} req`;
            document.getElementById('remoteAvg').textContent = this.formatDuration(stats.remote.avg_time_ns);
            document.getElementById('remoteMin').textContent = this.formatDuration(stats.remote.min_time_ns);
            document.getElementById('remoteMax').textContent = this.formatDuration(stats.remote.max_time_ns);
        }
    }

    updateLatencyDisplay(request) {
        const type = request.is_remote ? 'remote' : 'local';
        const latencyMs = request.duration_ns / 1000000;
        
        document.getElementById(`${type}Latency`).textContent = `${latencyMs.toFixed(0)}ms`;
        
        // Update bar (max 1000ms for scale)
        const percentage = Math.min(latencyMs / 1000 * 100, 100);
        document.getElementById(`${type}LatencyBar`).style.width = `${percentage}%`;
    }

    addRecentRequest(request) {
        const container = document.getElementById('recentRequests');
        const latencyMs = request.duration_ns / 1000000;
        const isRemote = request.is_remote;
        const statusColor = request.status < 400 ? 'text-success' : 'text-error';
        
        const el = document.createElement('div');
        el.className = 'flex items-center gap-2 py-0.5';
        el.innerHTML = `
            <span class="w-2 h-2 ${isRemote ? 'bg-primary' : 'bg-success'} rounded-full"></span>
            <span class="${statusColor}">${request.status}</span>
            <span class="flex-1 truncate text-muted-foreground">${this.truncatePath(request.path)}</span>
            <span>${latencyMs.toFixed(0)}ms</span>
        `;

        container.insertBefore(el, container.firstChild);

        // Keep only last 20
        while (container.children.length > 20) {
            container.removeChild(container.lastChild);
        }
    }

    truncatePath(path) {
        if (path.length > 40) {
            return '...' + path.slice(-37);
        }
        return path;
    }

    formatDuration(ns) {
        if (!ns || ns === 0) return '0ms';
        const ms = ns / 1000000;
        if (ms < 1) return '<1ms';
        if (ms < 1000) return `${ms.toFixed(0)}ms`;
        return `${(ms / 1000).toFixed(2)}s`;
    }

    makeDraggable(element, handle) {
        let pos1 = 0, pos2 = 0, pos3 = 0, pos4 = 0;
        
        handle.onmousedown = dragMouseDown;

        function dragMouseDown(e) {
            e.preventDefault();
            pos3 = e.clientX;
            pos4 = e.clientY;
            document.onmouseup = closeDragElement;
            document.onmousemove = elementDrag;
        }

        function elementDrag(e) {
            e.preventDefault();
            pos1 = pos3 - e.clientX;
            pos2 = pos4 - e.clientY;
            pos3 = e.clientX;
            pos4 = e.clientY;
            element.style.top = (element.offsetTop - pos2) + 'px';
            element.style.left = (element.offsetLeft - pos1) + 'px';
            element.style.right = 'auto';
            element.style.bottom = 'auto';
        }

        function closeDragElement() {
            document.onmouseup = null;
            document.onmousemove = null;
        }
    }
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', () => {
    window.viewer = new ViewerManager();
});
