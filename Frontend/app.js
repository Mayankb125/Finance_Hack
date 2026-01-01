// ============================================
// Configuration
// ============================================
const API_URL = "http://localhost:5051/api/signals";
const statusEl = document.getElementById("status");
const tbody = document.getElementById("signals-body");

// ============================================
// State Management
// ============================================
// Keep one row per symbol
const rows = {};

// Maintain a separate cache of signal data
const signalsCache = {};

// ============================================
// Utility Functions
// ============================================
function formatPrice(price) {
    return typeof price === 'number' ? `₹${price.toFixed(2)}` : "-";
}

function formatChange(change) {
    if (typeof change !== 'number') return "-";
    const sign = change >= 0 ? "+" : "";
    const className = change >= 0 ? "change-positive" : "change-negative";
    return `<span class="${className}">${sign}${change.toFixed(2)}</span>`;
}

function formatSentiment(score) {
    if (typeof score !== 'number') return "-";
    const className = score >= 0 ? "change-positive" : "change-negative";
    return `<span class="${className}">${score >= 0 ? '+' : ''}${score.toFixed(3)}</span>`;
}

function confidenceBar(confidence) {
    if (typeof confidence !== 'number') return "-";
    const percent = Math.min(Math.max(confidence * 100, 0), 100);
    return `
        <div class="confidence-bar">
            <div class="confidence-fill" style="width:${percent}%"></div>
        </div>
        <div class="confidence-percent">${percent.toFixed(1)}%</div>
    `;
}

function signalClass(signal) {
    const s = (signal || "NEUTRAL").toUpperCase();
    if (s === "BUY") return "signal-buy";
    if (s === "SELL") return "signal-sell";
    return "signal-neutral";
}

function formatTimestamp(timestamp) {
    if (!timestamp) return "";
    try {
        const date = new Date(timestamp);
        return date.toLocaleTimeString();
    } catch {
        return "";
    }
}

// ============================================
// Data Management
// ============================================
// Load cached data from localStorage on page load
function loadCachedSignals() {
    try {
        const cached = localStorage.getItem('market_signals');
        if (cached) {
            const data = JSON.parse(cached);
            // Loaded cached signals silently
            // Restore cache object
            Object.assign(signalsCache, data);
            // Update UI with cached data
            Object.values(data).forEach(item => {
                updateSignalRow(item);
            });
        }
    } catch (err) {
        console.error("Failed to load cached signals:", err);
    }
}

// Update row with signal data
function updateSignalRow(data) {
    const symbol = data.symbol ?? "N/A";
    const price = data.price ?? 0;
    const change = data.change ?? 0;
    const sentimentScore = data.sentimentScore ?? 0;
    const signal = data.signal ?? "NEUTRAL";
    const confidence = data.confidence ?? 0;
    const reason = data.reason ?? "-";
    const aiExplanation = data.ai_explanation ?? "No explanation available";
    const timestamp = data.timestamp ?? "";

    // Store in cache
    signalsCache[symbol] = data;
    
    // Save to localStorage
    try {
        localStorage.setItem('market_signals', JSON.stringify(signalsCache));
    } catch (err) {
        console.warn("Failed to save to localStorage:", err);
    }

    let row = rows[symbol];

    if (!row) {
        row = document.createElement("tr");
        rows[symbol] = row;
        tbody.prepend(row);
    }

    row.className = signal.toLowerCase();

    row.innerHTML = `
        <td><strong>${symbol}</strong></td>
        <td>${formatPrice(price)}</td>
        <td>${formatChange(change)}</td>
        <td>${formatSentiment(sentimentScore)}</td>
        <td class="${signalClass(signal)}">${signal}</td>
        <td>${confidenceBar(confidence)}</td>
        <td>${reason}</td>
        <td>
            <div class="ai-explanation">${aiExplanation}</div>
            <div class="timestamp">${formatTimestamp(timestamp)}</div>
        </td>
    `;
}

// ============================================
// API Functions
// ============================================
// Initial load from REST API
async function loadInitialSignals() {
    // Wait a bit for WebSocket to potentially connect first
    await new Promise(resolve => setTimeout(resolve, 500));
    
    try {
        const res = await fetch(API_URL);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        
        const data = await res.json();
        
        if (!data || data.length === 0) {
            // Only show message if we have no cached data and WebSocket is not connected
            if (Object.keys(signalsCache).length === 0 && !socket.connected) {
                tbody.innerHTML = `
                    <tr><td colspan="8" class="loading">No signals available yet. Waiting for data...</td></tr>
                `;
            }
            return;
        }

        // Clear loading message only if it exists and table is empty (don't clear if we have cached data)
        const loadingRow = tbody.querySelector('.loading');
        if (loadingRow && Object.keys(rows).length === 0 && Object.keys(signalsCache).length === 0) {
            tbody.innerHTML = "";
        }

        // Sort by timestamp (newest first)
        data.sort((a, b) => {
            const tsA = a.timestamp || "";
            const tsB = b.timestamp || "";
            return tsB.localeCompare(tsA);
        });

        // Update or add all signals (merge with existing data)
        data.forEach(item => {
            updateSignalRow(item);
        });

        // Signals loaded successfully
    } catch (err) {
        console.error("Failed to load initial signals:", err);
        // Wait a bit more and check again if WebSocket connected
        setTimeout(() => {
            if (!socket.connected) {
                tbody.innerHTML = `
                    <tr><td colspan="8" class="loading">Unable to load signals. Check if server is running on port 5051.</td></tr>
                `;
            } else {
                // WebSocket is connected, so initial load failure is not critical
                // REST API failed but WebSocket connected - non-critical
                // Clear error if WebSocket is now connected
                const loadingRow = tbody.querySelector('.loading');
                if (loadingRow && loadingRow.textContent.includes("Unable to load")) {
                    if (Object.keys(rows).length === 0) {
                        tbody.innerHTML = `
                            <tr><td colspan="8" class="loading">Waiting for signals via WebSocket...</td></tr>
                        `;
                    }
                }
            }
        }, 1000);
    }
}

// Manual refresh function
async function refreshData() {
    const btn = document.getElementById('refreshBtn');
    const originalText = btn.innerHTML;
    btn.innerHTML = '⏳ Refreshing...';
    btn.disabled = true;
    
    try {
        await loadInitialSignals();
        statusEl.textContent = "✅ Data refreshed successfully";
        statusEl.style.color = "#22c55e";
        setTimeout(() => {
            if (socket.connected) {
                statusEl.textContent = "✅ Connected to live market stream";
            }
        }, 2000);
    } catch (err) {
        console.error("Refresh failed:", err);
        statusEl.textContent = "⚠️ Refresh failed. Using cached data.";
        statusEl.style.color = "#facc15";
    } finally {
        btn.innerHTML = originalText;
        btn.disabled = false;
    }
}

// ============================================
// WebSocket Connection
// ============================================
// Connect to Flask Socket.IO backend
const socket = io("http://localhost:5051", {
    transports: ["websocket"],
    reconnection: true,
    reconnectionDelay: 1000,
    reconnectionAttempts: 5
});

socket.on("connect", () => {
    console.log("✅ WebSocket connected");
    statusEl.textContent = "✅ Connected to live market stream";
    statusEl.style.color = "#22c55e";
    
    // Clear any error messages when WebSocket connects
    const loadingRow = tbody.querySelector('.loading');
    if (loadingRow && loadingRow.textContent.includes("Unable to load")) {
        // Clear error message, signals will come via WebSocket
        if (Object.keys(rows).length === 0) {
            tbody.innerHTML = `
                <tr><td colspan="8" class="loading">Waiting for signals...</td></tr>
            `;
        }
    }
});

socket.on("disconnect", () => {
    console.warn("❌ WebSocket disconnected");
    statusEl.textContent = "❌ Disconnected from server. Attempting to reconnect...";
    statusEl.style.color = "#ef4444";
});

socket.on("connect_error", (error) => {
    console.error("WebSocket connection error:", error);
    statusEl.textContent = "⚠️ Connection error. Using REST API fallback...";
    statusEl.style.color = "#facc15";
});

// Listen for signal events (must match socketio.emit("signal", data))
socket.on("signal", (data) => {
    // Signal received and processed
    updateSignalRow(data);
});

// ============================================
// Initialization
// ============================================
// Load cached signals first (instant display)
loadCachedSignals();

// Then load fresh data from API
loadInitialSignals();

// Expose refresh function globally for button
window.refreshData = refreshData;
