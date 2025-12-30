const API_URL = "http://localhost:5051/api/signals";
const tbody = document.getElementById("signals-body");

function signalClass(signal) {
    if (signal === "BUY") return "signal-buy";
    if (signal === "SELL") return "signal-sell";
    return "signal-neutral";
}

function confidenceBar(confidence) {
    const percent = Math.min(Math.max(confidence * 100, 0), 100);
    return `
        <div class="confidence-bar">
            <div class="confidence-fill" style="width:${percent}%"></div>
        </div>
        ${percent.toFixed(1)}%
    `;
}

async function loadSignals() {
    try {
        const res = await fetch(API_URL);
        const data = await res.json();

        if (!data || data.length === 0) {
            tbody.innerHTML = `
                <tr><td colspan="5" class="loading">No signals available</td></tr>
            `;
            return;
        }

        tbody.innerHTML = "";

        data.forEach(item => {
            const row = document.createElement("tr");
            row.innerHTML = `
                <td>${item.symbol}</td>
                <td>₹${item.price}</td>
                <td class="${signalClass(item.signal)}">${item.signal}</td>
                <td>${confidenceBar(item.confidence)}</td>
                <td>${item.reason}</td>
            `;
            tbody.appendChild(row);
        });

    } catch (err) {
        tbody.innerHTML = `
            <tr><td colspan="5" class="loading">API not reachable</td></tr>
        `;
        console.error(err);
    }
}

// initial load
loadSignals();

// refresh every 5 seconds
setInterval(loadSignals, 5000);
