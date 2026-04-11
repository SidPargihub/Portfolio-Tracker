/* ════════════════════════════════════════════
   Portfolio Tracker — App Logic
   ════════════════════════════════════════════ */

const App = {
    state: {
        view: 'dashboard',
        portfolioId: null,
        portfolios: [],
        holdings: [],
        summary: {},
        sectors: [],
        watchlist: [],
        history: [],
        transactions: [],
        files: {},
        charts: {},
    },

    // ── Init ──
    async init() {
        this.loadTheme();
        await this.loadPortfolios();
        this.navigate('dashboard');
    },

    // ── Theme ──
    loadTheme() {
        const saved = localStorage.getItem('theme');
        const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
        const theme = saved || (prefersDark ? 'dark' : 'light');
        document.documentElement.setAttribute('data-theme', theme);
    },

    toggleTheme() {
        const current = document.documentElement.getAttribute('data-theme');
        const next = current === 'dark' ? 'light' : 'dark';
        document.documentElement.setAttribute('data-theme', next);
        localStorage.setItem('theme', next);
        Object.values(this.state.charts).forEach(c => c && c.destroy && c.destroy());
        this.state.charts = {};
        this.renderCurrentView();
    },

    toggleSidebar() {
        document.getElementById('sidebar').classList.toggle('open');
    },

    // ── API ──
    api: {
        async get(url) {
            const r = await fetch(url);
            return r.json();
        },
        async post(url, data) {
            const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
            return r.json();
        },
        async postForm(url, formData) {
            const r = await fetch(url, { method: 'POST', body: formData });
            return r.json();
        },
        async del(url) {
            const r = await fetch(url, { method: 'DELETE' });
            return r.json();
        },
    },

    // ── Portfolios ──
    async loadPortfolios() {
        this.state.portfolios = await this.api.get('/api/portfolios');
        const sel = document.getElementById('portfolio-select');
        sel.innerHTML = '<option value="">No Portfolio</option>';
        this.state.portfolios.forEach(p => {
            const opt = document.createElement('option');
            opt.value = p.id;
            opt.textContent = p.name;
            if (p.id == this.state.portfolioId) opt.selected = true;
            sel.appendChild(opt);
        });
        if (!this.state.portfolioId && this.state.portfolios.length > 0) {
            this.state.portfolioId = this.state.portfolios[0].id;
            sel.value = this.state.portfolioId;
        }
    },

    switchPortfolio(id) {
        this.state.portfolioId = id ? parseInt(id) : null;
        this.renderCurrentView();
    },

    async renamePortfolio() {
        if (!this.state.portfolioId) return;
        const p = this.state.portfolios.find(x => x.id === this.state.portfolioId);
        if (!p) return;
        const newName = prompt("Rename portfolio:", p.name);
        if (newName && newName.trim() !== "" && newName !== p.name) {
            try {
                await fetch(`/api/portfolios/${this.state.portfolioId}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name: newName.trim() })
                });
                await this.loadPortfolios();
            } catch (e) {
                alert("Error renaming portfolio.");
            }
        }
    },

    async deletePortfolio() {
        if (!this.state.portfolioId) return;
        const p = this.state.portfolios.find(x => x.id === this.state.portfolioId);
        if (!p) return;
        if (confirm(`Are you sure you want to permanently delete "${p.name}"? This cannot be undone.`)) {
            try {
                await this.api.del(`/api/portfolios/${this.state.portfolioId}`);
                this.state.portfolioId = null;
                await this.loadPortfolios();
                this.navigate('dashboard');
            } catch (e) {
                alert("Error deleting portfolio.");
            }
        }
    },

    // ── Navigation ──
    navigate(view) {
        this.state.view = view;
        document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
        const navItem = document.querySelector(`.nav-item[data-view="${view}"]`);
        if (navItem) navItem.classList.add('active');
        const titles = { dashboard: 'Dashboard', holdings: 'Holdings', sectors: 'Sectors', watchlist: 'Watchlist', history: 'History', performance: 'Performance', transactions: 'Transactions' };
        document.getElementById('page-title').textContent = titles[view] || 'Dashboard';
        document.getElementById('sidebar').classList.remove('open');
        this.renderCurrentView();
    },

    renderCurrentView() {
        const content = document.getElementById('app-content');
        content.innerHTML = '<div style="text-align:center;padding:60px;"><div class="skeleton" style="width:200px;height:20px;margin:0 auto 12px"></div><div class="skeleton" style="width:300px;height:16px;margin:0 auto"></div></div>';
        const actions = document.getElementById('header-actions');
        actions.innerHTML = '';
        const view = this.state.view;
        if (view === 'dashboard') this.renderDashboard(content, actions);
        else if (view === 'holdings') this.renderHoldings(content, actions);
        else if (view === 'sectors') this.renderSectors(content, actions);
        else if (view === 'watchlist') this.renderWatchlist(content, actions);
        else if (view === 'history') this.renderHistory(content, actions);
        else if (view === 'performance') this.renderPerformance(content, actions);
        else if (view === 'transactions') this.renderTransactions(content, actions);
    },

    // ── Dashboard View ──
    async renderDashboard(el, actions) {
        if (!this.state.portfolioId) {
            el.innerHTML = this.emptyState('Welcome to Portfolio Tracker', 'Import your broker\'s CSV to get started tracking your investments with live prices, sector analysis, and more.', 'Import Portfolio', 'App.showImportModal()');
            return;
        }
        actions.innerHTML = '<button class="btn btn-sm btn-secondary" onclick="App.showImportModal()">Import CSV</button>';
        try {
            const [holdingsData, sectors, history] = await Promise.all([
                this.api.get(`/api/portfolios/${this.state.portfolioId}/holdings`),
                this.api.get(`/api/portfolios/${this.state.portfolioId}/sectors`),
                this.api.get(`/api/portfolios/${this.state.portfolioId}/history`),
            ]);
            this.state.holdings = holdingsData.holdings || [];
            this.state.summary = holdingsData.summary || {};
            this.state.sectors = sectors || [];
            this.state.history = history || [];
            const s = this.state.summary;

            // Fetch XIRR in background
            const xirrPromise = this.api.get(`/api/portfolios/${this.state.portfolioId}/xirr`).catch(() => null);

            let html = `<div class="metrics-grid" id="dashboard-metrics">
                ${this.metricCard('Total Value', this.fmt(s.total_value), '', '')}
                ${this.metricCard('Invested', this.fmt(s.total_invested), '', '')}
                ${this.metricCard('Total P&L', this.fmt(s.total_pnl), this.fmtPct(s.total_return_pct), s.total_pnl >= 0 ? 'positive' : 'negative')}
                ${this.metricCard('Holdings', s.holdings_count || 0, 'Stocks', '')}
                <div class="metric-card" id="xirr-metric-card"><div class="metric-label">XIRR (Annualized)</div><div class="metric-value" style="color:var(--text-tertiary)">Loading...</div></div>
            </div>`;

            html += '<div class="grid-2">';

            // Portfolio value chart
            html += `<div class="card"><div class="card-header"><h3>Portfolio Value</h3></div><div class="chart-container"><canvas id="chart-value"></canvas></div></div>`;

            // Sector donut
            html += `<div class="card"><div class="card-header"><h3>Sector Allocation</h3></div><div class="chart-container"><canvas id="chart-sectors"></canvas></div></div>`;
            html += '</div>';

            // Top Movers
            const sorted = [...this.state.holdings].filter(h => h.pnl !== undefined);
            const gainers = sorted.sort((a, b) => b.return_pct - a.return_pct).slice(0, 5);
            const losers = sorted.sort((a, b) => a.return_pct - b.return_pct).slice(0, 5);

            html += '<div class="grid-2">';
            html += `<div class="card"><div class="card-header"><h3>Top Gainers</h3></div>${this.renderMovers(gainers)}</div>`;
            html += `<div class="card"><div class="card-header"><h3>Top Losers</h3></div>${this.renderMovers(losers)}</div>`;
            html += '</div>';

            // Gain/Loss Heatmap
            html += `<div class="card" style="margin-bottom:28px">
                <div class="card-header">
                    <h3>Gain/Loss Heatmap</h3>
                    <span class="badge">Size = Weight, Color = Return</span>
                </div>
                ${this.renderGainLossHeatmap(this.state.holdings)}
            </div>`;

            // Health score
            const healthScore = this.calcHealth();
            html += `<div class="card" style="margin-bottom:28px"><div class="card-header"><h3>Portfolio Health</h3></div>
                <div style="display:flex;align-items:center;gap:32px">
                    <div class="health-score ${healthScore >= 70 ? 'positive' : healthScore >= 40 ? '' : 'negative'}">${healthScore}</div>
                    <div style="flex:1">
                        <div class="health-bar"><div class="health-fill" style="width:${healthScore}%;background:${healthScore >= 70 ? 'var(--green)' : healthScore >= 40 ? 'var(--orange)' : 'var(--red)'}"></div></div>
                        <div style="font-size:0.82rem;color:var(--text-secondary);margin-top:8px">${this.healthText(healthScore)}</div>
                    </div>
                </div></div>`;

            el.innerHTML = html;
            this.renderValueChart();
            this.renderSectorDonut();

            // Fill in XIRR once fetched
            xirrPromise.then(data => {
                const card = document.getElementById('xirr-metric-card');
                if (!card) return;
                if (data && data.xirr !== null && data.xirr !== undefined) {
                    const cls = data.xirr >= 0 ? 'positive' : 'negative';
                    const sign = data.xirr >= 0 ? '+' : '';
                    card.innerHTML = `<div class="metric-label">XIRR (Annualized)</div><div class="metric-value ${cls}">${sign}${data.xirr}%</div><div class="metric-sub" style="color:var(--text-tertiary)">${data.method === 'transactions' ? 'From transactions' : 'From cost basis'}</div>`;
                } else {
                    card.innerHTML = `<div class="metric-label">XIRR (Annualized)</div><div class="metric-value" style="color:var(--text-tertiary)">—</div><div class="metric-sub" style="color:var(--text-tertiary)">Add transactions for XIRR</div>`;
                }
            });
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error loading data</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Holdings View ──
    async renderHoldings(el, actions) {
        if (!this.state.portfolioId) { el.innerHTML = this.emptyState('No Portfolio Selected', 'Import a CSV or select a portfolio to view holdings.', 'Import Portfolio', 'App.showImportModal()'); return; }
        actions.innerHTML = '<button class="btn btn-sm btn-secondary" onclick="App.exportCSV()">Export CSV</button>';
        try {
            const data = await this.api.get(`/api/portfolios/${this.state.portfolioId}/holdings`);
            this.state.holdings = data.holdings || [];
            this.state.summary = data.summary || {};

            if (this.state.holdings.length === 0) { el.innerHTML = this.emptyState('No Holdings', 'Import your broker CSV to add holdings.', 'Import CSV', 'App.showImportModal()'); return; }

            let html = `<div class="card"><div class="table-wrapper"><table>
                <thead><tr>
                    <th>Stock</th><th>Symbol</th><th class="text-right">Qty</th><th class="text-right">Avg Price</th>
                    <th class="text-right">LTP</th><th class="text-right">Invested</th><th class="text-right">Current</th>
                    <th class="text-right">P&L</th><th class="text-right">Return</th>
                </tr></thead><tbody>`;

            this.state.holdings.forEach(h => {
                const cls = h.pnl >= 0 ? 'positive' : 'negative';
                html += `<tr>
                    <td class="stock-name">${this.esc(h.name || h.symbol)}</td>
                    <td style="color:var(--text-tertiary);font-size:0.82rem">${this.esc(h.symbol)}</td>
                    <td class="text-right mono">${h.quantity}</td>
                    <td class="text-right mono">${this.fmt(h.avg_price)}</td>
                    <td class="text-right mono">${this.fmt(h.ltp)}</td>
                    <td class="text-right mono">${this.fmt(h.invested_value)}</td>
                    <td class="text-right mono">${this.fmt(h.current_value)}</td>
                    <td class="text-right mono ${cls}">${this.fmt(h.pnl)}</td>
                    <td class="text-right mono ${cls}">${this.fmtPct(h.return_pct)}</td>
                </tr>`;
            });

            html += '</tbody></table></div></div>';
            el.innerHTML = html;
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Sectors View ──
    async renderSectors(el) {
        if (!this.state.portfolioId) { el.innerHTML = this.emptyState('No Portfolio Selected', 'Select a portfolio to view sector allocation.'); return; }
        try {
            const sectors = await this.api.get(`/api/portfolios/${this.state.portfolioId}/sectors`);
            this.state.sectors = sectors;

            let html = `<div class="grid-2">
                <div class="card"><div class="card-header"><h3>Sector Allocation</h3></div><div class="chart-container"><canvas id="chart-sectors-full"></canvas></div></div>
                <div class="card"><div class="card-header"><h3>Allocation Drift</h3><span class="badge">Market vs Cost</span></div><div class="table-wrapper"><table>
                    <thead><tr><th>Sector</th><th class="text-right">Value</th><th class="text-right">Current %</th><th class="text-right">Invested %</th></tr></thead><tbody>`;

            sectors.forEach(s => {
                const diff = s.weight - s.invested_weight;
                const sign = diff > 0 ? '+' : '';
                const cls = diff > 0 ? 'positive' : (diff < 0 ? 'negative' : 'text-tertiary');
                html += `<tr>
                    <td class="stock-name">${this.esc(s.name)}</td>
                    <td class="text-right mono">${this.fmt(s.value)}</td>
                    <td class="text-right mono">${s.weight.toFixed(1)}%</td>
                    <td class="text-right mono">
                        ${s.invested_weight.toFixed(1)}%
                        <div style="font-size:0.7rem;margin-top:2px" class="${cls}">${sign}${diff.toFixed(1)}% drift</div>
                    </td>
                </tr>`;
            });

            html += '</tbody></table></div></div></div>';
            el.innerHTML = html;

            this.renderSectorChart('chart-sectors-full', sectors);
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Watchlist View ──
    async renderWatchlist(el, actions) {
        actions.innerHTML = '<button class="btn btn-sm btn-primary" onclick="App.showWatchlistModal()">+ Add Stock</button>';
        try {
            const items = await this.api.get('/api/watchlist');
            this.state.watchlist = items;

            if (items.length === 0) { el.innerHTML = this.emptyState('Watchlist Empty', 'Add stocks to your watchlist to track them.', 'Add Stock', 'App.showWatchlistModal()'); return; }

            let html = '<div class="watchlist-grid">';
            items.forEach(item => {
                const cls = item.change >= 0 ? 'positive' : 'negative';
                const sign = item.change >= 0 ? '+' : '';
                html += `<div class="watchlist-card">
                    <div class="wl-header"><div><div class="wl-symbol">${this.esc(item.symbol)}</div><div class="wl-name">${this.esc(item.name || '')}</div></div>
                    <button class="btn-icon" onclick="App.removeWatchlist('${this.esc(item.symbol)}')" title="Remove"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button></div>
                    <div class="wl-price">${this.fmt(item.price)}</div>
                    <div class="wl-change ${cls}">${sign}${this.fmt(item.change)} (${sign}${item.change_pct.toFixed(2)}%)</div>
                    ${item.target_price ? `<div class="wl-target">Target: ${this.fmt(item.target_price)}</div>` : ''}
                </div>`;
            });
            html += '</div>';
            el.innerHTML = html;
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error</h2><p>${e.message}</p></div>`;
        }
    },

    // ── History View ──
    async renderHistory(el) {
        if (!this.state.portfolioId) { el.innerHTML = this.emptyState('No Portfolio Selected', 'Select a portfolio to view history.'); return; }
        try {
            const history = await this.api.get(`/api/portfolios/${this.state.portfolioId}/history`);
            this.state.history = history;
            if (history.length === 0) { el.innerHTML = this.emptyState('No History Yet', 'Portfolio snapshots are saved automatically each time you view the dashboard.'); return; }
            let html = `<div class="card" style="margin-bottom:28px"><div class="card-header"><h3>Portfolio Value Over Time</h3></div><div class="chart-container" style="height:350px"><canvas id="chart-history"></canvas></div></div>`;
            html += `<div class="card"><div class="card-header"><h3>Snapshots</h3></div><div class="table-wrapper"><table>
                <thead><tr><th>Date</th><th class="text-right">Value</th><th class="text-right">Invested</th><th class="text-right">P&L</th></tr></thead><tbody>`;
            [...history].reverse().forEach(s => {
                const cls = s.total_pnl >= 0 ? 'positive' : 'negative';
                html += `<tr><td>${s.date}</td><td class="text-right mono">${this.fmt(s.total_value)}</td><td class="text-right mono">${this.fmt(s.total_invested)}</td><td class="text-right mono ${cls}">${this.fmt(s.total_pnl)}</td></tr>`;
            });
            html += '</tbody></table></div></div>';
            el.innerHTML = html;
            this.renderHistoryChart();
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Transactions View ──
    async renderTransactions(el, actions) {
        if (!this.state.portfolioId) { el.innerHTML = this.emptyState('No Portfolio Selected', 'Select a portfolio to view transactions.'); return; }
        actions.innerHTML = '<button class="btn btn-sm btn-primary" onclick="App.showTxnModal()">+ Add Transaction</button>';
        try {
            const txns = await this.api.get(`/api/portfolios/${this.state.portfolioId}/transactions`);
            this.state.transactions = txns;
            if (txns.length === 0) { el.innerHTML = this.emptyState('No Transactions', 'Record your buy/sell transactions to track history.', 'Add Transaction', 'App.showTxnModal()'); return; }
            let html = `<div class="card"><div class="table-wrapper"><table>
                <thead><tr><th>Date</th><th>Type</th><th>Symbol</th><th>Name</th><th class="text-right">Qty</th><th class="text-right">Price</th><th class="text-right">Value</th><th>Notes</th></tr></thead><tbody>`;
            txns.forEach(t => {
                const cls = t.type === 'BUY' ? 'positive' : 'negative';
                html += `<tr><td>${t.date || ''}</td><td><span class="${cls}" style="font-weight:600">${t.type}</span></td>
                    <td style="color:var(--text-tertiary);font-size:0.82rem">${this.esc(t.symbol)}</td><td class="stock-name">${this.esc(t.name || '')}</td>
                    <td class="text-right mono">${t.quantity}</td><td class="text-right mono">${this.fmt(t.price)}</td>
                    <td class="text-right mono">${this.fmt(t.quantity * t.price)}</td><td style="color:var(--text-secondary)">${this.esc(t.notes || '')}</td></tr>`;
            });
            html += '</tbody></table></div></div>';
            el.innerHTML = html;
        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Performance View ──
    async renderPerformance(el, actions) {
        if (!this.state.portfolioId) {
            el.innerHTML = this.emptyState('No Portfolio Selected', 'Select a portfolio to view performance analysis.');
            return;
        }

        try {
            const [xirrData, benchmarkData, holdingsData] = await Promise.all([
                this.api.get(`/api/portfolios/${this.state.portfolioId}/xirr`).catch(() => null),
                this.api.get(`/api/portfolios/${this.state.portfolioId}/benchmark`).catch(() => null),
                this.api.get(`/api/portfolios/${this.state.portfolioId}/holdings`).catch(() => ({ holdings: [], summary: {} })),
            ]);

            let html = '';

            // ── XIRR + Alpha + Method insight cards ──
            const xirrVal = xirrData && xirrData.xirr !== null ? xirrData.xirr : null;
            const xirrCls = xirrVal !== null ? (xirrVal >= 0 ? 'positive' : 'negative') : '';
            const xirrSign = xirrVal !== null && xirrVal >= 0 ? '+' : '';

            html += '<div class="insight-row">';

            // XIRR Card
            html += `<div class="insight-card">
                <div class="insight-icon ${xirrVal !== null && xirrVal >= 0 ? 'green' : 'red'}">📈</div>
                <div class="xirr-value ${xirrCls}">${xirrVal !== null ? xirrSign + xirrVal + '%' : '—'}</div>
                <div class="xirr-label">XIRR (Annualized Return)</div>
                <div class="xirr-note">${xirrData ? (xirrData.method === 'transactions' ? 'Calculated from transactions' : 'Estimated from cost basis') : 'Unavailable'}</div>
            </div>`;

            // Alpha Card
            let alphaVal = null;
            if (benchmarkData && benchmarkData.portfolio_returns && benchmarkData.benchmark_returns) {
                const pRet1Y = benchmarkData.portfolio_returns['1Y'];
                const bRet1Y = benchmarkData.benchmark_returns['1Y'];
                if (pRet1Y !== null && bRet1Y !== null) {
                    alphaVal = (pRet1Y - bRet1Y);
                }
            }
            const alphaCls = alphaVal !== null ? (alphaVal >= 0 ? 'outperform' : 'underperform') : '';
            const alphaIcon = alphaVal !== null && alphaVal >= 0 ? '🏆' : '📉';

            html += `<div class="insight-card">
                <div class="insight-icon ${alphaVal !== null && alphaVal >= 0 ? 'green' : 'red'}">${alphaIcon}</div>
                ${alphaVal !== null ?
                    `<div class="alpha-badge ${alphaCls}" style="font-size:1.6rem;padding:8px 0;border-radius:0;background:none">
                        ${alphaVal >= 0 ? '+' : ''}${alphaVal.toFixed(2)}%
                    </div>` :
                    `<div style="font-size:1.6rem;font-weight:800;color:var(--text-tertiary)">—</div>`
                }
                <div class="xirr-label">Alpha vs ${benchmarkData?.benchmark_name || 'Nifty 500'}</div>
                <div class="xirr-note">${alphaVal !== null ? (alphaVal >= 0 ? 'Outperforming the benchmark' : 'Underperforming the benchmark') : 'Need more snapshots for comparison'}</div>
            </div>`;

            // Benchmark Name Card
            html += `<div class="insight-card">
                <div class="insight-icon blue">🏛️</div>
                <div style="font-size:1.4rem;font-weight:700;margin-bottom:4px">${benchmarkData?.benchmark_name || 'Nifty 500'}</div>
                <div class="xirr-label">Benchmark Index</div>
                <div class="xirr-note">${benchmarkData?.full_benchmark?.length || 0} data points loaded</div>
            </div>`;

            html += '</div>';

            // ── Benchmark Comparison Chart ──
            html += `<div class="card" style="margin-bottom:28px">
                <div class="card-header">
                    <h3>Portfolio vs ${benchmarkData?.benchmark_name || 'Nifty 500'}</h3>
                    <span class="badge">Normalized to 100</span>
                </div>
                <div class="chart-container" style="height:350px"><canvas id="chart-benchmark"></canvas></div>
                <div class="chart-legend-custom">
                    <div class="legend-item"><div class="legend-dot" style="background:var(--accent)"></div>Your Portfolio</div>
                    <div class="legend-item"><div class="legend-dot dashed" style="color:var(--orange)"></div>${benchmarkData?.benchmark_name || 'Nifty 500'}</div>
                </div>
            </div>`;

            // ── Rolling Returns Comparison ──
            const periods = ['1M', '3M', '6M', '1Y'];
            const pReturns = benchmarkData?.portfolio_returns || {};
            const bReturns = benchmarkData?.benchmark_returns || {};

            html += `<div class="card" style="margin-bottom:28px">
                <div class="card-header"><h3>Rolling Returns Comparison</h3></div>
                <div class="returns-grid">
                    <div class="returns-cell header"></div>`;

            periods.forEach(p => {
                html += `<div class="returns-cell header">${p}</div>`;
            });

            // Portfolio row
            html += `<div class="returns-cell row-label">📊 Your Portfolio</div>`;
            periods.forEach(p => {
                const v = pReturns[p];
                if (v !== null && v !== undefined) {
                    const cls = v >= 0 ? 'positive' : 'negative';
                    html += `<div class="returns-cell ${cls}" style="font-weight:600">${v >= 0 ? '+' : ''}${v.toFixed(2)}%</div>`;
                } else {
                    html += `<div class="returns-cell" style="color:var(--text-tertiary)">—</div>`;
                }
            });

            // Benchmark row
            html += `<div class="returns-cell row-label">🏛️ ${benchmarkData?.benchmark_name || 'Nifty 500'}</div>`;
            periods.forEach(p => {
                const v = bReturns[p];
                if (v !== null && v !== undefined) {
                    const cls = v >= 0 ? 'positive' : 'negative';
                    html += `<div class="returns-cell ${cls}" style="font-weight:600">${v >= 0 ? '+' : ''}${v.toFixed(2)}%</div>`;
                } else {
                    html += `<div class="returns-cell" style="color:var(--text-tertiary)">—</div>`;
                }
            });

            // Alpha row
            html += `<div class="returns-cell row-label alpha-row">⚡ Alpha</div>`;
            periods.forEach(p => {
                const pv = pReturns[p];
                const bv = bReturns[p];
                if (pv !== null && pv !== undefined && bv !== null && bv !== undefined) {
                    const alpha = pv - bv;
                    const cls = alpha >= 0 ? 'positive' : 'negative';
                    html += `<div class="returns-cell alpha-row ${cls}">${alpha >= 0 ? '+' : ''}${alpha.toFixed(2)}%</div>`;
                } else {
                    html += `<div class="returns-cell" style="color:var(--text-tertiary)">—</div>`;
                }
            });

            html += '</div></div>';

            // ── Contribution to Returns ──
            const holdings = holdingsData.holdings || [];
            if (holdings.length > 0) {
                const totalInvested = Object.values(holdings).reduce((sum, h) => sum + (h.invested_value || 0), 0);
                
                let contributions = holdings.map(h => {
                    const pnl = h.pnl || 0;
                    const contribPct = totalInvested > 0 ? (pnl / totalInvested) * 100 : 0;
                    return { ...h, contribPct };
                }).filter(h => h.contribPct !== 0);

                contributions.sort((a, b) => b.contribPct - a.contribPct);
                
                const topContributors = contributions.filter(c => c.contribPct > 0).slice(0, 5);
                const bottomDetractors = contributions.filter(c => c.contribPct < 0).slice(-5).reverse();

                // Max absolute contribution for bar scaling
                const maxContrib = Math.max(0.1, ...contributions.map(c => Math.abs(c.contribPct)));

                html += `<div class="grid-2">`;
                
                // Top Contributors
                html += `<div class="card"><div class="card-header"><h3>Top Contributors to Return</h3></div>
                    <div class="contribution-list">`;
                if (topContributors.length === 0) html += `<div style="color:var(--text-tertiary);font-size:0.85rem">No positive contributors.</div>`;
                topContributors.forEach(c => {
                    const widthLimit = Math.max(2, (c.contribPct / maxContrib) * 100);
                    html += `<div class="contrib-item">
                        <div class="contrib-sym" title="${this.esc(c.name)}">${this.esc(c.symbol)}</div>
                        <div class="contrib-bar-wrap">
                            <div class="contrib-zero"></div>
                            <div class="contrib-bar positive" style="width: ${widthLimit}%; margin-left: 50%"></div>
                        </div>
                        <div class="contrib-val positive">+${c.contribPct.toFixed(2)}%</div>
                    </div>`;
                });
                html += `</div></div>`;

                // Top Detractors
                html += `<div class="card"><div class="card-header"><h3>Top Detractors from Return</h3></div>
                    <div class="contribution-list">`;
                if (bottomDetractors.length === 0) html += `<div style="color:var(--text-tertiary);font-size:0.85rem">No negative detractors.</div>`;
                bottomDetractors.forEach(c => {
                    const absPct = Math.abs(c.contribPct);
                    const widthLimit = Math.max(2, (absPct / maxContrib) * 100);
                    html += `<div class="contrib-item">
                        <div class="contrib-sym" title="${this.esc(c.name)}">${this.esc(c.symbol)}</div>
                        <div class="contrib-bar-wrap">
                            <div class="contrib-zero"></div>
                            <div class="contrib-bar negative" style="width: ${widthLimit}%; position: absolute; right: 50%"></div>
                        </div>
                        <div class="contrib-val negative">${c.contribPct.toFixed(2)}%</div>
                    </div>`;
                });
                html += `</div></div></div>`;
            }


            // ── XIRR Explanation ──
            html += `<div class="card">
                <div class="card-header"><h3>What is XIRR?</h3></div>
                <p style="color:var(--text-secondary);font-size:0.92rem;line-height:1.6">
                    <strong>XIRR (Extended Internal Rate of Return)</strong> is the annualized return of your portfolio that accounts for the
                    <em>timing</em> and <em>size</em> of each investment. Unlike simple returns, XIRR gives you the true picture of how your
                    money has grown on an annualized basis, making it the gold standard metric used by Indian investors and mutual fund
                    evaluators. A positive alpha means you're beating the benchmark — that's the goal!
                </p>
                <p style="color:var(--text-tertiary);font-size:0.82rem;margin-top:12px">
                    💡 <strong>Tip:</strong> Add your buy/sell transactions in the Transactions tab for more accurate XIRR. Currently using ${xirrData?.method === 'transactions' ? 'your actual transactions' : 'estimated cost basis dates'}.
                </p>
            </div>`;

            el.innerHTML = html;
            this.renderBenchmarkChart(benchmarkData);

        } catch (e) {
            el.innerHTML = `<div class="empty-state"><h2>Error loading performance data</h2><p>${e.message}</p></div>`;
        }
    },

    // ── Charts ──
    getChartColors() {
        const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
        return {
            accent: isDark ? '#0A84FF' : '#007AFF',
            green: isDark ? '#30D158' : '#34C759',
            red: isDark ? '#FF453A' : '#FF3B30',
            grid: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)',
            text: isDark ? '#98989D' : '#86868B',
            palette: ['#007AFF', '#34C759', '#FF9500', '#AF52DE', '#FF3B30', '#5AC8FA', '#FFCC00', '#FF2D55', '#64D2FF', '#BF5AF2'],
        };
    },

    renderValueChart() {
        const ctx = document.getElementById('chart-value');
        if (!ctx || this.state.history.length === 0) return;
        const c = this.getChartColors();
        const labels = this.state.history.map(h => h.date);
        const values = this.state.history.map(h => h.total_value);
        if (this.state.charts.value) this.state.charts.value.destroy();
        this.state.charts.value = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    data: values, borderColor: c.accent, borderWidth: 2.5, pointRadius: 0, pointHoverRadius: 5,
                    fill: true, backgroundColor: (ctx2) => {
                        const gradient = ctx2.chart.ctx.createLinearGradient(0, 0, 0, ctx2.chart.height);
                        gradient.addColorStop(0, c.accent + '30');
                        gradient.addColorStop(1, c.accent + '00');
                        return gradient;
                    }, tension: 0.4,
                }],
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { display: false }, tooltip: { callbacks: { label: (t) => '₹' + t.parsed.y.toLocaleString('en-IN') } } },
                scales: { x: { grid: { display: false }, ticks: { color: c.text, font: { size: 11 } } }, y: { grid: { color: c.grid }, ticks: { color: c.text, font: { size: 11 }, callback: v => '₹' + (v / 100000).toFixed(1) + 'L' } } },
                interaction: { intersect: false, mode: 'index' },
            },
        });
    },

    renderSectorDonut() {
        this.renderSectorChart('chart-sectors', this.state.sectors);
    },

    renderSectorChart(canvasId, sectors) {
        const ctx = document.getElementById(canvasId);
        if (!ctx || !sectors || sectors.length === 0) return;
        const c = this.getChartColors();
        if (this.state.charts[canvasId]) this.state.charts[canvasId].destroy();
        this.state.charts[canvasId] = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: sectors.map(s => s.name),
                datasets: [{ data: sectors.map(s => s.value), backgroundColor: c.palette.slice(0, sectors.length), borderWidth: 0, hoverOffset: 8 }],
            },
            options: {
                responsive: true, maintainAspectRatio: false, cutout: '65%',
                plugins: {
                    legend: { position: 'right', labels: { color: c.text, font: { size: 11, family: "'Inter', sans-serif" }, padding: 12, usePointStyle: true, pointStyleWidth: 8 } },
                    tooltip: { callbacks: { label: (t) => `${t.label}: ₹${t.parsed.toLocaleString('en-IN')} (${((t.parsed / t.dataset.data.reduce((a, b) => a + b, 0)) * 100).toFixed(1)}%)` } },
                },
            },
        });
    },

    renderHistoryChart() {
        const ctx = document.getElementById('chart-history');
        if (!ctx || this.state.history.length === 0) return;
        const c = this.getChartColors();
        if (this.state.charts.history) this.state.charts.history.destroy();
        this.state.charts.history = new Chart(ctx, {
            type: 'line',
            data: {
                labels: this.state.history.map(h => h.date),
                datasets: [
                    { label: 'Value', data: this.state.history.map(h => h.total_value), borderColor: c.accent, borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, fill: true, backgroundColor: c.accent + '15', tension: 0.4 },
                    { label: 'Invested', data: this.state.history.map(h => h.total_invested), borderColor: c.text, borderWidth: 1.5, borderDash: [6, 4], pointRadius: 0, fill: false, tension: 0.4 },
                ],
            },
            options: {
                responsive: true, maintainAspectRatio: false,
                plugins: { legend: { labels: { color: c.text, font: { size: 11 }, usePointStyle: true } }, tooltip: { callbacks: { label: t => t.dataset.label + ': ₹' + t.parsed.y.toLocaleString('en-IN') } } },
                scales: { x: { grid: { display: false }, ticks: { color: c.text, font: { size: 11 } } }, y: { grid: { color: c.grid }, ticks: { color: c.text, font: { size: 11 }, callback: v => '₹' + (v / 100000).toFixed(1) + 'L' } } },
                interaction: { intersect: false, mode: 'index' },
            },
        });
    },

    renderBenchmarkChart(benchmarkData) {
        const ctx = document.getElementById('chart-benchmark');
        if (!ctx) return;
        const c = this.getChartColors();

        if (this.state.charts.benchmark) this.state.charts.benchmark.destroy();

        // Prepare datasets
        const datasets = [];

        // Full benchmark line (primary — always visible)
        if (benchmarkData && benchmarkData.full_benchmark && benchmarkData.full_benchmark.length > 0) {
            datasets.push({
                label: benchmarkData.benchmark_name || 'Nifty 500',
                data: benchmarkData.full_benchmark.map(d => ({ x: d.date, y: d.value })),
                borderColor: c.palette[2], // orange
                borderWidth: 2,
                borderDash: [6, 4],
                pointRadius: 0,
                pointHoverRadius: 4,
                fill: false,
                tension: 0.3,
            });
        }

        // Portfolio line (overlay on matching dates)
        if (benchmarkData && benchmarkData.portfolio && benchmarkData.portfolio.length > 0) {
            datasets.push({
                label: 'Your Portfolio',
                data: benchmarkData.portfolio.map(d => ({ x: d.date, y: d.value })),
                borderColor: c.accent,
                borderWidth: 2.5,
                pointRadius: 4,
                pointHoverRadius: 6,
                pointBackgroundColor: c.accent,
                fill: true,
                backgroundColor: (ctx2) => {
                    const gradient = ctx2.chart.ctx.createLinearGradient(0, 0, 0, ctx2.chart.height);
                    gradient.addColorStop(0, c.accent + '25');
                    gradient.addColorStop(1, c.accent + '00');
                    return gradient;
                },
                tension: 0.3,
            });
        }

        if (datasets.length === 0) {
            ctx.parentElement.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:350px;color:var(--text-tertiary)">No benchmark data available yet. Visit the Dashboard daily to build portfolio snapshots.</div>';
            return;
        }

        // Collect all dates for x-axis labels
        const allDates = new Set();
        datasets.forEach(ds => ds.data.forEach(dp => allDates.add(dp.x)));
        const labels = [...allDates].sort();

        this.state.charts.benchmark = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (t) => `${t.dataset.label}: ${t.parsed.y.toFixed(2)}`,
                        },
                    },
                },
                scales: {
                    x: {
                        type: 'category',
                        grid: { display: false },
                        ticks: { color: c.text, font: { size: 11 }, maxTicksLimit: 12 },
                    },
                    y: {
                        grid: { color: c.grid },
                        ticks: { color: c.text, font: { size: 11 } },
                    },
                },
                interaction: { intersect: false, mode: 'index' },
            },
        });
    },

    // ── Helpers ──
    metricCard(label, value, sub, cls) {
        return `<div class="metric-card"><div class="metric-label">${label}</div><div class="metric-value ${cls}">${value}</div>${sub ? `<div class="metric-sub ${cls}">${sub}</div>` : ''}</div>`;
    },

    renderMovers(items) {
        if (!items || items.length === 0) return '<div style="padding:16px;color:var(--text-tertiary);font-size:0.88rem">No data</div>';
        let html = '';
        items.forEach(h => {
            const cls = h.pnl >= 0 ? 'positive' : 'negative';
            const sign = h.pnl >= 0 ? '+' : '';
            html += `<div class="mover-item"><div><div class="mover-name">${this.esc(h.name || h.symbol)}</div><div class="mover-symbol">${this.esc(h.symbol)}</div></div><div class="mover-value"><div class="mover-pnl ${cls}">${sign}${this.fmt(h.pnl)}</div><div class="mover-pct ${cls}">${sign}${(h.return_pct || 0).toFixed(2)}%</div></div></div>`;
        });
        return html;
    },

    renderGainLossHeatmap(holdings) {
        if (!holdings || holdings.length === 0) return this.emptyState('No Data', 'Add holdings to view the heatmap.');

        // Filter and sort holdings by weight (current_value)
        const totalValue = holdings.reduce((sum, h) => sum + (h.current_value || 0), 0);
        if (totalValue <= 0) return '<div style="padding:16px;color:var(--text-tertiary)">Calculated portfolio value is zero.</div>';

        const mapData = holdings.map(h => ({
            symbol: h.symbol || h.name,
            weight: ((h.current_value || 0) / totalValue) * 100,
            returnPct: h.return_pct || 0
        })).filter(h => h.weight > 0).sort((a, b) => b.weight - a.weight);

        // Limit to top 50 to avoid clutter
        const topHoldings = mapData.slice(0, 50);
        
        let html = '<div class="heatmap-container">';

        topHoldings.forEach(d => {
            // Determine heat color mapping based on exact ranges
            let heatClass = 'heat-flat';
            if (d.returnPct >= 20) heatClass = 'heat-gain-4';
            else if (d.returnPct >= 10) heatClass = 'heat-gain-3';
            else if (d.returnPct >= 5) heatClass = 'heat-gain-2';
            else if (d.returnPct > 0) heatClass = 'heat-gain-1';
            else if (d.returnPct <= -20) heatClass = 'heat-loss-4';
            else if (d.returnPct <= -10) heatClass = 'heat-loss-3';
            else if (d.returnPct <= -5) heatClass = 'heat-loss-2';
            else if (d.returnPct < 0) heatClass = 'heat-loss-1';

            // Tile scaling logic
            // Since it's flex, we map the weight (0-100) directly to flex basis. 
            // In a wrapping flex container, large basis items take prominent space. 
            // We use flex-grow derived from weight to simulate a dynamic grid map.
            const minW = Math.max(8, d.weight); // prevent microscopic tiles
            const sign = d.returnPct >= 0 ? '+' : '';
            
            html += `<div class="heatmap-tile ${heatClass}" 
                style="flex: ${d.weight} 1 ${minW}%; min-height: ${Math.max(40, d.weight * 3)}px;"
                title="${d.symbol} • Weight: ${d.weight.toFixed(1)}% • Return: ${sign}${d.returnPct.toFixed(2)}%">
                <div class="heatmap-symbol">${d.symbol}</div>
                <div class="heatmap-pct">${sign}${d.returnPct.toFixed(1)}%</div>
            </div>`;
        });

        html += '</div>';
        return html;
    },

    emptyState(title, desc, btnText, btnAction) {
        return `<div class="empty-state"><svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg><h2>${title}</h2><p>${desc}</p>${btnText ? `<button class="btn btn-primary" onclick="${btnAction}">${btnText}</button>` : ''}</div>`;
    },

    calcHealth() {
        const h = this.state.holdings;
        if (!h || h.length === 0) return 0;
        let score = 50;
        if (h.length >= 10) score += 15; else if (h.length >= 5) score += 8;
        const total = h.reduce((s, x) => s + (x.current_value || 0), 0);
        if (total > 0) {
            const maxW = Math.max(...h.map(x => (x.current_value || 0) / total * 100));
            if (maxW < 15) score += 15; else if (maxW < 25) score += 8; else score -= 5;
        }
        const sectors = new Set(h.map(x => x.sector).filter(Boolean));
        if (sectors.size >= 5) score += 15; else if (sectors.size >= 3) score += 8;
        const profitable = h.filter(x => x.pnl > 0).length;
        if (profitable / h.length > 0.6) score += 5;
        return Math.min(100, Math.max(0, score));
    },

    healthText(s) {
        if (s >= 80) return 'Excellent diversification. Your portfolio is well-balanced across sectors and stocks.';
        if (s >= 60) return 'Good portfolio health. Consider adding more sectors for better diversification.';
        if (s >= 40) return 'Moderate risk. Your portfolio has some concentration. Consider diversifying.';
        return 'High concentration risk. Your portfolio needs more diversification across sectors and stocks.';
    },

    fmt(n) {
        if (n === undefined || n === null || isNaN(n)) return '₹0';
        return '₹' + Number(n).toLocaleString('en-IN', { maximumFractionDigits: 2 });
    },

    fmtPct(n) {
        if (n === undefined || n === null || isNaN(n)) return '0%';
        const sign = n >= 0 ? '+' : '';
        return sign + Number(n).toFixed(2) + '%';
    },

    esc(s) {
        const d = document.createElement('div');
        d.textContent = s || '';
        return d.innerHTML;
    },

    // ── Modals ──
    showImportModal() {
        this.state.files = {};
        document.querySelectorAll('.file-drop').forEach(el => { el.classList.remove('has-file'); });
        document.querySelectorAll('.drop-file').forEach(el => { el.textContent = ''; });
        document.querySelectorAll('.modal-body input[type="file"]').forEach(el => { el.value = ''; });
        document.getElementById('import-portfolio-name').value = 'My Portfolio';
        document.getElementById('import-modal').classList.add('active');
    },

    showTxnModal() {
        document.getElementById('txn-date').value = new Date().toISOString().split('T')[0];
        document.getElementById('txn-modal').classList.add('active');
    },

    showWatchlistModal() {
        document.getElementById('watchlist-modal').classList.add('active');
    },

    closeModal(e) {
        if (e && e.target && !e.target.classList.contains('modal-overlay')) return;
        document.querySelectorAll('.modal-overlay').forEach(m => m.classList.remove('active'));
    },

    handleFile(input, type) {
        const file = input.files[0];
        if (!file) return;
        this.state.files[type] = file;
        const dropEl = document.getElementById(`drop-${type === 'symbolmap' ? 'symbolmap' : type}`);
        const nameEl = document.getElementById(`drop-${type === 'symbolmap' ? 'symbolmap' : type}-name`);
        dropEl.classList.add('has-file');
        nameEl.textContent = file.name;
    },

    async importCSV() {
        const holdings = this.state.files.holdings;
        if (!holdings) { this.toast('Please select a Holdings CSV file', 'error'); return; }
        const btn = document.getElementById('btn-import');
        btn.textContent = 'Importing...';
        btn.disabled = true;

        const form = new FormData();
        form.append('holdings', holdings);
        form.append('portfolio_name', document.getElementById('import-portfolio-name').value || 'My Portfolio');
        if (this.state.portfolioId) form.append('portfolio_id', this.state.portfolioId);
        if (this.state.files.gainloss) form.append('gainloss', this.state.files.gainloss);
        if (this.state.files.symbolmap) form.append('symbol_map', this.state.files.symbolmap);

        try {
            const res = await this.api.postForm('/api/import', form);
            if (res.error) { this.toast(res.error, 'error'); return; }
            this.state.portfolioId = res.portfolio_id;
            await this.loadPortfolios();
            document.getElementById('portfolio-select').value = this.state.portfolioId;
            this.closeModal();
            this.toast(`Imported ${res.holdings_count} holdings successfully`);
            this.navigate('dashboard');
        } catch (e) {
            this.toast('Import failed: ' + e.message, 'error');
        } finally {
            btn.textContent = 'Import Portfolio';
            btn.disabled = false;
        }
    },

    async addTransaction() {
        const data = {
            symbol: document.getElementById('txn-symbol').value,
            type: document.getElementById('txn-type').value,
            quantity: parseFloat(document.getElementById('txn-qty').value),
            price: parseFloat(document.getElementById('txn-price').value),
            date: document.getElementById('txn-date').value,
            notes: document.getElementById('txn-notes').value,
        };
        if (!data.symbol || !data.quantity || !data.price) { this.toast('Please fill in required fields', 'error'); return; }
        await this.api.post(`/api/portfolios/${this.state.portfolioId}/transactions`, data);
        this.closeModal();
        this.toast('Transaction added');
        this.navigate('transactions');
    },

    async addToWatchlist() {
        const data = {
            symbol: document.getElementById('wl-symbol').value,
            name: document.getElementById('wl-name').value,
            target_price: parseFloat(document.getElementById('wl-target').value) || null,
            notes: document.getElementById('wl-notes').value,
        };
        if (!data.symbol) { this.toast('Please enter a symbol', 'error'); return; }
        await this.api.post('/api/watchlist', data);
        this.closeModal();
        this.toast('Added to watchlist');
        this.navigate('watchlist');
    },

    async removeWatchlist(symbol) {
        await this.api.del(`/api/watchlist/${encodeURIComponent(symbol)}`);
        this.toast('Removed from watchlist');
        this.navigate('watchlist');
    },

    exportCSV() {
        const h = this.state.holdings;
        if (!h || h.length === 0) { this.toast('No holdings to export'); return; }
        const headers = ['Name', 'Symbol', 'ISIN', 'Qty', 'Avg Price', 'LTP', 'Invested', 'Current Value', 'P&L', 'Return %', 'Sector'];
        const rows = h.map(r => [r.name, r.symbol, r.isin, r.quantity, r.avg_price, r.ltp, r.invested_value, r.current_value, r.pnl, (r.return_pct || 0).toFixed(2), r.sector || '']);
        const csv = [headers, ...rows].map(r => r.map(v => `"${v}"`).join(',')).join('\n');
        const blob = new Blob([csv], { type: 'text/csv' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `portfolio_${new Date().toISOString().split('T')[0]}.csv`;
        a.click();
    },

    toast(msg, type) {
        const el = document.getElementById('toast');
        el.textContent = msg;
        el.style.background = type === 'error' ? 'var(--red)' : 'var(--text-primary)';
        el.classList.add('visible');
        setTimeout(() => el.classList.remove('visible'), 3000);
    },
};

// Boot
document.addEventListener('DOMContentLoaded', () => App.init());
