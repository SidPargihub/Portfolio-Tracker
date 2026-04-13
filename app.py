import os
from flask import Flask, render_template, request, jsonify
import database as db
import data_processor as dp
import price_fetcher as pf
import xirr_calculator as xirr

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB

db.init_db()


def _persist_resolved_symbols():
    """Save any symbols that were auto-resolved via ISIN lookup to the DB
    symbol_map, so future imports use the correct Yahoo Finance ticker."""
    resolved = pf.get_resolved_symbols()
    if not resolved:
        return
    sym_map = db.get_symbol_map()
    for orig_sym, new_sym in resolved.items():
        for isin, entry in sym_map.items():
            if entry.get('symbol') == orig_sym:
                db.save_symbol_mapping(isin, entry.get('name', ''), new_sym, entry.get('sector', ''))
                break


@app.route('/')
def index():
    return render_template('index.html')


# ── Portfolios ──

@app.route('/api/portfolios', methods=['GET'])
def get_portfolios():
    return jsonify(db.get_portfolios())


@app.route('/api/portfolios', methods=['POST'])
def create_portfolio():
    data = request.json
    pid = db.create_portfolio(data.get('name', 'My Portfolio'))
    return jsonify({'id': pid, 'name': data.get('name')})


@app.route('/api/portfolios/<int:pid>', methods=['PUT'])
def update_portfolio(pid):
    db.rename_portfolio(pid, request.json.get('name', 'Portfolio'))
    return jsonify({'ok': True})


@app.route('/api/portfolios/<int:pid>', methods=['DELETE'])
def delete_portfolio(pid):
    db.delete_portfolio(pid)
    return jsonify({'ok': True})


# ── Holdings ──

@app.route('/api/portfolios/<int:pid>/holdings', methods=['GET'])
def get_holdings(pid):
    holdings = db.get_holdings(pid)
    symbols = list({h['symbol'] for h in holdings if h.get('symbol')})

    # Build symbol → ISIN map so price_fetcher can resolve broken symbols via ISIN
    isin_map = {h['symbol']: h['isin'] for h in holdings if h.get('symbol') and h.get('isin')}
    prices = pf.fetch_prices(symbols, isin_map=isin_map)

    # Auto-update DB mappings when ISIN lookup discovers a better symbol
    _persist_resolved_symbols()

    total_value = 0
    total_invested = 0

    for h in holdings:
        sym = h.get('symbol', '')
        if sym in prices:
            p = prices[sym]
            h['ltp'] = p['price']
            h['day_change'] = p['change']
            h['day_change_pct'] = p['change_pct']
            h['current_value'] = h['quantity'] * p['price']
        else:
            h['ltp'] = h.get('avg_price', 0)
            h['day_change'] = 0
            h['day_change_pct'] = 0
            h['current_value'] = h['quantity'] * h.get('avg_price', 0)

        h['pnl'] = h['current_value'] - h.get('invested_value', 0)
        inv = h.get('invested_value', 0)
        h['return_pct'] = (h['pnl'] / inv * 100) if inv > 0 else 0

        total_value += h['current_value']
        total_invested += inv

    if total_value > 0:
        db.save_snapshot(pid, total_value, total_invested, total_value - total_invested)

    return jsonify({
        'holdings': holdings,
        'summary': {
            'total_value': total_value,
            'total_invested': total_invested,
            'total_pnl': total_value - total_invested,
            'total_return_pct': ((total_value - total_invested) / total_invested * 100) if total_invested > 0 else 0,
            'holdings_count': len(holdings),
        }
    })


# ── Import ──

@app.route('/api/import', methods=['POST'])
def import_csv():
    holdings_file = request.files.get('holdings')
    if not holdings_file:
        return jsonify({'error': 'Holdings CSV is required'}), 400

    try:
        holdings_df = dp.process_holdings_csv(holdings_file)

        gainloss_file = request.files.get('gainloss')
        if gainloss_file:
            gainloss_df = dp.process_holdings_csv(gainloss_file)
            holdings_df = dp.merge_holdings_gainloss(holdings_df, gainloss_df)

        symbol_map_file = request.files.get('symbol_map')
        if symbol_map_file:
            mappings = dp.process_symbol_map_csv(symbol_map_file)
            db.save_symbol_map_bulk(mappings)

        symbol_map = db.get_symbol_map()
        holdings_data = dp.extract_holdings_data(holdings_df, symbol_map)

        for h in holdings_data:
            if not h.get('symbol') and h.get('name'):
                h['symbol'] = dp.auto_resolve_symbol(h['name'])
                if h.get('isin'):
                    db.save_symbol_mapping(h['isin'], h['name'], h['symbol'], h.get('sector'))

        portfolio_id = request.form.get('portfolio_id')
        if portfolio_id:
            portfolio_id = int(portfolio_id)
        else:
            portfolio_id = db.create_portfolio(request.form.get('portfolio_name', 'My Portfolio'))

        db.save_holdings(portfolio_id, holdings_data)

        return jsonify({
            'portfolio_id': portfolio_id,
            'holdings_count': len(holdings_data),
            'message': f'Imported {len(holdings_data)} holdings successfully',
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ── Edit Holding ──

@app.route('/api/holdings/<int:hid>', methods=['PATCH'])
def update_holding(hid):
    """Update editable fields of a single holding."""
    d = request.json
    db.update_holding(hid, d)
    return jsonify({'ok': True})


# ── Sectors ──

@app.route('/api/portfolios/<int:pid>/sectors', methods=['GET'])
def get_sectors(pid):
    holdings = db.get_holdings(pid)
    symbols = list({h['symbol'] for h in holdings if h.get('symbol')})
    isin_map = {h['symbol']: h['isin'] for h in holdings if h.get('symbol') and h.get('isin')}
    prices = pf.fetch_prices(symbols, isin_map=isin_map)

    sectors = {}
    total_val = 0
    total_inv = 0
    for h in holdings:
        sym = h.get('symbol', '')
        price = prices.get(sym, {}).get('price', h.get('avg_price', 0))
        value = h['quantity'] * price
        inv_value = h.get('invested_value', 0)
        
        sector = h.get('sector') or 'Unclassified'
        if sector not in sectors:
            sectors[sector] = {'name': sector, 'value': 0, 'invested': 0, 'holdings': []}
            
        sectors[sector]['value'] += value
        sectors[sector]['invested'] += inv_value
        sectors[sector]['holdings'].append({'name': h.get('name', sym), 'symbol': sym, 'value': value, 'invested': inv_value})
        total_val += value
        total_inv += inv_value

    result = []
    for s in sectors.values():
        s['weight'] = (s['value'] / total_val * 100) if total_val > 0 else 0
        s['invested_weight'] = (s['invested'] / total_inv * 100) if total_inv > 0 else 0
        result.append(s)
        
    result.sort(key=lambda x: x['value'], reverse=True)
    return jsonify(result)


# ── History ──

@app.route('/api/portfolios/<int:pid>/history', methods=['GET'])
def get_history(pid):
    return jsonify(db.get_snapshots(pid))


# ── Watchlist ──

@app.route('/api/watchlist', methods=['GET'])
def get_watchlist():
    items = db.get_watchlist()
    symbols = [i['symbol'] for i in items]
    prices = pf.fetch_prices(symbols)
    for item in items:
        if item['symbol'] in prices:
            p = prices[item['symbol']]
            item['price'] = p['price']
            item['change'] = p['change']
            item['change_pct'] = p['change_pct']
        else:
            item['price'] = 0
            item['change'] = 0
            item['change_pct'] = 0
    return jsonify(items)


@app.route('/api/watchlist', methods=['POST'])
def add_watchlist():
    d = request.json
    db.add_to_watchlist(d['symbol'], d.get('name'), d.get('target_price'), d.get('notes'))
    return jsonify({'ok': True})


@app.route('/api/watchlist/<path:symbol>', methods=['DELETE'])
def remove_watchlist(symbol):
    db.remove_from_watchlist(symbol)
    return jsonify({'ok': True})


# ── Transactions ──

@app.route('/api/portfolios/<int:pid>/transactions', methods=['GET'])
def get_transactions(pid):
    return jsonify(db.get_transactions(pid))


@app.route('/api/portfolios/<int:pid>/transactions', methods=['POST'])
def add_transaction(pid):
    d = request.json
    db.add_transaction(pid, d['symbol'], d.get('name', ''), d['type'], d['quantity'], d['price'], d['date'], d.get('notes'))
    return jsonify({'ok': True})


# ── XIRR ──

@app.route('/api/portfolios/<int:pid>/xirr', methods=['GET'])
def get_xirr(pid):
    """Compute XIRR (annualized return) for a portfolio."""
    holdings = db.get_holdings(pid)
    if not holdings:
        return jsonify({'xirr': None, 'error': 'No holdings found'})

    # Get live prices to compute current values
    symbols = list({h['symbol'] for h in holdings if h.get('symbol')})
    isin_map = {h['symbol']: h['isin'] for h in holdings if h.get('symbol') and h.get('isin')}
    prices = pf.fetch_prices(symbols, isin_map=isin_map)

    for h in holdings:
        sym = h.get('symbol', '')
        if sym in prices:
            h['current_value'] = h['quantity'] * prices[sym]['price']
        else:
            h['current_value'] = h['quantity'] * h.get('avg_price', 0)

    # Try transactions first for precise XIRR
    transactions = db.get_transactions(pid, limit=10000)

    xirr_val = xirr.portfolio_xirr(holdings, transactions if transactions else None)

    return jsonify({
        'xirr': round(xirr_val * 100, 2) if xirr_val is not None else None,
        'cashflows_count': len(transactions) if transactions else len(holdings),
        'method': 'transactions' if transactions else 'holdings_cost_basis',
    })


# ── Benchmark ──

@app.route('/api/portfolios/<int:pid>/benchmark', methods=['GET'])
def get_benchmark(pid):
    """Return portfolio vs Nifty 500 benchmark comparison data."""
    period = request.args.get('period', '1y')

    # Get portfolio snapshots
    snapshots = db.get_snapshots(pid)
    if not snapshots:
        return jsonify({'error': 'No portfolio snapshots available'})

    # Get benchmark data
    benchmark_data = pf.fetch_benchmark_history(period=period)
    benchmark_name = 'Nifty 500'

    if not benchmark_data:
        return jsonify({
            'error': 'Could not fetch benchmark data',
            'portfolio': [],
            'benchmark': [],
        })

    # Build date-indexed maps
    snap_map = {s['date']: s for s in snapshots}
    bench_map = {b['date']: b['close'] for b in benchmark_data}

    # Find overlapping dates, or align to nearest
    all_bench_dates = sorted(bench_map.keys())
    all_snap_dates = sorted(snap_map.keys())

    # Normalize both to base 100 from their respective start points
    portfolio_series = []
    benchmark_series = []

    if all_snap_dates and all_bench_dates:
        # Use snapshot dates, find closest benchmark value for each
        snap_base = snap_map[all_snap_dates[0]]['total_value']

        # Find the benchmark value closest to the first snapshot date
        first_snap_date = all_snap_dates[0]
        bench_start_val = None
        for bd in all_bench_dates:
            if bd <= first_snap_date:
                bench_start_val = bench_map[bd]
            elif bench_start_val is None:
                bench_start_val = bench_map[bd]
                break

        if bench_start_val and snap_base > 0:
            for sd in all_snap_dates:
                snap = snap_map[sd]
                portfolio_series.append({
                    'date': sd,
                    'value': round((snap['total_value'] / snap_base) * 100, 2),
                    'raw_value': snap['total_value'],
                })

                # Find closest benchmark date
                closest_bench = None
                for bd in all_bench_dates:
                    if bd <= sd:
                        closest_bench = bench_map[bd]
                if closest_bench:
                    benchmark_series.append({
                        'date': sd,
                        'value': round((closest_bench / bench_start_val) * 100, 2),
                        'raw_value': closest_bench,
                    })

    # Also provide full benchmark series for richer chart
    full_benchmark = []
    if all_bench_dates:
        bench_base = bench_map[all_bench_dates[0]]
        if bench_base > 0:
            for bd in all_bench_dates:
                full_benchmark.append({
                    'date': bd,
                    'value': round((bench_map[bd] / bench_base) * 100, 2),
                    'raw_value': bench_map[bd],
                })

    # Calculate returns for rolling periods
    def calc_return(data, days):
        if not data or len(data) < 2:
            return None
        end = data[-1]['raw_value']
        target_idx = max(0, len(data) - days - 1)
        start = data[target_idx]['raw_value']
        if start > 0:
            return round(((end / start) - 1) * 100, 2)
        return None

    portfolio_returns = {}
    benchmark_returns = {}
    for label, days in [('1M', 22), ('3M', 66), ('6M', 132), ('1Y', 252)]:
        portfolio_returns[label] = calc_return(portfolio_series, days)
        benchmark_returns[label] = calc_return(full_benchmark, days)

    return jsonify({
        'portfolio': portfolio_series,
        'benchmark': benchmark_series,
        'full_benchmark': full_benchmark,
        'benchmark_name': benchmark_name,
        'portfolio_returns': portfolio_returns,
        'benchmark_returns': benchmark_returns,
    })


# ── Symbol Map ──

@app.route('/api/symbol-map', methods=['GET'])
def get_symbol_map():
    return jsonify(list(db.get_symbol_map().values()))


@app.route('/api/symbol-map', methods=['POST'])
def update_symbol_map():
    d = request.json
    db.save_symbol_mapping(d['isin'], d.get('name'), d['symbol'], d.get('sector'))
    return jsonify({'ok': True})


if __name__ == '__main__':
    port = int(os.getenv('PORT', '5001'))
    print(f"\n  🚀 Portfolio Tracker running at http://localhost:{port}\n")
    app.run(debug=True, port=port)
