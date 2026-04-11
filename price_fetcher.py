import yfinance as yf
import time

_price_cache = {}
_CACHE_TTL = 60

# Persistent map of symbols that failed → working symbols resolved via ISIN.
# Avoids repeating slow ISIN lookups on every page load.
_resolved_symbols = {}


def fetch_prices(symbols, isin_map=None):
    """Batch-fetch live prices with 60s cache, multi-stage fallbacks, and
    automatic ISIN-based symbol resolution for any ticker that yfinance
    can't recognise.

    Args:
        symbols: list of Yahoo Finance ticker strings.
        isin_map: optional dict mapping symbol → ISIN (e.g. {'NSDL.BO': 'INE301O01023'}).
                  When provided, failed symbols are resolved via ISIN lookup as a
                  last-resort fallback.  The resolved (correct) symbol is cached in
                  _resolved_symbols so subsequent calls are fast.
    """
    if not symbols:
        return {}

    now = time.time()
    cached, to_fetch = {}, []

    for s in symbols:
        # If we previously resolved this symbol to a different one, use it
        effective = _resolved_symbols.get(s, s)
        if effective in _price_cache and (now - _price_cache[effective]['t']) < _CACHE_TTL:
            cached[s] = _price_cache[effective]['d']
        else:
            to_fetch.append(s)

    if not to_fetch:
        return cached

    # ── Stage 1: Batch-fetch all symbols (using resolved aliases where known) ──
    effective_to_orig = {}
    effective_list = []
    for s in to_fetch:
        eff = _resolved_symbols.get(s, s)
        effective_to_orig[eff] = s
        effective_list.append(eff)

    fetched_results = _do_fetch(effective_list)

    # Map results back to original symbols
    still_failed = []
    for eff, orig in effective_to_orig.items():
        if eff in fetched_results:
            _price_cache[eff] = {'d': fetched_results[eff], 't': now}
            cached[orig] = fetched_results[eff]
        else:
            still_failed.append(orig)

    # ── Stage 2: .NS → .BO exchange fallback ──
    if still_failed:
        bo_fallbacks = {}  # bo_symbol → original_symbol
        for s in still_failed:
            if s.endswith('.NS') and s not in _resolved_symbols:
                bo_sym = s[:-3] + '.BO'
                bo_fallbacks[bo_sym] = s

        if bo_fallbacks:
            bo_results = _do_fetch(list(bo_fallbacks.keys()))
            resolved_via_bo = set()
            for bo_sym, orig in bo_fallbacks.items():
                if bo_sym in bo_results:
                    _price_cache[bo_sym] = {'d': bo_results[bo_sym], 't': now}
                    _resolved_symbols[orig] = bo_sym
                    cached[orig] = bo_results[bo_sym]
                    resolved_via_bo.add(orig)
            still_failed = [s for s in still_failed if s not in resolved_via_bo]

    # ── Stage 3: ISIN-based resolution (universal last resort) ──
    if still_failed and isin_map:
        for orig_sym in list(still_failed):
            isin = isin_map.get(orig_sym)
            if not isin:
                continue
            resolved = _resolve_via_isin(isin)
            if resolved:
                _resolved_symbols[orig_sym] = resolved['symbol']
                _price_cache[resolved['symbol']] = {'d': resolved['data'], 't': now}
                cached[orig_sym] = resolved['data']
                still_failed.remove(orig_sym)

    return cached


def _do_fetch(ticker_list):
    """Download price data for a list of tickers. Returns {symbol: price_dict}."""
    res = {}
    if not ticker_list:
        return res
    try:
        data = yf.download(
            tickers=" ".join(ticker_list), period="5d", interval="1d",
            group_by="ticker", threads=True, progress=False
        )
        for s in ticker_list:
            try:
                if len(ticker_list) == 1:
                    closes = data["Close"].dropna()
                else:
                    if s in data and "Close" in data[s]:
                        closes = data[s]["Close"].dropna()
                    else:
                        continue

                if closes.empty:
                    continue

                price = float(closes.iloc[-1])
                if price <= 0:
                    continue

                prev = float(closes.iloc[-2]) if len(closes) >= 2 else price
                res[s] = {
                    'price': price, 'prev_close': prev,
                    'change': price - prev,
                    'change_pct': ((price - prev) / prev * 100) if prev else 0,
                }
            except Exception:
                continue
    except Exception:
        pass
    return res


def _resolve_via_isin(isin):
    """Use yfinance's ISIN lookup to find the correct ticker and fetch price data.
    Returns {'symbol': resolved_ticker, 'data': price_dict} or None."""
    try:
        t = yf.Ticker(isin)
        hist = t.history(period='5d')
        if hist.empty:
            return None

        closes = hist['Close'].dropna()
        if closes.empty:
            return None

        price = float(closes.iloc[-1])
        if price <= 0:
            return None

        prev = float(closes.iloc[-2]) if len(closes) >= 2 else price

        # Get the resolved Yahoo symbol from ticker info
        resolved_sym = isin  # default to ISIN if info lookup fails
        try:
            info = t.info
            if info and info.get('symbol'):
                resolved_sym = info['symbol']
        except Exception:
            pass

        return {
            'symbol': resolved_sym,
            'data': {
                'price': price, 'prev_close': prev,
                'change': price - prev,
                'change_pct': ((price - prev) / prev * 100) if prev else 0,
            }
        }
    except Exception:
        return None


def get_resolved_symbols():
    """Return the map of original_symbol → resolved_symbol for DB updates."""
    return dict(_resolved_symbols)


_benchmark_cache = {}
_BENCHMARK_CACHE_TTL = 300  # 5 minutes


def fetch_benchmark_history(ticker='^CRSLDX', period='1y', fallbacks=None):
    """Fetch historical close prices for a benchmark index.

    Args:
        ticker: Yahoo Finance ticker (default: Nifty 500 TRI).
        period: yfinance period string e.g. '1y', '6mo', '2y'.
        fallbacks: list of alternative tickers to try if primary fails.

    Returns:
        list of {'date': 'YYYY-MM-DD', 'close': float} dicts, or [].
    """
    if fallbacks is None:
        fallbacks = ['NIFTY500MULTICAP5050.NS', '0P0001BKJR.BO', '^NSEI']

    cache_key = f'{ticker}_{period}'
    now = time.time()
    if cache_key in _benchmark_cache and (now - _benchmark_cache[cache_key]['t']) < _BENCHMARK_CACHE_TTL:
        return _benchmark_cache[cache_key]['d']

    tickers_to_try = [ticker] + fallbacks
    for t in tickers_to_try:
        try:
            data = yf.download(t, period=period, interval='1d', progress=False)
            if data.empty:
                continue
            closes = data['Close'].dropna()
            if hasattr(closes, 'columns'):
                closes = closes.iloc[:, 0]
            if closes.empty:
                continue
            result = [
                {'date': d.strftime('%Y-%m-%d'), 'close': float(v)}
                for d, v in closes.items()
            ]
            if len(result) > 5:  # Need enough data points
                _benchmark_cache[cache_key] = {'d': result, 't': now}
                return result
        except Exception:
            continue

    return []


def fetch_stock_detail(symbol):
    """Detailed info for a single stock."""
    try:
        # Use resolved symbol if available
        effective = _resolved_symbols.get(symbol, symbol)
        t = yf.Ticker(effective)
        info = t.info or {}
        hist = t.history(period="1y")
        return {
            'symbol': symbol,
            'name': info.get('longName', info.get('shortName', symbol)),
            'price': info.get('currentPrice', info.get('regularMarketPrice', 0)),
            'prev_close': info.get('previousClose', 0),
            'day_high': info.get('dayHigh', 0),
            'day_low': info.get('dayLow', 0),
            'volume': info.get('volume', 0),
            'market_cap': info.get('marketCap', 0),
            'pe_ratio': info.get('trailingPE', 0),
            'week_52_high': info.get('fiftyTwoWeekHigh', 0),
            'week_52_low': info.get('fiftyTwoWeekLow', 0),
            'dividend_yield': info.get('dividendYield', 0),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'history': [
                {'date': d.strftime('%Y-%m-%d'), 'close': float(row['Close'])}
                for d, row in hist.iterrows()
            ] if not hist.empty else [],
        }
    except Exception as e:
        return {'symbol': symbol, 'error': str(e)}
