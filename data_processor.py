import pandas as pd
import re


def load_csv_smart(file_or_path):
    """Try multiple strategies to load a CSV with broker header rows."""
    # Known data column keywords to validate we found the real header
    HEADER_KEYWORDS = {'isin', 'name', 'quantity', 'qty', 'code', 'security',
                       'symbol', 'rate', 'valuation', 'holding', 'current'}

    def _looks_like_header(df):
        """Check if the DataFrame columns look like real data headers."""
        if df.shape[1] < 3 or df.shape[0] < 1:
            return False
        col_str = ' '.join(str(c).lower() for c in df.columns if 'unnamed' not in str(c).lower())
        return any(kw in col_str for kw in HEADER_KEYWORDS)

    # Try comma-separated first (standard for Indian broker CSVs)
    for sep in [',', ';', '\t', None]:
        for skip in range(25):
            try:
                if hasattr(file_or_path, 'seek'):
                    file_or_path.seek(0)
                kwargs = {'skiprows': skip, 'on_bad_lines': 'skip'}
                if sep is None:
                    kwargs['sep'] = None
                    kwargs['engine'] = 'python'
                else:
                    kwargs['sep'] = sep
                    kwargs['engine'] = 'python'
                df = pd.read_csv(file_or_path, **kwargs)
                if _looks_like_header(df):
                    return df
            except Exception:
                continue

    # Final fallback
    if hasattr(file_or_path, 'seek'):
        file_or_path.seek(0)
    return pd.read_csv(file_or_path, engine="python", on_bad_lines="skip")


def normalize_columns(df):
    """Normalize column names and rename common broker-specific variants."""
    df = df.copy()

    # Identify 'lakhs'/'lacs' columns before removing characters
    lakhs_cols = []
    for c in df.columns:
        c_str = str(c).lower()
        if ('lakh' in c_str or 'lac' in c_str) and ('value' in c_str or 'amount' in c_str or 'fair' in c_str):
            lakhs_cols.append(c)

    # Convert to lowercase, replace non-alphanumeric with underscore, strip underscores
    new_cols = []
    for c in df.columns:
        if c in lakhs_cols:
            new_cols.append('current_value_in_lakhs')
        else:
            new_cols.append(re.sub(r'[^a-z0-9]', '_', str(c).strip().lower()).strip('_'))
    df.columns = new_cols

    renames = {
        # ISIN variants
        'isin_no': 'isin',
        'isin_no_': 'isin',

        # Name variants
        'isin_name': 'name',
        'instrument': 'name',
        'stock': 'name',
        'stock_name': 'name',
        'security': 'name',
        'scrip_name': 'name',
        'name_of_the_instrument': 'name',
        'name_of_instrument': 'name',
        
        # Sector variants
        'industry_classification': 'sector',
        'sector_name': 'sector',

        # Quantity variants
        'qty': 'quantity',
        'qty_': 'quantity',
        'shares': 'quantity',
        'holding_qty': 'quantity',

        # Avg price / cost variants
        'avg_cost': 'avg_price',
        'average_price': 'avg_price',
        'buy_price': 'avg_price',
        'buy_avg': 'avg_price',
        'average_cost': 'avg_price',
        'holding_rate': 'avg_price',

        # Invested value variants
        'invested_value': 'invested',
        'invested_amount': 'invested',
        'cost_value': 'invested',
        'holding_cost': 'invested',

        # Current price (LTP) variants
        'rate': 'ltp',
        'ltp': 'ltp',
        'last_price': 'ltp',
        'close': 'ltp',
        'current_rate': 'ltp',

        # Current value variants
        'cur_val': 'current_value',
        'current_val': 'current_value',
        'market_value': 'current_value',
        'valuation': 'current_value',
        'current_amount': 'current_value',

        # P&L variants
        'p_l': 'pnl',
        'profit_loss': 'pnl',
        'unrealised_p_l': 'pnl',
        'unrealized_p_l': 'pnl',
    }

    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})
    return df


def parse_numeric(val):
    if pd.isna(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(',', '').replace('₹', '').replace('$', '').strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def process_holdings_csv(file_or_path):
    df = load_csv_smart(file_or_path)
    df = normalize_columns(df)
    for col in ['quantity', 'avg_price', 'ltp', 'current_value', 'pnl', 'invested', 'current_value_in_lakhs']:
        if col in df.columns:
            df[col] = df[col].apply(parse_numeric)
    return df


def merge_holdings_gainloss(holdings_df, gainloss_df):
    """Merge holdings (current positions) with gain/loss (cost basis) data."""
    h = normalize_columns(holdings_df.copy())
    g = normalize_columns(gainloss_df.copy())

    # We aggregate Holdings to combine Beneficiary and Pledged shares into a single entry
    if 'isin' in h.columns:
        group_key = 'isin'
    elif 'name' in h.columns:
        group_key = 'name'
    else:
        group_key = None

    if group_key:
        if 'quantity' in h.columns:
            h['quantity'] = h['quantity'].apply(parse_numeric)
        # Setup aggregation rules: sum for quantity, keep first for all other fields
        agg_funcs = {col: 'first' for col in h.columns if col != group_key}
        if 'quantity' in h.columns:
            agg_funcs['quantity'] = 'sum'
        h = h.groupby(group_key, as_index=False).agg(agg_funcs)

    # Filter Gain Loss: keep only positive quantity rows (exclude sold stocks)
    if 'quantity' in g.columns:
        g['quantity'] = g['quantity'].apply(parse_numeric)
        g = g[g['quantity'] > 0].copy()

    # Parse numeric columns
    for col in ['quantity', 'ltp', 'current_value', 'current_value_in_lakhs']:
        if col in h.columns:
            h[col] = h[col].apply(parse_numeric)
    for col in ['quantity', 'avg_price', 'invested', 'ltp', 'current_value', 'current_value_in_lakhs']:
        if col in g.columns:
            g[col] = g[col].apply(parse_numeric)

    # Merge on ISIN — LEFT join so all holdings are kept even without gain/loss data
    if 'isin' in h.columns and 'isin' in g.columns:
        merged = pd.merge(h, g, on='isin', how='left', suffixes=('', '_gl'))
    elif 'name' in h.columns and 'name' in g.columns:
        merged = pd.merge(h, g, on='name', how='left', suffixes=('', '_gl'))
    else:
        return h

    return merged


# Direct ISIN → Yahoo Finance symbol mapping for Indian stocks
ISIN_TO_SYMBOL = {
    'INE713T01028': ('APOLLO.NS', 'Defence'),
    'INE296A01032': ('BAJFINANCE.NS', 'Finance'),
    'INE377Y01014': ('BAJAJHFL.NS', 'Finance'),
    'INE05XR01022': ('BHARATCOAL.NS', 'Mining'),
    'INE171Z01026': ('BDL.NS', 'Defence'),
    'INE0HOQ01053': ('GROWW.NS', 'Fintech'),
    'INE153T01027': ('BLS.NS', 'Services'),
    'INE736A01011': ('CDSL.NS', 'Finance'),
    'INE501A01019': ('DEEPAKFERT.NS', 'Chemicals'),
    'INE040A01034': ('HDFCBANK.NS', 'Banking'),
    'INE066F01020': ('HAL.NS', 'Defence'),
    'INE379A01028': ('ITCHOTELS.NS', 'Hotels'),
    'INE718I01012': ('JSWCEMENT.NS', 'Cement'),
    'INE324D01010': ('LGEINDIA.NS', 'Electronics'),
    'INE301O01023': ('NSDL.BO', 'Finance'),
    'INE095N01031': ('NBCC.NS', 'Construction'),
    'INE733E01010': ('NTPC.NS', 'Power'),
    'INE1NPP01017': ('ENRIN.NS', 'Energy'),
    'INE003A01024': ('SIEMENS.NS', 'Capital Goods'),
    'INE062A01020': ('SBIN.NS', 'Banking'),
    'INE040H01021': ('SUZLON.NS', 'Energy'),
    'INE398R01022': ('SYNGENE.NS', 'Pharma'),
    'INE976I01016': ('TATACAP.NS', 'Finance'),
    'INE142M01025': ('TATATECH.NS', 'IT'),
    'INE245A01021': ('TATAPOWER.NS', 'Power'),
    'INE669E01016': ('IDEA.NS', 'Telecom'),
    'INE377N01017': ('WAAREEENER.NS', 'Energy'),
    'INE596F01018': ('PTCIL.NS', 'Industrial Products'), # Fix for PTC Industries
    'INE0LXG01040': ('OLAELEC.NS', 'Automobiles'),
    'INE0BS701011': ('PREMIERENE.NS', 'Energy'),
    'INE775A01035': ('MOTHERSON.NS', 'Auto Components'),
}


def _clean_str(val):
    """Return clean string or empty string (never 'nan')."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ''
    s = str(val).strip()
    if s.lower() == 'nan':
        return ''
    return s


def extract_holdings_data(df, symbol_map=None):
    """Extract standardized holdings data from a (possibly merged) DataFrame."""
    holdings = []

    for _, row in df.iterrows():
        # Get ISIN
        isin = ''
        for col in ['isin', 'isin_gl']:
            val = _clean_str(row.get(col))
            if val:
                isin = val
                break

        # Get name — prefer the longer/more descriptive version
        name = ''
        for col in ['name', 'name_gl']:
            val = _clean_str(row.get(col))
            if len(val) > len(name):
                name = val
        # Clean up name: remove parenthetical notes like "(EQ NEW FV RE 1/-)"
        name = re.sub(r'\s*\(.*?\)\s*$', '', name).strip()

        # Get quantity — prefer Holdings (no suffix) as it's current position
        quantity = parse_numeric(row.get('quantity', 0))
        if quantity <= 0:
            quantity = parse_numeric(row.get('quantity_gl', 0))
        if quantity <= 0:
            continue  # Skip zero/negative qty

        # Get avg_price — from gain/loss data (holding rate / avg cost)
        avg_price = parse_numeric(row.get('avg_price', 0))
        if avg_price == 0:
            avg_price = parse_numeric(row.get('avg_price_gl', 0))
        # Fallback 1: use LTP from holdings if no avg price available
        if avg_price == 0:
            avg_price = parse_numeric(row.get('ltp', 0))
        # Fallback 2: deduce from current_value (or current_value_in_lakhs) if no avg cost present
        if avg_price == 0:
            cv = parse_numeric(row.get('current_value', 0))
            if cv == 0 and 'current_value_in_lakhs' in row:
                cv = parse_numeric(row.get('current_value_in_lakhs', 0)) * 100000.0
            if cv > 0 and quantity > 0:
                avg_price = cv / float(quantity)

        # Always calculate invested value from quantity * avg_price.
        # This ensures correctness when Beneficiary + Pledge shares are
        # aggregated (the Gain/Loss 'invested' column only covers the
        # original Gain/Loss quantity, not the combined total).
        invested = quantity * avg_price

        # --- Resolve symbol and sector ---
        symbol = ''
        sector = _clean_str(row.get('sector', ''))

        # 1) Try direct ISIN mapping (most reliable)
        if isin in ISIN_TO_SYMBOL:
            direct_symbol, direct_sector = ISIN_TO_SYMBOL[isin]
            symbol = symbol or direct_symbol
            sector = sector or direct_sector

        # 2) Try symbol map from DB/CSV
        if not symbol and symbol_map and isin in symbol_map:
            sm = symbol_map[isin]
            symbol = _clean_str(sm.get('symbol', ''))
            sector = _clean_str(sm.get('sector', '')) or sector
            if not name and sm.get('name'):
                name = _clean_str(sm['name'])

        holdings.append({
            'isin': isin,
            'name': name,
            'quantity': quantity,
            'avg_price': avg_price,
            'invested_value': invested,
            'symbol': symbol,
            'sector': sector,
        })

    return holdings


def process_symbol_map_csv(file_or_path):
    """Process symbol map CSV."""
    df = load_csv_smart(file_or_path)
    df = normalize_columns(df)

    mappings = []
    for _, row in df.iterrows():
        isin = _clean_str(row.get('isin', ''))
        if not isin:
            continue
        symbol = _clean_str(row.get('symbol', row.get('yahoosymbol', row.get('yahoo_symbol', ''))))
        sector = _clean_str(row.get('sector', ''))
        name = _clean_str(row.get('name', row.get('security', '')))

        # If symbol is empty, try ISIN direct mapping
        if not symbol and isin in ISIN_TO_SYMBOL:
            symbol, sector = ISIN_TO_SYMBOL[isin]

        mappings.append({
            'isin': isin,
            'name': name,
            'symbol': symbol,
            'sector': sector,
        })
    return mappings


# Known Indian stock ISIN -> Yahoo symbol mappings
KNOWN_SYMBOLS = {
    'HDFCBANK': 'HDFCBANK.NS', 'SBIN': 'SBIN.NS', 'STATEBANKOFINDIA': 'SBIN.NS',
    'STATEBANKOF': 'SBIN.NS',
    'RELIANCE': 'RELIANCE.NS', 'TCS': 'TCS.NS', 'INFOSYS': 'INFY.NS',
    'ITC': 'ITC.NS', 'TATAMOTORS': 'TATAMOTORS.NS', 'TATAPOWER': 'TATAPOWER.NS',
    'TATAPOWERCO': 'TATAPOWER.NS', 'THETATAPOWER': 'TATAPOWER.NS',
    'BAJFINANCE': 'BAJFINANCE.NS', 'BAJAJFINANCE': 'BAJFINANCE.NS',
    'HAL': 'HAL.NS', 'HINDUSTANAERONAUTICS': 'HAL.NS',
    'SUZLON': 'SUZLON.NS', 'SUZLONENERGY': 'SUZLON.NS',
    'NTPC': 'NTPC.NS', 'SIEMENS': 'SIEMENS.NS', 'SIEMENSLIMIT': 'SIEMENS.NS',
    'NBCC': 'NBCC.NS', 'NBCCINDIA': 'NBCC.NS',
    'BDL': 'BDL.NS', 'BHARATDYNAMICS': 'BDL.NS',
    'VODAFONEIDEA': 'IDEA.NS', 'IDEA': 'IDEA.NS',
    'TATATECH': 'TATATECH.NS', 'TATATECHNOLOGIES': 'TATATECH.NS',
    'BAJAJHOUSINGFINANCE': 'BAJAJHFL.NS', 'BAJAJHOUSING': 'BAJAJHFL.NS',
    'DEEPAKFERTILISERS': 'DEEPAKFERT.NS', 'DEEPAKFERTILIZERS': 'DEEPAKFERT.NS',
    'WAAREE': 'WAAREEENER.NS', 'WAAREEENERGIES': 'WAAREEENER.NS',
    'ITCHOTELS': 'ITCHOTELS.NS',
    'BLS': 'BLS.NS', 'BLSINTERNATIONAL': 'BLS.NS', 'BLSINTERNATIONALSERVICES': 'BLS.NS',
    'APOLLOMICROSYSTEMS': 'APOLLOMICRO.NS', 'APOLLOMICRO': 'APOLLOMICRO.NS',
    'BHARATCOKINGCOAL': 'BCC.NS',
    'BILLIONBRAINSGARAGEVENTURES': 'BIKAJI.NS',
    'CENTRALDEPOSITORYSERVICESINDIA': 'CDSL.NS', 'CDSL': 'CDSL.NS',
    'HDFCBANK': 'HDFCBANK.NS', 'HDFCBANKLIMIT': 'HDFCBANK.NS',
    'JSWCEMENT': 'JSWCEMENT.NS',
    'LGELECTRONICSINDIA': 'LGEINFRA.NS',
    'NATIONALSECURITIESDEPOSITORY': 'NSDL.BO', 'NSDL': 'NSDL.BO',
    'SIEMENSENERGY': 'SIEMENSENGY.NS', 'SIEMENSENERGYINDIA': 'SIEMENSENGY.NS',
    'SYNGENE': 'SYNGENE.NS', 'SYNGENEINTERNATIONAL': 'SYNGENE.NS',
    'TATACAPITAL': 'TATACAPITAL.NS',
    'BHARATELECTRONICS': 'BEL.NS', 'BHARATPETROLEUM': 'BPCL.NS',
    'INDIANOIL': 'IOC.NS', 'POWERFINANCE': 'PFC.NS', 
    'HINDUSTANPETROLEUM': 'HINDPETRO.NS', 'BANKOFBARODA': 'BANKBARODA.NS',
    'CANARABANK': 'CANBK.NS', 'RECLIMITED': 'RECLTD.NS', 'REC': 'RECLTD.NS',
    'STEELAUTHORITY': 'SAIL.NS', 'PUNJABNATIONALBANK': 'PNB.NS',
    'UNIONBANKOFINDIA': 'UNIONBANK.NS', 'LIFEINSURANCECORPORATION': 'LICI.NS',
    'INDIANBANK': 'INDIANB.NS', 'PETRONETLNG': 'PETRONET.NS',
    'BANKOFINDIA': 'BANKINDIA.NS', 'LICHOUSINGFINANCE': 'LICHSGFIN.NS',
    'OILNATURALGAS': 'ONGC.NS', 'COALINDIA': 'COALINDIA.NS',
    'TATAMOTORSPASSENGER': 'TATAMOTORS.NS', 'GAIL': 'GAIL.NS',
    'HINDALCO': 'HINDALCO.NS', 'AWLAGRIBUSINESS': 'AWL.NS',
    'GENERALINSURANCECORPORATION': 'GICRE.NS', 'BANKOFMAHARASHTRA': 'MAHABANK.NS',
    'ACC': 'ACC.NS', 'BANDHANBANK': 'BANDHANBNK.NS', 'CENTRALBANKOFINDIA': 'CENTRALBK.NS',
    'THENEWINDIAASSURANCE': 'NIACL.NS', 'UCOBANK': 'UCOBANK.NS',
}


def auto_resolve_symbol(name, exchange='NS'):
    """Try to auto-resolve a stock name to a Yahoo Finance symbol."""
    name_clean = name.upper().strip()
    # Remove common suffixes
    name_clean = re.sub(r'\s*(LIMITED|LTD|CORPORATION|CORP|INDIA|COMPANY|CO)\s*$', '', name_clean).strip()
    name_clean = re.sub(r'\s*(LIMITED|LTD|CORPORATION|CORP)\s*$', '', name_clean).strip()
    # Remove parenthetical content
    name_clean = re.sub(r'\(.*?\)', '', name_clean).strip()
    # Remove spaces and special chars for lookup
    lookup_key = re.sub(r'[^A-Z0-9]', '', name_clean)

    if lookup_key in KNOWN_SYMBOLS:
        return KNOWN_SYMBOLS[lookup_key]

    # Try shorter prefixes
    for length in [15, 12, 10, 8]:
        prefix = lookup_key[:length]
        if prefix in KNOWN_SYMBOLS:
            return KNOWN_SYMBOLS[prefix]

    # Generic guess: first word as symbol
    words = name_clean.split()
    if words:
        guess = re.sub(r'[^A-Z]', '', words[0])
        if guess:
            return f"{guess}.{exchange}"

    return f"{lookup_key[:12]}.{exchange}"
