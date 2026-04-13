"""XIRR (Extended Internal Rate of Return) calculator.

Pure Python implementation using Newton-Raphson — no external deps needed.
"""

from datetime import datetime, date


def _xnpv(rate, cashflows):
    """Net present value for irregular cashflows.

    Args:
        rate: discount rate (annual)
        cashflows: list of (date, amount) tuples
    """
    if rate <= -1.0:
        return float('inf')
    d0 = cashflows[0][0]
    return sum(
        amount / (1.0 + rate) ** ((dt - d0).days / 365.0)
        for dt, amount in cashflows
    )


def _xnpv_deriv(rate, cashflows):
    """Derivative of XNPV with respect to rate."""
    if rate <= -1.0:
        return float('inf')
    d0 = cashflows[0][0]
    result = 0.0
    for dt, amount in cashflows:
        t = (dt - d0).days / 365.0
        denom = (1.0 + rate)
        if denom == 0:
            return float('inf')
        result -= t * amount / denom ** (t + 1.0)
    return result


def compute_xirr(cashflows, guess=0.1, max_iter=200, tol=1e-7):
    """Compute XIRR for a list of irregular cashflows.

    Args:
        cashflows: list of (date, amount) tuples.
                   Negative = money invested (outflow).
                   Positive = money received (inflow, e.g. current value).
        guess: initial rate guess (default 10%).
        max_iter: maximum Newton-Raphson iterations.
        tol: convergence tolerance.

    Returns:
        Annualized return as a float (e.g. 0.12 = 12%), or None if
        the solver fails to converge.
    """
    if not cashflows or len(cashflows) < 2:
        return None

    # Sort by date
    cashflows = sorted(cashflows, key=lambda x: x[0])

    # Sanity: need at least one negative and one positive flow
    has_neg = any(a < 0 for _, a in cashflows)
    has_pos = any(a > 0 for _, a in cashflows)
    if not (has_neg and has_pos):
        return None

    # Check if all dates are the same (happens with brand-new portfolios)
    if all(cf[0] == cashflows[0][0] for cf in cashflows):
        total = sum(a for _, a in cashflows)
        if total > 0:
            return None  # instantaneous gain can't be annualized
        return None

    rate = guess

    # Newton-Raphson iteration
    for _ in range(max_iter):
        npv = _xnpv(rate, cashflows)
        deriv = _xnpv_deriv(rate, cashflows)

        if abs(deriv) < 1e-12:
            # Derivative too small — try bisection fallback
            break

        new_rate = rate - npv / deriv

        # Clamp to avoid wild divergence
        new_rate = max(-0.999, min(new_rate, 10.0))

        if abs(new_rate - rate) < tol:
            return new_rate

        rate = new_rate

    # Newton-Raphson failed — try bisection as fallback
    return _bisection_xirr(cashflows)


def _bisection_xirr(cashflows, lo=-0.99, hi=5.0, tol=1e-6, max_iter=300):
    """Bisection fallback for XIRR when Newton-Raphson diverges."""
    npv_lo = _xnpv(lo, cashflows)
    npv_hi = _xnpv(hi, cashflows)

    # If both same sign, try wider bounds
    if npv_lo * npv_hi > 0:
        for hi_try in [10.0, 50.0, 100.0]:
            npv_hi = _xnpv(hi_try, cashflows)
            if npv_lo * npv_hi < 0:
                hi = hi_try
                break
        else:
            return None

    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        npv_mid = _xnpv(mid, cashflows)

        if abs(npv_mid) < tol or (hi - lo) / 2.0 < tol:
            return mid

        if npv_lo * npv_mid < 0:
            hi = mid
        else:
            lo = mid
            npv_lo = npv_mid

    return (lo + hi) / 2.0


def _parse_date(d):
    """Convert various date formats to a date object."""
    if isinstance(d, date):
        return d
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, str):
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(d, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Cannot parse date: {d}")


def portfolio_xirr(holdings, transactions=None, as_of_date=None):
    """Compute portfolio XIRR from holdings and optional transactions.

    Strategy:
    1. If transactions exist, use them as actual cashflows (BUY = negative, SELL = positive).
    2. Otherwise, use each holding's invested_value as a single outflow on its purchase_date
       (or a default estimated date).
    3. The current portfolio value is treated as a single positive inflow on as_of_date.

    Args:
        holdings: list of holding dicts with keys: invested_value, current_value, purchase_date (optional)
        transactions: list of transaction dicts with keys: type, quantity, price, date
        as_of_date: date for the terminal value (defaults to today)

    Returns:
        XIRR as float (e.g. 0.12 = 12%) or None
    """
    if as_of_date is None:
        as_of_date = date.today()
    else:
        as_of_date = _parse_date(as_of_date)

    cashflows = []

    # Use holdings cost basis as approximate cashflows for the entire portfolio
    for h in holdings:
        invested = float(h.get('invested_value', 0))
        if invested <= 0:
            continue

        purchase_date = h.get('purchase_date')
        if purchase_date:
            try:
                purchase_date = _parse_date(purchase_date)
            except (ValueError, TypeError):
                purchase_date = None

        if not purchase_date:
            from datetime import timedelta
            purchase_date = as_of_date - timedelta(days=180)

        cashflows.append((purchase_date, -invested))

    if not cashflows:
        return None

    # Terminal value: current portfolio value as inflow today
    total_current = sum(float(h.get('current_value', 0)) for h in holdings)
    if total_current <= 0:
        return None

    cashflows.append((as_of_date, total_current))

    return compute_xirr(cashflows)
