import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'portfolio.db')


def get_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS portfolios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS holdings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            isin TEXT,
            symbol TEXT,
            name TEXT,
            quantity REAL DEFAULT 0,
            avg_price REAL DEFAULT 0,
            invested_value REAL DEFAULT 0,
            sector TEXT,
            purchase_date TEXT,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            total_value REAL,
            total_invested REAL,
            total_pnl REAL,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS symbol_map (
            isin TEXT PRIMARY KEY,
            name TEXT,
            symbol TEXT,
            sector TEXT
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL UNIQUE,
            name TEXT,
            target_price REAL,
            notes TEXT,
            added_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            symbol TEXT,
            name TEXT,
            type TEXT CHECK(type IN ('BUY', 'SELL')),
            quantity REAL,
            price REAL,
            date TEXT,
            notes TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (portfolio_id) REFERENCES portfolios(id) ON DELETE CASCADE
        );
    ''')
    conn.commit()

    # Add purchase_date column to legacy databases that don't have it
    try:
        conn.execute("ALTER TABLE holdings ADD COLUMN purchase_date TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists

    conn.close()


# ── Portfolio CRUD ──

def create_portfolio(name):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("INSERT INTO portfolios (name) VALUES (?)", (name,))
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    return pid


def get_portfolios():
    conn = get_db()
    rows = conn.execute("SELECT * FROM portfolios ORDER BY updated_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_portfolio(pid):
    conn = get_db()
    row = conn.execute("SELECT * FROM portfolios WHERE id = ?", (pid,)).fetchone()
    conn.close()
    return dict(row) if row else None


def rename_portfolio(pid, name):
    conn = get_db()
    conn.execute("UPDATE portfolios SET name = ?, updated_at = datetime('now') WHERE id = ?", (name, pid))
    conn.commit()
    conn.close()


def delete_portfolio(pid):
    conn = get_db()
    conn.execute("DELETE FROM portfolios WHERE id = ?", (pid,))
    conn.commit()
    conn.close()


# ── Holdings ──

def save_holdings(portfolio_id, holdings_list):
    conn = get_db()
    conn.execute("DELETE FROM holdings WHERE portfolio_id = ?", (portfolio_id,))
    for h in holdings_list:
        conn.execute(
            "INSERT INTO holdings (portfolio_id, isin, symbol, name, quantity, avg_price, invested_value, sector, purchase_date) VALUES (?,?,?,?,?,?,?,?,?)",
            (portfolio_id, h.get('isin', ''), h.get('symbol', ''), h.get('name', ''),
             h.get('quantity', 0), h.get('avg_price', 0), h.get('invested_value', 0), h.get('sector', ''), h.get('purchase_date'))
        )
    conn.execute("UPDATE portfolios SET updated_at = datetime('now') WHERE id = ?", (portfolio_id,))
    conn.commit()
    conn.close()


def get_holdings(portfolio_id):
    conn = get_db()
    rows = conn.execute("SELECT * FROM holdings WHERE portfolio_id = ?", (portfolio_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Snapshots ──

def save_snapshot(portfolio_id, total_value, total_invested, total_pnl):
    conn = get_db()
    today = datetime.now().strftime('%Y-%m-%d')
    existing = conn.execute("SELECT id FROM snapshots WHERE portfolio_id = ? AND date = ?", (portfolio_id, today)).fetchone()
    if existing:
        conn.execute("UPDATE snapshots SET total_value=?, total_invested=?, total_pnl=? WHERE id=?",
                      (total_value, total_invested, total_pnl, existing['id']))
    else:
        conn.execute("INSERT INTO snapshots (portfolio_id, date, total_value, total_invested, total_pnl) VALUES (?,?,?,?,?)",
                      (portfolio_id, today, total_value, total_invested, total_pnl))
    conn.commit()
    conn.close()


def get_snapshots(portfolio_id, limit=365):
    conn = get_db()
    rows = conn.execute("SELECT * FROM snapshots WHERE portfolio_id = ? ORDER BY date ASC LIMIT ?", (portfolio_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Symbol Map ──

def save_symbol_map_bulk(mappings):
    conn = get_db()
    for m in mappings:
        conn.execute("INSERT OR REPLACE INTO symbol_map (isin, name, symbol, sector) VALUES (?,?,?,?)",
                      (m.get('isin', ''), m.get('name', ''), m.get('symbol', ''), m.get('sector', '')))
    conn.commit()
    conn.close()


def save_symbol_mapping(isin, name, symbol, sector=None):
    conn = get_db()
    conn.execute("INSERT OR REPLACE INTO symbol_map (isin, name, symbol, sector) VALUES (?,?,?,?)",
                  (isin, name, symbol, sector or ''))
    conn.commit()
    conn.close()


def get_symbol_map():
    conn = get_db()
    rows = conn.execute("SELECT * FROM symbol_map").fetchall()
    conn.close()
    return {r['isin']: dict(r) for r in rows}


# ── Watchlist ──

def add_to_watchlist(symbol, name=None, target_price=None, notes=None):
    conn = get_db()
    try:
        conn.execute("INSERT INTO watchlist (symbol, name, target_price, notes) VALUES (?,?,?,?)",
                      (symbol, name, target_price, notes))
    except sqlite3.IntegrityError:
        conn.execute("UPDATE watchlist SET name=?, target_price=?, notes=? WHERE symbol=?",
                      (name, target_price, notes, symbol))
    conn.commit()
    conn.close()


def get_watchlist():
    conn = get_db()
    rows = conn.execute("SELECT * FROM watchlist ORDER BY added_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def remove_from_watchlist(symbol):
    conn = get_db()
    conn.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
    conn.commit()
    conn.close()


# ── Transactions ──

def add_transaction(portfolio_id, symbol, name, txn_type, quantity, price, date, notes=None):
    conn = get_db()
    conn.execute(
        "INSERT INTO transactions (portfolio_id, symbol, name, type, quantity, price, date, notes) VALUES (?,?,?,?,?,?,?,?)",
        (portfolio_id, symbol, name, txn_type, quantity, price, date, notes)
    )
    conn.commit()
    conn.close()


def get_transactions(portfolio_id, limit=100):
    conn = get_db()
    rows = conn.execute("SELECT * FROM transactions WHERE portfolio_id = ? ORDER BY date DESC LIMIT ?", (portfolio_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]
