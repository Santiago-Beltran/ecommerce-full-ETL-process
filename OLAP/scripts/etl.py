"""
ETL Script for OLTP -> OLAP Incremental Synchronization
Handles SCD2 for dim_user and dim_product, SCD0 for dim_date,
and incremental loads for fact_stock_history and fact_transactions.
"""

import sqlite3
import argparse
import sys
from datetime import datetime, date, timedelta
from typing import Tuple


# =============================================================================
# Database Connection Functions
# =============================================================================

def connect_oltp() -> sqlite3.Connection:
    """Connect to OLTP database."""
    conn = sqlite3.connect('../OLTP/ecommerce-OLTP.db')
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


def connect_olap() -> sqlite3.Connection:
    """Connect to OLAP database."""
    conn = sqlite3.connect('./ecommerce-OLAP.db')
    conn.execute('PRAGMA foreign_keys = ON')
    return conn


# =============================================================================
# OLTP Data Fetch Helper Functions (Read-Only)
# =============================================================================

def fetch_oltp_users(conn_oltp: sqlite3.Connection) -> list:
    """Fetch all users from OLTP database."""
    return conn_oltp.execute('''
        SELECT user_id, name, email, join_date
        FROM users
        ORDER BY user_id
    ''').fetchall()


def fetch_oltp_products(conn_oltp: sqlite3.Connection) -> list:
    """Fetch all products from OLTP database."""
    return conn_oltp.execute('''
        SELECT product_id, name, category, price, stock
        FROM products
        ORDER BY product_id
    ''').fetchall()


def fetch_oltp_transactions(conn_oltp: sqlite3.Connection, today: date) -> list:
    """Fetch transactions for the given date from OLTP."""
    today_str = today.strftime('%Y-%m-%d')
    return conn_oltp.execute('''
        SELECT t.transaction_id, t.date, t.user_id, t.product_id,
               t.quantity, t.price, t.payment_type, t.status
        FROM transactions t
        WHERE t.date = ?
        ORDER BY t.transaction_id
    ''', (today_str,)).fetchall()


# =============================================================================
# Date Utility Functions
# =============================================================================

def get_date_id(d: date) -> int:
    """Convert date to integer format YYYYMMDD."""
    return int(d.strftime('%Y%m%d'))


def ensure_dim_date(conn_olap: sqlite3.Connection, d: date):
    """Insert date dimension row if it doesn't exist (SCD0)."""
    date_id = get_date_id(d)
    
    # Check if date already exists
    cursor = conn_olap.execute(
        'SELECT 1 FROM dim_date WHERE date_id = ?',
        (date_id,)
    )
    
    if cursor.fetchone() is None:
        # ISO week and weekday
        _, iso_week, iso_weekday = d.isocalendar()
        
        conn_olap.execute('''
            INSERT INTO dim_date (date_id, full_date, year, month, day, week, weekday)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            date_id,
            d.strftime('%Y-%m-%d'),
            d.year,
            d.month,
            d.day,
            iso_week,
            iso_weekday
        ))


# =============================================================================
# Dimension Loading Functions (SCD2)
# =============================================================================

def upsert_dim_user(conn_olap: sqlite3.Connection, oltp_users: list, today: date):
    """
    Synchronize dim_user using SCD2.
    Tracks changes in name, email.
    """
    
    inserted = 0
    updated = 0
    unchanged = 0
    
    for user_id, name, email, join_date in oltp_users:
        # Find current dimension row
        current = conn_olap.execute('''
            SELECT user_sk, name, email
            FROM dim_user
            WHERE user_id = ? AND current_flag = 1
        ''', (user_id,)).fetchone()
        
        if current is None:
            # New user - insert initial version
            conn_olap.execute('''
                INSERT INTO dim_user (
                    user_id, name, email, join_date,
                    start_date, end_date, current_flag
                )
                VALUES (?, ?, ?, ?, ?, NULL, 1)
            ''', (user_id, name, email, join_date, join_date))
            inserted += 1
            
        else:
            user_sk, old_name, old_email = current
            
            # Check if tracked attributes have changed
            if name != old_name or email != old_email:
                # Close current version
                end_date = today - timedelta(days=1)
                conn_olap.execute('''
                    UPDATE dim_user
                    SET end_date = ?, current_flag = 0
                    WHERE user_sk = ?
                ''', (end_date.strftime('%Y-%m-%d'), user_sk))
                
                # Insert new version
                conn_olap.execute('''
                    INSERT INTO dim_user (
                        user_id, name, email, join_date,
                        start_date, end_date, current_flag
                    )
                    VALUES (?, ?, ?, ?, ?, NULL, 1)
                ''', (user_id, name, email, join_date, today.strftime('%Y-%m-%d')))
                updated += 1

            else:
                unchanged += 1
    

def upsert_dim_product(conn_olap: sqlite3.Connection, oltp_products: list, today: date):
    """
    Synchronize dim_product using SCD2.
    Tracks changes in name, category, price (NOT stock).
    Stock changes are recorded in fact_stock_history.
    """
    
    inserted = 0
    updated = 0
    unchanged = 0
    
    for product_id, name, category, price, _ in oltp_products:
        # Find current dimension row
        current = conn_olap.execute('''
            SELECT product_sk, name, category, price
            FROM dim_product
            WHERE product_id = ? AND current_flag = 1
        ''', (product_id,)).fetchone()
        
        if current is None:
            # New product - insert initial version
            conn_olap.execute('''
                INSERT INTO dim_product (
                    product_id, name, category, price,
                    start_date, end_date, current_flag
                )
                VALUES (?, ?, ?, ?, ?, NULL, 1)
            ''', (product_id, name, category, price, today.strftime('%Y-%m-%d')))
            inserted += 1
            
        else:
            product_sk, old_name, old_category, old_price = current
            
            # Check if tracked attributes have changed (ignore stock)
            if name != old_name or category != old_category or price != old_price:
                # Close current version
                end_date = today - timedelta(days=1)
                conn_olap.execute('''
                    UPDATE dim_product
                    SET end_date = ?, current_flag = 0
                    WHERE product_sk = ?
                ''', (end_date.strftime('%Y-%m-%d'), product_sk))
                
                # Insert new version
                conn_olap.execute('''
                    INSERT INTO dim_product (
                        product_id, name, category, price,
                        start_date, end_date, current_flag
                    )
                    VALUES (?, ?, ?, ?, ?, NULL, 1)
                ''', (product_id, name, category, price, today.strftime('%Y-%m-%d')))
                updated += 1
            else:
                unchanged += 1
    

# =============================================================================
# Fact Loading Functions
# =============================================================================

def load_fact_stock_history(conn_olap: sqlite3.Connection, oltp_products: list, today: date):
    """
    Load stock changes into fact_stock_history.
    Only inserts if stock has changed from last recorded value.
    """
    
    today_id = get_date_id(today)
    inserted = 0
    skipped = 0
    
    for product_id, _, _, _, stock in oltp_products:
        current_stock = stock
        
        product_sk_row = conn_olap.execute('''
            SELECT product_sk
            FROM dim_product
            WHERE product_id = ? AND current_flag = 1
        ''', (product_id,)).fetchone()
        
        if product_sk_row is None:
            print(f"  Warning: Product {product_id} not in dim_product, skipping stock history")
            skipped += 1
            continue
        
        product_sk = product_sk_row[0]
        
        last_stock_row = conn_olap.execute('''
            SELECT stock
            FROM fact_stock_history
            WHERE product_sk = ?
            ORDER BY date_id DESC
            LIMIT 1
        ''', (product_sk,)).fetchone()
        
        if last_stock_row is None or last_stock_row[0] != current_stock:
            ensure_dim_date(conn_olap, today)
            
            conn_olap.execute('''
                INSERT INTO fact_stock_history (product_sk, date_id, stock, load_date)
                VALUES (?, ?, ?, ?)
            ''', (product_sk, today_id, current_stock, today.strftime('%Y-%m-%d')))
            inserted += 1
        else:
            skipped += 1


def load_fact_transactions(conn_olap: sqlite3.Connection, oltp_transactions: list, oltp_users: list, oltp_products: list, today: date):
    """
    Incrementally load transactions from OLTP to OLAP.
    Uses effective-dated lookup for user_sk and product_sk.
    """
    
    # Note: I know this is highly inefficient, but it's a simple implementation for demonstration.
    # In an e-commerce setting, an event-based approach would be better.
    existing_tx_ids = set(
        row[0] for row in conn_olap.execute('SELECT transaction_id FROM fact_transactions').fetchall()
    )
    new_transactions = [tx for tx in oltp_transactions if tx[0] not in existing_tx_ids]
    
    if not new_transactions:
        print("  No new transactions to load")
        return
        
    inserted = 0
    skipped = 0
    
    for tx_id, tx_date_str, user_id, product_id, quantity, total_price, payment_type, status in new_transactions:
        tx_date = datetime.strptime(tx_date_str, '%Y-%m-%d').date()
        
        ensure_dim_date(conn_olap, tx_date)
        date_id = get_date_id(tx_date)
        
        user_sk_row = conn_olap.execute('''
            SELECT user_sk
            FROM dim_user
            WHERE user_id = ?
              AND start_date <= ?
              AND (end_date IS NULL OR end_date >= ?)
            ORDER BY start_date DESC
            LIMIT 1
        ''', (user_id, tx_date_str, tx_date_str)).fetchone()

        if user_sk_row is None:
            print(f"  Warning: User {user_id} not in dim_user for transaction {tx_id}, skipping")
            skipped += 1
            continue
        
        user_sk = user_sk_row[0]
        
        # Resolve product_sk with effective dating
        product_sk_row = conn_olap.execute('''
            SELECT product_sk
            FROM dim_product
            WHERE product_id = ?
              AND start_date <= ?
              AND (end_date IS NULL OR end_date >= ?)
            ORDER BY start_date DESC
            LIMIT 1
        ''', (product_id, tx_date_str, tx_date_str)).fetchone()
        
        if product_sk_row is None:
            print(f"  Warning: Product {product_id} not in dim_product for transaction {tx_id}, skipping")
            skipped += 1
            continue
        
        product_sk = product_sk_row[0]
        
        # Insert into fact_transactions
        try:
            conn_olap.execute('''
                INSERT INTO fact_transactions (
                    transaction_id, user_sk, product_sk, date_id,
                    quantity, total, payment_type, status, load_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tx_id, user_sk, product_sk, date_id,
                quantity, total_price, payment_type, status,
                today.strftime('%Y-%m-%d')
            ))
            inserted += 1
        except sqlite3.IntegrityError as e:
            print(f"  Warning: Could not insert transaction {tx_id}: {e}")
            skipped += 1
    

# =============================================================================
# Index Creation
# =============================================================================

def create_indexes(conn_olap: sqlite3.Connection):
    """Create performance indexes if they don't exist."""
    
    indexes = [
        # dim_user indexes
        ('idx_dim_user_id_flag', 'dim_user', '(user_id, current_flag)'),
        ('idx_dim_user_id_dates', 'dim_user', '(user_id, start_date, end_date)'),
        
        # dim_product indexes
        ('idx_dim_product_id_flag', 'dim_product', '(product_id, current_flag)'),
        ('idx_dim_product_id_dates', 'dim_product', '(product_id, start_date, end_date)'),
        
        # fact_transactions indexes
        ('idx_fact_tx_id', 'fact_transactions', '(transaction_id)'),
        
        # fact_stock_history indexes
        ('idx_fact_stock_sk_date', 'fact_stock_history', '(product_sk, date_id)'),
        
        # dim_date indexes
        ('idx_dim_date_full', 'dim_date', '(full_date)'),
    ]
    
    for idx_name, table_name, columns in indexes:
        try:
            conn_olap.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} {columns}')
        except sqlite3.Error as e:
            print(f"  Warning: Could not create index {idx_name}: {e}")
    
    print("  Indexes created/verified")


# =============================================================================
# Main ETL Orchestration
# =============================================================================

def main(today_str: str):
    """
    Main ETL process.
    
    Args:
        today_str: Date string in YYYY-MM-DD format
    """
    # Parse and validate today date
    try:
        today = datetime.strptime(today_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"Error: Invalid date format '{today_str}'. Expected YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting ETL process for date: {today}")
    print("=" * 60)
    
    # Connect to databases
    try:
        conn_oltp = connect_oltp()
        conn_olap = connect_olap()
    except sqlite3.Error as e:
        print(f"Error: Could not connect to databases: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Create indexes first
        create_indexes(conn_olap)
        print()
        
        # Ensure today exists in dim_date
        ensure_dim_date(conn_olap, today)
        conn_olap.commit()
        print()
        
        # Fetch all data from OLTP (read-only operations)
        print("Fetching data from OLTP...")
        oltp_users = fetch_oltp_users(conn_oltp)
        oltp_products = fetch_oltp_products(conn_oltp)
        oltp_transactions = fetch_oltp_transactions(conn_oltp, today)
        print(f"  Fetched {len(oltp_users)} users, {len(oltp_products)} products, {len(oltp_transactions)} transactions")
        print()
        
        # Process dimensions (SCD2)
        try:
            upsert_dim_user(conn_olap, oltp_users, today)
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in dim_user: {e}", file=sys.stderr)
            raise
        print()
        
        try:
            upsert_dim_product(conn_olap, oltp_products, today)
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in dim_product: {e}", file=sys.stderr)
            raise
        print()
        
        # Process facts
        try:
            load_fact_stock_history(conn_olap, oltp_products, today)
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in fact_stock_history: {e}", file=sys.stderr)
            raise
        print()
        
        try:
            load_fact_transactions(conn_olap, oltp_transactions, oltp_users, oltp_products, today)
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in fact_transactions: {e}", file=sys.stderr)
            raise
        print()
        
        # Print summary
        print("=" * 60)
        print("ETL Summary:")
        
        # Count records in each table
        dim_user_count = conn_olap.execute('SELECT COUNT(*) FROM dim_user').fetchone()[0]
        dim_product_count = conn_olap.execute('SELECT COUNT(*) FROM dim_product').fetchone()[0]
        dim_date_count = conn_olap.execute('SELECT COUNT(*) FROM dim_date').fetchone()[0]
        fact_tx_count = conn_olap.execute('SELECT COUNT(*) FROM fact_transactions').fetchone()[0]
        fact_stock_count = conn_olap.execute('SELECT COUNT(*) FROM fact_stock_history').fetchone()[0]
        
        print(f"  dim_user total rows: {dim_user_count}")
        print(f"  dim_product total rows: {dim_product_count}")
        print(f"  dim_date total rows: {dim_date_count}")
        print(f"  fact_transactions total rows: {fact_tx_count}")
        print(f"  fact_stock_history total rows: {fact_stock_count}")
        
        # Count current versions
        current_users = conn_olap.execute('SELECT COUNT(*) FROM dim_user WHERE current_flag = 1').fetchone()[0]
        current_products = conn_olap.execute('SELECT COUNT(*) FROM dim_product WHERE current_flag = 1').fetchone()[0]
        
        print(f"  Current users: {current_users}")
        print(f"  Current products: {current_products}")
        
        print("=" * 60)
        print(f"ETL completed successfully for {today}")
        
    except Exception as e:
        print(f"\nETL failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn_oltp.close()
        conn_olap.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='ETL script for OLTP to OLAP synchronization'
    )
    parser.add_argument(
        '--today',
        required=True,
        help='ETL execution date in YYYY-MM-DD format'
    )
    
    args = parser.parse_args()
    main(args.today)
