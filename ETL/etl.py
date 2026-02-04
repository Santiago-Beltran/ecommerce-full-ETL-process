import sqlite3
import argparse
import sys
from datetime import datetime, date, timedelta
from typing import Tuple, Dict, List, Any
import os
import re

RUN_ID = None
ERROR_COUNT = 0
WARNING_COUNT = 0
DQ_METRICS = {
    'orphan_user_tx': 0,
    'orphan_product_tx': 0,
    'qty_zero_tx': 0,
    'qty_negative_tx': 0,
    'price_ge_10000_product': 0,
    'price_mismatch_tx': 0,
    'invalid_payment_type_tx': 0,
    'invalid_status_tx': 0,
    'bad_date_format_tx': 0,
    'duplicate_tx_id': 0,
    'invalid_user_records': 0,
    'negative_stock_product': 0
}

def connect_oltp() -> sqlite3.Connection:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, '..'))
    oltp_db_path = os.path.join(repo_root, 'OLTP', 'ecommerce-OLTP.db')
    conn = sqlite3.connect(oltp_db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    return conn

def connect_olap() -> sqlite3.Connection:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, '..'))
    olap_db_path = os.path.join(repo_root, 'OLAP', 'ecommerce-OLAP.db')
    conn = sqlite3.connect(olap_db_path)
    conn.execute('PRAGMA foreign_keys = ON')
    return conn

def ensure_etl_tables(conn_olap: sqlite3.Connection):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    create_db_path = os.path.join(script_dir, 'scripts', 'create_db.py')
    
    tables_exist = conn_olap.execute("""
        SELECT COUNT(*) FROM sqlite_master 
        WHERE type='table' AND name IN ('etl_run_log', 'etl_error_log')
    """).fetchone()[0]
    
    if tables_exist < 2:
        print("  Creating ETL logging tables...")
        import subprocess
        subprocess.run([sys.executable, create_db_path], check=True)

def start_etl_run(conn_olap: sqlite3.Connection, today: date) -> int:
    global RUN_ID
    run_date = datetime.now().strftime('%Y-%m-%d')
    source_date = today.strftime('%Y-%m-%d')
    started_at = datetime.now().isoformat()
    
    cursor = conn_olap.execute("""
        INSERT INTO etl_run_log (
            run_date, source_date, status, started_at,
            rows_dim_user_inserted, rows_dim_product_inserted,
            rows_fact_transactions_inserted, rows_fact_stock_history_inserted,
            errors, warnings
        ) VALUES (?, ?, 'failed', ?, 0, 0, 0, 0, 0, 0)
    """, (run_date, source_date, started_at))
    
    RUN_ID = cursor.lastrowid
    conn_olap.commit()
    return RUN_ID

def log_error(conn_olap: sqlite3.Connection, entity: str, table_name: str, 
              record_id: str, error_type: str, message: str, severity: str = 'error'):
    global ERROR_COUNT, WARNING_COUNT, DQ_METRICS
    
    if severity == 'error':
        ERROR_COUNT += 1
    else:
        WARNING_COUNT += 1
    
    if error_type in DQ_METRICS:
        DQ_METRICS[error_type] += 1
    
    conn_olap.execute("""
        INSERT INTO etl_error_log (
            run_id, entity, table_name, record_id, error_type, 
            message, created_at, severity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (RUN_ID, entity, table_name, record_id, error_type, 
           message, datetime.now().isoformat(), severity))

def finish_etl_run(conn_olap: sqlite3.Connection, success: bool, 
                   counts: Dict[str, int], start_time: datetime):
    global ERROR_COUNT, WARNING_COUNT
    
    end_time = datetime.now()
    duration_ms = int((end_time - start_time).total_seconds() * 1000)
    status = 'success' if success else 'failed'
    
    conn_olap.execute("""
        UPDATE etl_run_log SET
            status = ?, ended_at = ?, duration_ms = ?,
            rows_dim_user_inserted = ?, rows_dim_product_inserted = ?,
            rows_fact_transactions_inserted = ?, rows_fact_stock_history_inserted = ?,
            errors = ?, warnings = ?
        WHERE run_id = ?
    """, (status, end_time.isoformat(), duration_ms,
           counts.get('dim_user_inserted', 0), counts.get('dim_product_inserted', 0),
           counts.get('fact_transactions_inserted', 0), counts.get('fact_stock_history_inserted', 0),
           ERROR_COUNT, WARNING_COUNT, RUN_ID))
    
    conn_olap.commit()

def get_date_id(d: date) -> int:
    return int(d.strftime('%Y%m%d'))

def ensure_dim_date(conn_olap: sqlite3.Connection, d: date):
    date_id = get_date_id(d)
    
    cursor = conn_olap.execute(
        'SELECT 1 FROM dim_date WHERE date_id = ?',
        (date_id,)
    )
    
    if cursor.fetchone() is None:
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

def validate_users(conn_olap: sqlite3.Connection, users: List[Tuple]) -> Tuple[List[Tuple], List[Tuple]]:
    valid_users = []
    rejected_users = []
    
    email_pattern = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]+$')
    
    for user in users:
        user_id, name, email, join_date = user
        
        if not name or name.strip() == '':
            log_error(conn_olap, 'user', 'dim_user', str(user_id), 'invalid_user', 
                     f'Empty name for user {user_id}', 'error')
            rejected_users.append(user)
            continue
        
        if not email or not email_pattern.match(email):
            log_error(conn_olap, 'user', 'dim_user', str(user_id), 'invalid_user',
                     f'Invalid email "{email}" for user {user_id}', 'error')
            rejected_users.append(user)
            continue
        
        if join_date is None:
            log_error(conn_olap, 'user', 'dim_user', str(user_id), 'invalid_user',
                     f'NULL join_date for user {user_id}', 'error')
            rejected_users.append(user)
            continue
        
        valid_users.append(user)
    
    return valid_users, rejected_users

def validate_products(conn_olap: sqlite3.Connection, products: List[Tuple]) -> Tuple[List[Tuple], List[Tuple]]:
    valid_products = []
    rejected_products = []
    
    for product in products:
        product_id, name, category, price, stock = product
        
        if price >= 10000:
            log_error(conn_olap, 'product', 'dim_product', str(product_id), 'price_ge_10000',
                     f'Product {product_id} price {price} >= 10000', 'error')
            rejected_products.append(product)
            continue
        
        if stock < 0:
            log_error(conn_olap, 'product', 'dim_product', str(product_id), 'negative_stock',
                     f'Product {product_id} has negative stock {stock}', 'error')
            rejected_products.append(product)
            continue
        
        valid_products.append(product)
    
    return valid_products, rejected_products

def validate_transactions(conn_olap: sqlite3.Connection, transactions: List[Tuple], 
                         valid_user_ids: set, valid_product_ids: set, 
                         product_prices: Dict[int, float]) -> Tuple[List[Tuple], List[Tuple]]:
    valid_transactions = []
    rejected_transactions = []
    seen_tx_ids = set()
    
    valid_payment_types = {'visa', 'mastercard', 'wire transfer', 'other'}
    valid_statuses = {'success', 'failed'}
    
    for transaction in transactions:
        tx_id, tx_date_str, user_id, product_id, quantity, total_price, payment_type, status = transaction
        
        if user_id not in valid_user_ids:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'orphan_user',
                     f'Transaction {tx_id} references non-existent user {user_id}', 'error')
            rejected_transactions.append(transaction)
            continue
        
        if product_id not in valid_product_ids:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'orphan_product',
                     f'Transaction {tx_id} references non-existent product {product_id}', 'error')
            rejected_transactions.append(transaction)
            continue
        
        if quantity == 0:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'qty_zero',
                     f'Transaction {tx_id} has zero quantity', 'error')
            rejected_transactions.append(transaction)
            continue
        elif quantity < 0:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'qty_negative',
                     f'Transaction {tx_id} has negative quantity {quantity}', 'error')
            rejected_transactions.append(transaction)
            continue
        
        payment_type_normalized = payment_type.lower() if payment_type else ''
        if payment_type_normalized not in valid_payment_types:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'invalid_payment_type',
                     f'Transaction {tx_id} has invalid payment_type "{payment_type}"', 'error')
            rejected_transactions.append(transaction)
            continue
        
        status_normalized = status.lower() if status else ''
        if status_normalized not in valid_statuses:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'invalid_status',
                     f'Transaction {tx_id} has invalid status "{status}"', 'error')
            rejected_transactions.append(transaction)
            continue
        
        try:
            parsed_date = datetime.strptime(tx_date_str, '%Y-%m-%d').date()
        except ValueError:
            try:
                if '/' in tx_date_str:
                    parsed_date = datetime.strptime(tx_date_str, '%Y/%m/%d').date()
                elif 'T' in tx_date_str:
                    parsed_date = datetime.fromisoformat(tx_date_str.split('T')[0]).date()
                elif tx_date_str.isdigit() and len(tx_date_str) == 8:
                    parsed_date = datetime.strptime(tx_date_str, '%Y%m%d').date()
                else:
                    raise ValueError("Unparseable date")
            except ValueError:
                log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'bad_date_format',
                         f'Transaction {tx_id} has unparseable date "{tx_date_str}"', 'error')
                rejected_transactions.append(transaction)
                continue
        
        if product_id in product_prices:
            expected_price = product_prices[product_id]
            if abs(total_price / quantity - expected_price) > 0.01:
                log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'price_mismatch',
                         f'Transaction {tx_id} price mismatch: expected {expected_price}, got {total_price/quantity}', 'warning')
        
        if tx_id in seen_tx_ids:
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'duplicate_tx_id',
                     f'Duplicate transaction_id {tx_id}', 'warning')
        else:
            seen_tx_ids.add(tx_id)
        
        normalized_transaction = (
            tx_id, parsed_date.strftime('%Y-%m-%d'), user_id, product_id, 
            quantity, total_price, payment_type_normalized, status_normalized
        )
        valid_transactions.append(normalized_transaction)
    
    return valid_transactions, rejected_transactions

def fetch_oltp_users(conn_oltp: sqlite3.Connection) -> list:
    return conn_oltp.execute('''
        SELECT user_id, name, email, join_date
        FROM users
        ORDER BY user_id
    ''').fetchall()

def fetch_oltp_products(conn_oltp: sqlite3.Connection) -> list:
    return conn_oltp.execute('''
        SELECT product_id, name, category, price, stock
        FROM products
        ORDER BY product_id
    ''').fetchall()

def fetch_oltp_transactions(conn_oltp: sqlite3.Connection, today: date) -> list:
    today_str = today.strftime('%Y-%m-%d')
    return conn_oltp.execute('''
        SELECT t.transaction_id, t.date, t.user_id, t.product_id,
               t.quantity, t.price, t.payment_type, t.status
        FROM transactions t
        WHERE t.date = ?
        ORDER BY t.transaction_id
    ''', (today_str,)).fetchall()

def upsert_dim_user(conn_olap: sqlite3.Connection, valid_users: list, today: date) -> Dict[str, int]:
    inserted = 0
    updated = 0
    unchanged = 0
    
    for user_id, name, email, join_date in valid_users:
        current = conn_olap.execute('''
            SELECT user_sk, name, email
            FROM dim_user
            WHERE user_id = ? AND current_flag = 1
        ''', (user_id,)).fetchone()
        
        if current is None:
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
            
            if name != old_name or email != old_email:
                end_date = today - timedelta(days=1)
                conn_olap.execute('''
                    UPDATE dim_user
                    SET end_date = ?, current_flag = 0
                    WHERE user_sk = ?
                ''', (end_date.strftime('%Y-%m-%d'), user_sk))
                
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
    
    print(f"  Users: {inserted} inserted, {updated} updated, {unchanged} unchanged")
    return {'inserted': inserted, 'updated': updated, 'unchanged': unchanged}

def upsert_dim_product(conn_olap: sqlite3.Connection, valid_products: list, today: date) -> Dict[str, int]:
    inserted = 0
    updated = 0
    unchanged = 0
    
    for product_id, name, category, price, _ in valid_products:
        current = conn_olap.execute('''
            SELECT product_sk, name, category, price
            FROM dim_product
            WHERE product_id = ? AND current_flag = 1
        ''', (product_id,)).fetchone()
        
        if current is None:
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
            
            if name != old_name or category != old_category or price != old_price:
                end_date = today - timedelta(days=1)
                conn_olap.execute('''
                    UPDATE dim_product
                    SET end_date = ?, current_flag = 0
                    WHERE product_sk = ?
                ''', (end_date.strftime('%Y-%m-%d'), product_sk))
                
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
    
    print(f"  Products: {inserted} inserted, {updated} updated, {unchanged} unchanged")
    return {'inserted': inserted, 'updated': updated, 'unchanged': unchanged}

def load_fact_stock_history(conn_olap: sqlite3.Connection, valid_products: list, today: date) -> Dict[str, int]:
    today_id = get_date_id(today)
    inserted = 0
    skipped = 0
    
    for product_id, _, _, _, stock in valid_products:
        product_sk_row = conn_olap.execute('''
            SELECT product_sk
            FROM dim_product
            WHERE product_id = ? AND current_flag = 1
        ''', (product_id,)).fetchone()
        
        if product_sk_row is None:
            log_error(conn_olap, 'product', 'fact_stock_history', str(product_id), 'orphan_product',
                     f'Product {product_id} not in dim_product, skipping stock history', 'warning')
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
        
        if last_stock_row is None or last_stock_row[0] != stock:
            ensure_dim_date(conn_olap, today)
            
            conn_olap.execute('''
                INSERT INTO fact_stock_history (product_sk, date_id, stock, load_date)
                VALUES (?, ?, ?, ?)
            ''', (product_sk, today_id, stock, today.strftime('%Y-%m-%d')))
            inserted += 1
        else:
            skipped += 1
    
    print(f"  Stock history: {inserted} inserted, {skipped} skipped")
    return {'inserted': inserted, 'skipped': skipped}

def load_fact_transactions(conn_olap: sqlite3.Connection, valid_transactions: list, today: date) -> Dict[str, int]:
    existing_tx_ids = set(
        row[0] for row in conn_olap.execute('SELECT transaction_id FROM fact_transactions').fetchall()
    )
    new_transactions = [tx for tx in valid_transactions if tx[0] not in existing_tx_ids]
    
    if not new_transactions:
        print("  No new transactions to load")
        return {'inserted': 0, 'skipped': 0}
        
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
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'orphan_user',
                     f'User {user_id} not in dim_user for transaction {tx_id}, skipping', 'warning')
            skipped += 1
            continue
        
        user_sk = user_sk_row[0]
        
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
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'orphan_product',
                     f'Product {product_id} not in dim_product for transaction {tx_id}, skipping', 'warning')
            skipped += 1
            continue
        
        product_sk = product_sk_row[0]
        
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
            log_error(conn_olap, 'transaction', 'fact_transactions', str(tx_id), 'duplicate_tx_id',
                     f'Could not insert transaction {tx_id}: {e}', 'warning')
            skipped += 1
    
    print(f"  Transactions: {inserted} inserted, {skipped} skipped")
    return {'inserted': inserted, 'skipped': skipped}

def create_indexes(conn_olap: sqlite3.Connection):
    indexes = [
        ('idx_dim_user_id_flag', 'dim_user', '(user_id, current_flag)'),
        ('idx_dim_user_id_dates', 'dim_user', '(user_id, start_date, end_date)'),
        ('idx_dim_product_id_flag', 'dim_product', '(product_id, current_flag)'),
        ('idx_dim_product_id_dates', 'dim_product', '(product_id, start_date, end_date)'),
        ('idx_fact_tx_id', 'fact_transactions', '(transaction_id)'),
        ('idx_fact_stock_sk_date', 'fact_stock_history', '(product_sk, date_id)'),
        ('idx_dim_date_full', 'dim_date', '(full_date)'),
    ]
    
    for idx_name, table_name, columns in indexes:
        try:
            conn_olap.execute(f'CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} {columns}')
        except sqlite3.Error as e:
            print(f"  Warning: Could not create index {idx_name}: {e}")
    
    print("  Indexes created/verified")

def main(today_str: str):
    start_time = datetime.now()
    success = False
    counts = {}
    
    try:
        today = datetime.strptime(today_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"Error: Invalid date format '{today_str}'. Expected YYYY-MM-DD", file=sys.stderr)
        sys.exit(1)
    
    print(f"Starting ETL process for date: {today}")
    print("=" * 60)
    
    try:
        conn_oltp = connect_oltp()
        conn_olap = connect_olap()
        
        ensure_etl_tables(conn_olap)
        start_etl_run(conn_olap, today)
        
    except sqlite3.Error as e:
        print(f"Error: Could not connect to databases: {e}", file=sys.stderr)
        sys.exit(1)
    
    try:
        create_indexes(conn_olap)
        print()
        
        ensure_dim_date(conn_olap, today)
        conn_olap.commit()
        print()
        
        print("Fetching data from OLTP...")
        oltp_users = fetch_oltp_users(conn_oltp)
        oltp_products = fetch_oltp_products(conn_oltp)
        oltp_transactions = fetch_oltp_transactions(conn_oltp, today)
        print(f"  Fetched {len(oltp_users)} users, {len(oltp_products)} products, {len(oltp_transactions)} transactions")
        print()
        
        print("Validating data quality...")
        
        valid_users, rejected_users = validate_users(conn_olap, oltp_users)
        print(f"  Users: {len(valid_users)} valid, {len(rejected_users)} rejected")
        
        valid_products, rejected_products = validate_products(conn_olap, oltp_products)
        print(f"  Products: {len(valid_products)} valid, {len(rejected_products)} rejected")
        
        valid_user_ids = {user[0] for user in valid_users}
        valid_product_ids = {product[0] for product in valid_products}
        product_prices = {product[0]: product[3] for product in valid_products}
        
        valid_transactions, rejected_transactions = validate_transactions(
            conn_olap, oltp_transactions, valid_user_ids, valid_product_ids, product_prices)
        print(f"  Transactions: {len(valid_transactions)} valid, {len(rejected_transactions)} rejected")
        print()
        
        try:
            user_results = upsert_dim_user(conn_olap, valid_users, today)
            counts['dim_user_inserted'] = user_results['inserted']
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in dim_user: {e}", file=sys.stderr)
            raise
        print()
        
        try:
            product_results = upsert_dim_product(conn_olap, valid_products, today)
            counts['dim_product_inserted'] = product_results['inserted']
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in dim_product: {e}", file=sys.stderr)
            raise
        print()
        
        try:
            stock_results = load_fact_stock_history(conn_olap, valid_products, today)
            counts['fact_stock_history_inserted'] = stock_results['inserted']
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in fact_stock_history: {e}", file=sys.stderr)
            raise
        print()
        
        try:
            tx_results = load_fact_transactions(conn_olap, valid_transactions, today)
            counts['fact_transactions_inserted'] = tx_results['inserted']
            conn_olap.commit()
        except Exception as e:
            conn_olap.rollback()
            print(f"Error in fact_transactions: {e}", file=sys.stderr)
            raise
        print()
        
        print("=" * 60)
        print("ETL Summary:")
        
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
        
        current_users = conn_olap.execute('SELECT COUNT(*) FROM dim_user WHERE current_flag = 1').fetchone()[0]
        current_products = conn_olap.execute('SELECT COUNT(*) FROM dim_product WHERE current_flag = 1').fetchone()[0]
        
        print(f"  Current users: {current_users}")
        print(f"  Current products: {current_products}")
        
        print(f"\n  Data Quality Summary:")
        print(f"    Errors: {ERROR_COUNT}")
        print(f"    Warnings: {WARNING_COUNT}")
        for metric_name, count in DQ_METRICS.items():
            if count > 0:
                print(f"    {metric_name}: {count}")
        
        print("=" * 60)
        print(f"ETL completed successfully for {today}")
        success = True
        
    except Exception as e:
        print(f"\nETL failed: {e}", file=sys.stderr)
        success = False
        raise
    finally:
        if RUN_ID:
            finish_etl_run(conn_olap, success, counts, start_time)
        conn_oltp.close()
        conn_olap.close()
        
        if not success:
            sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ETL script for OLTP to OLAP synchronization')
    parser.add_argument('--today', required=True, help='ETL execution date in YYYY-MM-DD format')
    args = parser.parse_args()
    main(args.today)
