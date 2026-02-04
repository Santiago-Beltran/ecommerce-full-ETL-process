"""
Invalid Data Generator for OLTP Testing
Generates specific data quality issues based on oltp_data_quality_issues.txt
"""

import random
import string
import sqlite3
import os
from datetime import datetime, date, timedelta
from typing import Tuple, List, Dict, Any, Optional


# =============================================================================
# Configuration & Constants
# =============================================================================

VALID_PAYMENT_TYPES = ['visa', 'mastercard', 'wire transfer', 'other']
VALID_STATUSES = ['success', 'failed']
VALID_CATEGORIES = ['Footwear', 'Clothing', 'Accessories', 'Electronics', 'Misc']

# Valid reference data (will be populated from database)
VALID_USER_IDS = []
VALID_PRODUCT_IDS = []
CURRENT_DATE = '2026-02-01'  # Will be set by generate_invalid_records
MAX_USER_ID = 0
MAX_PRODUCT_ID = 0


# =============================================================================
# Database Query Functions
# =============================================================================

def get_oltp_db_path() -> str:
    """Get the path to the OLTP database."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, '..', 'ecommerce-OLTP.db')


def fetch_valid_ids() -> Tuple[List[int], List[int]]:
    """
    Query OLTP database to get existing user_ids and product_ids.
    
    Returns:
        Tuple of (user_ids, product_ids)
        
    Raises:
        FileNotFoundError: If OLTP database does not exist
        ValueError: If users or products tables are empty
        sqlite3.Error: If database query fails
    """
    global MAX_USER_ID, MAX_PRODUCT_ID
    
    db_path = get_oltp_db_path()
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"OLTP database not found at {db_path}. "
            f"Please create the database first using create_db.py"
        )
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Fetch user IDs
        cursor.execute("SELECT user_id FROM users ORDER BY user_id")
        user_ids = [row[0] for row in cursor.fetchall()]
        
        # Fetch product IDs
        cursor.execute("SELECT product_id FROM products ORDER BY product_id")
        product_ids = [row[0] for row in cursor.fetchall()]
        
        # Get max IDs for generating new ones
        if user_ids:
            MAX_USER_ID = max(user_ids)
        else:
            MAX_USER_ID = 0
            
        if product_ids:
            MAX_PRODUCT_ID = max(product_ids)
        else:
            MAX_PRODUCT_ID = 0
        
        if not user_ids:
            raise ValueError(
                "No users found in database. "
                "Please populate the database first using populate.py"
            )
        
        if not product_ids:
            raise ValueError(
                "No products found in database. "
                "Please populate the database first using populate.py"
            )
        
        return (user_ids, product_ids)
        
    finally:
        conn.close()


# =============================================================================
# Error Type 1: Orphan Transactions (missing user_id/product_id)
# =============================================================================

def generate_orphan_user_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with non-existent user_id."""
    # Find a user_id that doesn't exist
    if not VALID_USER_IDS:
        raise ValueError("No valid user IDs available")
    
    max_id = max(VALID_USER_IDS)
    orphan_user_id = max_id + random.randint(1, 100)
    
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': orphan_user_id,
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': random.choice(VALID_STATUSES)
    })


def generate_orphan_product_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with non-existent product_id."""
    # Find a product_id that doesn't exist
    if not VALID_PRODUCT_IDS:
        raise ValueError("No valid product IDs available")
    
    max_id = max(VALID_PRODUCT_IDS)
    orphan_product_id = max_id + random.randint(1, 100)
    
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': orphan_product_id,
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': random.choice(VALID_STATUSES)
    })


# =============================================================================
# Error Type 2: Quantity <= 0 (including returns)
# =============================================================================

def generate_zero_quantity_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with quantity = 0."""
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': 0,
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': 'failed'
    })


def generate_negative_quantity_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with negative quantity (return)."""
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(-5, -1),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': 'success'
    })


# =============================================================================
# Error Type 3: Price Violations (>= 10000)
# =============================================================================

def generate_excessive_price_product() -> Tuple[str, Dict[str, Any]]:
    """Generate product with price >= 10000."""
    global MAX_PRODUCT_ID
    MAX_PRODUCT_ID += 1
    return ('product', {
        'product_id': MAX_PRODUCT_ID,
        'name': f'Luxury Item {random.randint(1, 100)}',
        'category': 'Accessories',
        'price': round(random.uniform(10000, 50000), 2),
        'stock': random.randint(1, 10)
    })


# =============================================================================
# Error Type 4: Price Mismatch vs Product Price (discounts/drift)
# =============================================================================

def generate_price_mismatch_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with price different from product price."""
    if not VALID_PRODUCT_IDS:
        raise ValueError("No valid product IDs available")
    
    # Query actual product price from database
    product_id = random.choice(VALID_PRODUCT_IDS)
    db_path = get_oltp_db_path()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT price FROM products WHERE product_id = ?", (product_id,))
        result = cursor.fetchone()
        
        if result is None:
            raise ValueError(f"Product {product_id} not found in database")
        
        actual_price = result[0]
        
        # Generate a different price (ensure mismatch)
        # Apply a random discount/markup between 10-50%
        if random.choice([True, False]):
            # Discount
            mismatch_price = round(actual_price * random.uniform(0.5, 0.9), 2)
        else:
            # Markup
            mismatch_price = round(actual_price * random.uniform(1.1, 1.5), 2)
        
        # Ensure they're actually different
        while mismatch_price == actual_price:
            mismatch_price = round(actual_price + random.uniform(-10, 10), 2)
        
    finally:
        conn.close()
    
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': product_id,
        'quantity': random.randint(1, 5),
        'price': mismatch_price,
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': 'success'
    })


# =============================================================================
# Error Type 5: Invalid payment_type
# =============================================================================

def generate_invalid_payment_type_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with invalid payment_type."""
    invalid_types = ['Bitcoin', 'PayPal', 'bank_transfer', 'crypto', 'cash', 'check']
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(invalid_types),
        'status': random.choice(VALID_STATUSES)
    })


# =============================================================================
# Error Type 6: Invalid status
# =============================================================================

def generate_invalid_status_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with invalid status."""
    invalid_statuses = ['pending', 'processing', 'cancelled', 'refunded', 'unknown']
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': random.choice(invalid_statuses)
    })


# =============================================================================
# Error Type 7: Bad Date Formats
# =============================================================================

def generate_bad_date_format_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate transaction with bad date format."""
    # Convert CURRENT_DATE to various bad formats
    from datetime import datetime
    
    try:
        dt = datetime.strptime(CURRENT_DATE, '%Y-%m-%d')
    except:
        dt = datetime(2026, 2, 1)  # Fallback
    
    bad_formats = [
        dt.strftime('%Y/%m/%d'),
        dt.strftime('%d-%m-%Y'),
        dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        dt.strftime('%Y%m%d'),
        dt.strftime('%b %d, %Y')
    ]
    return ('transaction', {
        'transaction_id': random.randint(2000, 9999),
        'date': random.choice(bad_formats),
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': random.choice(VALID_STATUSES)
    })


# =============================================================================
# Error Type 9: Duplicate transaction_id
# =============================================================================

def generate_duplicate_transaction() -> Tuple[str, Dict[str, Any]]:
    """Generate duplicate transaction with same transaction_id as an existing one."""
    db_path = get_oltp_db_path()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Get a random existing transaction_id
        cursor.execute("SELECT transaction_id FROM transactions ORDER BY RANDOM() LIMIT 1")
        result = cursor.fetchone()
        
        if result is None:
            # No transactions exist, use a low ID that might exist soon
            existing_id = 1
        else:
            existing_id = result[0]
        
    finally:
        conn.close()
    
    return ('transaction', {
        'transaction_id': existing_id,
        'date': CURRENT_DATE,
        'user_id': random.choice(VALID_USER_IDS),
        'product_id': random.choice(VALID_PRODUCT_IDS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10, 100), 2),
        'payment_type': random.choice(VALID_PAYMENT_TYPES),
        'status': random.choice(VALID_STATUSES)
    })



# =============================================================================
# Error Type 11: Invalid Emails, Empty Names, NULL join_date
# =============================================================================

def generate_empty_name_user() -> Tuple[str, Dict[str, Any]]:
    """Generate user with empty name."""
    global MAX_USER_ID
    MAX_USER_ID += 1
    return ('user', {
        'user_id': MAX_USER_ID,
        'name': '',
        'email': f'user{MAX_USER_ID}@example.com',
        'join_date': '2025-01-15'
    })


def generate_invalid_email_user() -> Tuple[str, Dict[str, Any]]:
    """Generate user with invalid email format."""
    global MAX_USER_ID
    MAX_USER_ID += 1
    invalid_emails = ['bademail', 'user@', '@example.com', 'user.example.com', 'user @email.com']
    return ('user', {
        'user_id': MAX_USER_ID,
        'name': f'User {random.randint(1, 100)}',
        'email': random.choice(invalid_emails),
        'join_date': '2025-01-15'
    })


def generate_null_joindate_user() -> Tuple[str, Dict[str, Any]]:
    """Generate user with NULL join_date."""
    global MAX_USER_ID
    MAX_USER_ID += 1
    return ('user', {
        'user_id': MAX_USER_ID,
        'name': f'User {random.randint(1, 100)}',
        'email': f'user{MAX_USER_ID}@example.com',
        'join_date': None
    })


# =============================================================================
# Error Type 12: Negative Stock 
# =============================================================================

def generate_negative_stock_product() -> Tuple[str, Dict[str, Any]]:
    """Generate product with negative stock."""
    global MAX_PRODUCT_ID
    MAX_PRODUCT_ID += 1
    return ('product', {
        'product_id': MAX_PRODUCT_ID,
        'name': f'Product {random.randint(1, 100)}',
        'category': random.choice(VALID_CATEGORIES),
        'price': round(random.uniform(10, 100), 2),
        'stock': random.randint(-50, -1)
    })


# =============================================================================
# Main Generator Function
# =============================================================================

# Registry of all error generator functions
ERROR_GENERATORS = [
    generate_orphan_user_transaction,
    generate_orphan_product_transaction,
    generate_zero_quantity_transaction,
    generate_negative_quantity_transaction,
    generate_excessive_price_product,
    generate_price_mismatch_transaction,
    generate_invalid_payment_type_transaction,
    generate_invalid_status_transaction,
    generate_bad_date_format_transaction,
    generate_duplicate_transaction,
    generate_empty_name_user,
    generate_invalid_email_user,
    generate_null_joindate_user,
    generate_negative_stock_product,
]


def generate_invalid_records(count: int, today: str, user_ids: Optional[List[int]] = None, product_ids: Optional[List[int]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Generate a specified number of invalid records with random error types.
    
    Args:
        count: Number of invalid records to generate
        today: Date string in YYYY-MM-DD format to use for transactions
        user_ids: List of valid user IDs (fetched from DB if None)
        product_ids: List of valid product IDs (fetched from DB if None)
        
    Returns:
        Dictionary with keys 'users', 'products', 'transactions' containing lists of invalid records
    """
    global VALID_USER_IDS, VALID_PRODUCT_IDS, CURRENT_DATE
    
    # Set current date
    CURRENT_DATE = today
    
    # Fetch valid IDs from database if not provided
    if user_ids is None or product_ids is None:
        fetched_user_ids, fetched_product_ids = fetch_valid_ids()
        VALID_USER_IDS = user_ids if user_ids is not None else fetched_user_ids
        VALID_PRODUCT_IDS = product_ids if product_ids is not None else fetched_product_ids
    else:
        VALID_USER_IDS = user_ids
        VALID_PRODUCT_IDS = product_ids
    
    results = {
        'users': [],
        'products': [],
        'transactions': []
    }
    
    for _ in range(count):
        # Randomly select an error generator
        generator = random.choice(ERROR_GENERATORS)
        record_type, record_data = generator()
        
        # Add to appropriate list
        if record_type == 'user':
            results['users'].append(record_data)
        elif record_type == 'product':
            results['products'].append(record_data)
        elif record_type == 'transaction':
            results['transactions'].append(record_data)
    
    return results


def insert_invalid_records(records: Dict[str, List[Dict[str, Any]]]) -> int:
    """
    Insert generated invalid records into the OLTP database.
    
    Args:
        records: Dictionary with 'users', 'products', 'transactions' keys
        
    Returns:
        Total number of records inserted
    """
    db_path = get_oltp_db_path()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Disable foreign key constraints to allow orphan records
        cursor.execute('PRAGMA foreign_keys = OFF')
        
        total_inserted = 0
        
        # Insert users
        for user in records['users']:
            try:
                cursor.execute('''
                    INSERT INTO users (user_id, name, email, join_date)
                    VALUES (?, ?, ?, ?)
                ''', (user['user_id'], user['name'], user['email'], user['join_date']))
                total_inserted += 1
            except sqlite3.IntegrityError as e:
                print(f"  Warning: Could not insert user {user['user_id']}: {e}")
        
        # Insert products
        for product in records['products']:
            try:
                cursor.execute('''
                    INSERT INTO products (product_id, name, category, price, stock)
                    VALUES (?, ?, ?, ?, ?)
                ''', (product['product_id'], product['name'], product['category'], 
                      product['price'], product['stock']))
                total_inserted += 1
            except sqlite3.IntegrityError as e:
                print(f"  Warning: Could not insert product {product['product_id']}: {e}")
        
        # Insert transactions
        for transaction in records['transactions']:
            try:
                cursor.execute('''
                    INSERT INTO transactions 
                    (transaction_id, date, user_id, product_id, quantity, price, payment_type, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (transaction['transaction_id'], transaction['date'], transaction['user_id'],
                      transaction['product_id'], transaction['quantity'], transaction['price'],
                      transaction['payment_type'], transaction['status']))
                total_inserted += 1
            except sqlite3.IntegrityError as e:
                print(f"  Warning: Could not insert transaction {transaction['transaction_id']}: {e}")
        
        conn.commit()
        
        # Re-enable foreign key constraints
        cursor.execute('PRAGMA foreign_keys = ON')
        
        return total_inserted
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()





def main(count=20, today='2026-02-01'):
    print(f"Generating {count} invalid records for date: {today}")
    print("Querying OLTP database for valid reference IDs...")
    
    user_ids, product_ids = fetch_valid_ids()
    print(f"  Found {len(user_ids)} users: {user_ids[:10]}{' ...' if len(user_ids) > 10 else ''}")
    print(f"  Found {len(product_ids)} products: {product_ids[:10]}{' ...' if len(product_ids) > 10 else ''}")
    print()
    
    records = generate_invalid_records(count, today, user_ids, product_ids)
        
    print("Inserting invalid records into OLTP database...")
    inserted = insert_invalid_records(records)
    print(f"  Successfully inserted {inserted} records into database")


def main_cli():
    import sys
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(
        description='Generate invalid OLTP records for ETL testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python generate_invalid.py --count 20
  python generate_invalid.py -c 15 --today 2026-03-15
  python generate_invalid.py --count 10 --today 2026-02-01
        '''
    )
    
    parser.add_argument(
        '-c', '--count',
        type=int,
        default=20,
        help='Number of invalid records to generate (default: 20)'
    )
    
    parser.add_argument(
        '-t', '--today',
        type=str,
        default='2026-02-01',
        help='Date for transactions in YYYY-MM-DD format (default: 2026-02-01)'
    )
    
    args = parser.parse_args()
    
    try:
        datetime.strptime(args.today, '%Y-%m-%d')
    except ValueError:
        parser.error(f"Invalid date format '{args.today}'. Expected YYYY-MM-DD")
    
    main(args.count, args.today)


if __name__ == '__main__':
    main_cli()
