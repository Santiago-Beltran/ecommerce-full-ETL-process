import sqlite3
import random
from faker import Faker
from datetime import date

fake = Faker()

def populate_db_first_time(conn: sqlite3.Connection, num_users: int, num_products: int, join_date: date):
    cursor = conn.cursor()

    # Clear tables (order matters because of FK constraints)
    cursor.execute("DELETE FROM transactions")
    cursor.execute("DELETE FROM users")
    cursor.execute("DELETE FROM products")

    create_new_users(conn, num_users, join_date)
    create_new_products(conn, num_products)

    conn.close()

def create_new_users(conn: sqlite3.Connection, num_new_users: int, join_date: date):
    cursor = conn.cursor()

    users = []
    for _ in range(num_new_users):
        name = fake.name()
        email = fake.unique.email()
        users.append((name, email, join_date))  # use provided join_date

    cursor.executemany("""
        INSERT INTO users (name, email, join_date)
        VALUES (?, ?, ?);
    """, users)

    conn.commit()


def create_new_products(conn: sqlite3.Connection, num_new_products: int):
    cursor = conn.cursor()

    categories = ['Footwear', 'Electronics', 'Clothing', 'Accessories', 'Beauty']
    products = []

    for _ in range(num_new_products):
        name = f"{fake.word().capitalize()} {fake.word().capitalize()}"
        category = random.choice(categories)
        price = round(random.uniform(5.0, 500.0), 2)
        stock = random.randint(1, 200)
        products.append((name, category, price, stock))

    cursor.executemany("""
        INSERT INTO products (name, category, price, stock)
        VALUES (?, ?, ?, ?);
    """, products)

    conn.commit()


def change_existent_users(conn: sqlite3.Connection, num_users_to_change: int):
    cursor = conn.cursor()

    # Get all existing user_ids
    cursor.execute("SELECT user_id FROM users")
    all_user_ids = [row[0] for row in cursor.fetchall()]

    if not all_user_ids:
        return

    # Clamp number to available users
    num_users_to_change = min(num_users_to_change, len(all_user_ids))

    # Randomly select users to change
    users_to_change = random.sample(all_user_ids, num_users_to_change)

    # Only update the email now
    updates = []
    for user_id in users_to_change:
        new_email = fake.unique.email()
        updates.append((new_email, user_id))

    cursor.executemany("""
        UPDATE users
        SET email = ?
        WHERE user_id = ?;
    """, updates)

    conn.commit()


def change_existent_products(
    conn: sqlite3.Connection,
    num_products_to_change: int,
    price_range: list,
):
    """Update the price of a random selection of existing products.

    This function only updates `price` and does not modify name/category/stock.
    """
    cursor = conn.cursor()

    # Get all existing product_ids
    cursor.execute("SELECT product_id FROM products")
    all_product_ids = [row[0] for row in cursor.fetchall()]

    if not all_product_ids:
        return

    # Clamp number to available products
    num_products_to_change = min(num_products_to_change, len(all_product_ids))

    # Randomly select products to change
    products_to_change = random.sample(all_product_ids, num_products_to_change)

    for product_id in products_to_change:
        new_price = round(random.uniform(price_range[0], price_range[1]), 2)
        cursor.execute(
            "UPDATE products SET price = ? WHERE product_id = ?",
            (new_price, product_id),
        )

    conn.commit()


def update_product_stocks(
    conn: sqlite3.Connection,
    num_products_to_change: int,
    stock_range: list,
    stock_threshold: int = None,
):
    """Restock products whose stock is below `stock_threshold` (or all selected if threshold is None).

    New stock values are chosen randomly between max(current_stock+1, stock_range[0]) and stock_range[1].
    """
    cursor = conn.cursor()

    # Get all existing product_ids
    cursor.execute("SELECT product_id FROM products")
    all_product_ids = [row[0] for row in cursor.fetchall()]

    if not all_product_ids:
        return

    # Clamp number to available products
    num_products_to_change = min(num_products_to_change, len(all_product_ids))

    # Randomly select products to consider for restocking
    products_to_change = random.sample(all_product_ids, num_products_to_change)

    for product_id in products_to_change:
        cursor.execute("SELECT stock FROM products WHERE product_id = ?", (product_id,))
        row = cursor.fetchone()
        if not row:
            continue
        current_stock = row[0] if row[0] is not None else 0

        if stock_threshold is not None and current_stock >= stock_threshold:
            continue

        lower = max(current_stock + 1, stock_range[0])
        upper = stock_range[1]
        if lower <= upper:
            new_stock = random.randint(lower, upper)
            cursor.execute(
                "UPDATE products SET stock = ? WHERE product_id = ?",
                (new_stock, product_id),
            )

    conn.commit()


def create_new_transactions(
    conn: sqlite3.Connection,
    n: int,
    transaction_date: date,
    batch_size: int = 500,
    status_weights: list = None,
    multi_product_chance: float = 0.2,  # 20% chance of multi-product transactions
):
    # use module-level `random` imported at the top of the file
    cursor = conn.cursor()

    # load users
    cursor.execute("SELECT user_id FROM users")
    users = [r[0] for r in cursor.fetchall()]

    if not users:
        raise ValueError("No users available to create transactions.")

    # load products with stock and price
    cursor.execute("SELECT product_id, price, stock FROM products")
    products = [{"id": r[0], "price": r[1], "stock": r[2]} for r in cursor.fetchall()]
    products = [p for p in products if p["stock"] and p["stock"] > 0]
    if not products:
        raise ValueError("No products with stock available.")


    inserts = []
    updates = {}

    inserted = 0
    
    # Get the next transaction_id (max + 1, or 1 if no transactions exist)
    cursor.execute("SELECT MAX(transaction_id) FROM transactions")
    result = cursor.fetchone()
    next_transaction_id = (result[0] or 0) + 1

    # prepare weighted product list by stock to favor available items
    product_pool = []
    for p in products:
        weight = min(max(int(p["stock"]), 1), 10)
        product_pool += [p["id"]] * weight
    prod_map = {p["id"]: p for p in products}

    payment_types = ["visa", "mastercard", "wire transfer", "other"]
    status_choices = ["success", "failed"]

    # Resolve status selection weights
    if status_weights is None:
        weights = [0.9, 0.1]
    else:
        if not isinstance(status_weights, (list, tuple)):
            raise ValueError("status_weights must be a list or tuple of numeric weights")
        if len(status_weights) != len(status_choices):
            raise ValueError(f"status_weights length must be {len(status_choices)}")
        weights = list(status_weights)

    # Track current transaction context for multi-product transactions
    current_transaction_context = None

    for _ in range(n):
        # If we don't have a transaction context, or we're starting a new transaction
        if current_transaction_context is None:
            user_id = random.choice(users)
            payment_type = random.choice(payment_types)
            status = random.choices(status_choices, weights=weights)[0]
            current_transaction_context = {
                'transaction_id': next_transaction_id,
                'user_id': user_id,
                'payment_type': payment_type,
                'status': status
            }

        prod_id = random.choice(product_pool)
        prod = prod_map[prod_id]

        available = prod["stock"] - updates.get(prod_id, 0)
        if available <= 0:
            continue

        qty = random.randint(1, min(5, available))
        unit_price = prod["price"]
        total_price = round(unit_price * qty, 2)
        created_at = transaction_date

        inserts.append((
            current_transaction_context['transaction_id'],
            created_at,
            current_transaction_context['user_id'],
            prod_id,
            qty,
            total_price,
            current_transaction_context['payment_type'],
            current_transaction_context['status']
        ))

        # only decrement stock for successful transactions
        if current_transaction_context['status'] == "success":
            updates[prod_id] = updates.get(prod_id, 0) + qty

        inserted += 1

        if random.random() > multi_product_chance:
            # Start new transaction next iteration
            next_transaction_id += 1
            current_transaction_context = None

        if len(inserts) >= batch_size:
            # flush batch
            with conn:
                cursor.executemany(
                    "INSERT INTO transactions (transaction_id, date, user_id, product_id, quantity, price, payment_type, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    inserts,
                )
                if updates:
                    cursor.executemany(
                        "UPDATE products SET stock = stock - ? WHERE product_id = ?",
                        [(qty, pid) for pid, qty in updates.items()],
                    )
            inserts = []
            updates = {}

    # flush remaining
    if inserts:
        with conn:
            cursor.executemany(
                "INSERT INTO transactions (transaction_id, date, user_id, product_id, quantity, price, payment_type, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                inserts,
            )
            if updates:
                cursor.executemany(
                    "UPDATE products SET stock = stock - ? WHERE product_id = ?",
                    [(qty, pid) for pid, qty in updates.items()],
                )

    return inserted



