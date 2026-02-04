import sqlite3

def main():
    conn = sqlite3.connect('ecommerce-OLAP.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        full_date DATE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        week INTEGER,
        weekday INTEGER
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_user (
        user_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        name TEXT,
        email TEXT,
        join_date DATE,
        start_date DATE,
        end_date DATE,
        current_flag INTEGER
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dim_product (
        product_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        name TEXT,
        category TEXT,
        price REAL,
        start_date DATE,
        end_date DATE,
        current_flag INTEGER
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_transactions (
        transaction_id INTEGER,
        user_sk INTEGER,
        product_sk INTEGER,
        date_id INTEGER,
        quantity INTEGER,
        total REAL,
        payment_type TEXT,
        status TEXT,
        load_date DATE,
        PRIMARY KEY (transaction_id, product_sk),
        FOREIGN KEY (user_sk) REFERENCES dim_user(user_sk),
        FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS fact_stock_history (
        product_sk INTEGER,
        date_id INTEGER,
        stock INTEGER,
        load_date DATE,
        FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
        FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
    );
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
