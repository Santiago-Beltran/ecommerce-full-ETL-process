import sqlite3

def main():
    conn = sqlite3.connect('ecommerce-OLTP.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INTEGER,
        date DATE,
        user_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        price REAL,
        payment_type TEXT,
        status TEXT,
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT UNIQUE NOT NULL,
        join_date DATE
    );
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name TEXT,
        category TEXT,
        price REAL,
        stock INTEGER
    );
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
