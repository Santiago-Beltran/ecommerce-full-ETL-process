import sqlite3
import os

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
    olap_db_path = os.path.join(repo_root, 'OLAP', 'ecommerce-OLAP.db')
    conn = sqlite3.connect(olap_db_path)

    conn.execute('''
    CREATE TABLE IF NOT EXISTS etl_run_log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_date DATE,
        source_date DATE,
        status TEXT CHECK(status IN ('success','failed')),
        started_at DATETIME,
        ended_at DATETIME,
        duration_ms INTEGER,
        rows_dim_user_inserted INTEGER,
        rows_dim_product_inserted INTEGER,
        rows_fact_transactions_inserted INTEGER,
        rows_fact_stock_history_inserted INTEGER,
        errors INTEGER,
        warnings INTEGER,
        notes TEXT
    );
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS etl_error_log (
        error_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL,
        entity TEXT CHECK(entity IN ('user','product','transaction','date')),
        table_name TEXT,
        record_id TEXT,
        error_type TEXT,
        message TEXT,
        created_at DATETIME,
        severity TEXT CHECK(severity IN ('error','warning')),
        FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
    );
    ''')

    conn.commit()
    conn.close()

if __name__ == '__main__':
    main()
