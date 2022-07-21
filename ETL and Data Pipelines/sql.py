import psycopg2 as pg2
import os
from dotenv import load_dotenv
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

#connect to db
try:
    conn = pg2.connect(dbname=DB_NAME, user = DB_USER, password = DB_PASSWORD)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS sales(
            row_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            price INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            timestamp TIMESTAMP NOT NULL
        )
    ''')
    conn.commit()
    cur.close()
except Exception as e:
    print(e)