import psycopg2 as pg2 
import os
from dotenv import load_dotenv
load_dotenv('./env')

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
# connect to database
try:
    conn = pg2.connect(
        dbname= DB_NAME,
        user= DB_USER,
        password = DB_PASSWORD
    )
    cur = conn.cursor()
except Exception as e:
    print(e)


create_table= '''
CREATE TABLE sales_data(
    product_id INTEGER NOT NULL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    price INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    timestamp TIMESTAMP
)
'''
