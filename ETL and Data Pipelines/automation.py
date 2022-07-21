import psycopg2 as pg2
import os
from dotenv import load_dotenv
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# connect 
conn = pg2.connect(dbname=DB_NAME, user = DB_USER, password = DB_PASSWORD)
cur = conn.cursor()
connection = pg2.connect(dbname = datawarehouse, user= DB_USER, password = DB_PASSWORD)
curr = connection.cursor()


# get last row id
def get_last_rowid():
    curr.execute('SELECT row_id FROM sales ORDER BY row_id DESC LIMIT 1')
    connection.commit()
last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid: int):
    curr.execute('SELECT row_id FROM sales WHERE row_id > $last_row_id')
    conn.commit()
	
new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", len(new_records))


# Insert the additional records from PostgreSQL into data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database.

def insert_records(records):
	pass

insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# disconnect from mysql warehouse
cur.close()
# disconnect from DB2 data warehouse
curr.close()