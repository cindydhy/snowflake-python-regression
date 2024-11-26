import snowflake.connector
import os
import sys

SNOWFLAKE_ACCOUNT = os.environ['SF_ACCOUNT']
SNOWFLAKE_USERNAME = os.environ['SF_USERNAME']
SNOWFLAKE_PASSWORD = os.environ['SF_PASSWORD']
SNOWFLAKE_ROLE = os.environ['SF_ROLE']
SNOWFLAKE_WAREHOUSE = os.environ['SF_WAREHOUSE']
SNOWFLAKE_DATABASE = os.environ['SF_DATABASE']

con = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USERNAME,
    password=SNOWFLAKE_PASSWORD,
    role=SNOWFLAKE_ROLE,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema="PUBLIC"
)

cur = con.cursor()

# get column names for md5 checksum
query = "SELECT DISTINCT COLUMN_NAME, ORDINAL_POSITION FROM WINE_DB.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'WHITE_WINE_QUALITY' ORDER BY ORDINAL_POSITION;"
cur.execute(query)
all_results = cur.fetchall()

# check if golden data tables are created
if len(all_results) == 0:
    print("golden data tables not created")
    sys.exit()

column_names = "("
count = 0

for columns in all_results:
    column_names += str(columns[0])
    if (count != 0 and count != len(all_results) - 1):
        column_names += ", "
    count += 1
column_names += ")"

print("column names: " + column_names)

# get resource tables
query = "SELECT TABLE_NAME FROM WINE_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%%GD%%';"
cur.execute(query)
all_results = cur.fetchall()
table_names = []
count = 0

for tables in all_results:
    table_names.append(tables[0])

print("table names: " + str(table_names))

for table in table_names:
    query = "INSERT INTO " + table + " " + column_names + " SELECT " + column_names + " FROM WHITE_WINE_QUALITY;"
    print(query)
    cur.execute(query)
    query = "UPDATE " + table + " SET GD_MD5_VALUE=MD5(TO_VARCHAR(ARRAY_CONSTRUCT" + column_names + "));"
    print(query)
    cur.execute(query)

cur.close()
con.close()