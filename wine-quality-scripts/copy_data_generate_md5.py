import snowflake.connector
import os

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

# get list of tables to copy WHITE_WINE_QUALITY into
query = "SELECT TABLE_NAME FROM WINE_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%%GD%%';"
cur.execute(query)
all_results = cur.fetchall()
table_names = []

for names in all_results:
    print("looping")
    table_names.append(names[1])
print("column names: " + str(table_names))

cur.close()
con.close()