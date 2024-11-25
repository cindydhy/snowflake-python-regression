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

query = "SELECT DISTINCT COLUMN_NAME, ORDINAL_POSITION FROM WINE_DB.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'WHITE_WINE_QUALITY' ORDER BY ORDINAL_POSITION;"
cur.execute(query)
all_results = cur.fetchall()
column_names = []
for names in all_results:
    column_names.append(names[0])

print("column names: " + str(column_names))

query = "SELECT * FROM WHITE_WINE_QUALITY LIMIT 1';"
cur.execute(query)
first_result = cur.fetchall()
print(first_result)

cur.close()
con.close()