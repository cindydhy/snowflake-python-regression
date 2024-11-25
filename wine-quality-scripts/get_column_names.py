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

query = "SELECT DISTINCT COLUMN_NAME FROM WINE_DB.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'WHITE_WINE_QUALITY';"
cur.execute(query)
column_names = cur.fetchall()
print(column_names)

cur.close()
con.close()