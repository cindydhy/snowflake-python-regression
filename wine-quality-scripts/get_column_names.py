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

# resource tables
resource_tables = ['GD_ACCOUNT_ADT', 'GD_ALLERGY_INTOLERANCE_ADT', 'GD_BUNDLE_RESOURCE_METADATA_ADT', 'GD_CONDITION_ADT', 'GD_CONTRACT_ADT']

query = "SELECT TABLE_NAME FROM WINE_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '%%GD%%';"
cur.execute(query)
all_results = cur.fetchall()
table_names = []

for names in all_results:
    print("looping")
    table_names.append(names[0])
print("column names: " + str(table_names))

cur.close()
con.close()