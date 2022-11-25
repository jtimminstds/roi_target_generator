import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.getenv('snowflake_user'),
    password=os.getenv('snowflake_password'),
    account=os.getenv('snowflake_account'),
    warehouse=os.getenv('snowflake_warehouse'),
    database=os.getenv('snowflake_database')
    )
cur = conn.cursor()