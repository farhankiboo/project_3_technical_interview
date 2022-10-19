import json
import zipfile
import psycopg2 as pg
from zipfile import ZipFile
import pandas as pd
from sqlalchemy import create_engine

schema_json = '/Users/farha/Documents/tutorial_python/project_3_de/sql/schemas/user_address.json' 
create_schema_sql = """ create table user_address_2018_snapshots {} """
zip_small_file = '/Users/farha/Documents/tutorial_python/project_3_de/temp/dataset-small.zip'
small_file_name = 'dataset-small.csv'
database='shipping_orders'
user='postgres'
password=''
host='127.0.0.1'
port=1234
table_name = 'user_address_2018_snapshots'

with open(schema_json, 'r') as schema:
    content = json.loads(schema.read())

list_schema = []
for c in content:
    col_name = c['column_name']
    col_type = c['column_type']
    constraint = c['is_null_able']
    ddl_list = [col_name, col_type, constraint]
    list_schema.append(ddl_list)

list_schema_2 = []
for l in list_schema:
    s = ' '.join(l)
    list_schema_2.append(s)

create_schema_sql_final = create_schema_sql.format(tuple(list_schema_2)).replace("'", "")

#Init Postges connection
conn = pg.connect(database=database,
                    user=user,
                    password=password,
                    host=host,
                    port=port)

conn.autocommit=True
cursor=conn.cursor()

try:
    cursor.execute(create_schema_sql_final)
    print("DDL schema created successfully...")
except pg.errors.DuplicateTable:
    print("table already created...")

#Load zipped file to dataframe
zf = ZipFile(zip_small_file)
df = pd.read_csv(zf.open(small_file_name), header=None)

col_name_df = [c['column_name'] for c in content]
df.columns = col_name_df

#check min dan max tanggal
# print(df['created_at'].min())
# print(df['created_at'].max())

df_filtered = df[(df['created_at'] >= '2018-02-01') & (df['created_at'] < '2018-12-31')]

# # Check hasil filtering tanggal
# print(df_filtered['created_at'].min())
# print(df_filtered['created_at'].max())

#create engine
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

#insert to postgres 
df_filtered.to_sql(table_name, engine, if_exists='replace', index=False)
print(f'Total inserted rows: {len(df_filtered)}')
print(f'Initial created_at: {df_filtered.created_at.min()}')
print(f'Last created_at: {df_filtered.created_at.max()}')