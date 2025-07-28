import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()  # This loads .env into environment

# connecting to snowflake, 
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    application = 'first_raw_load'
)

curs= conn.cursor()
print("2.2 object =>", type(curs))

try :
    curs.execute("""select
                 current_version(), current_database(),
                 current_schema(), current_warehouse(),current_client()""")
    one_row = curs.fetchone()
finally:
    print("Connection closed successfully.")

csv_urls = [
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Calendar.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Customers.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Product_Categories.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Product_Subcategories.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Products.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Returns.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Sales_2015.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Sales_2016.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Sales_2017.csv',
    'https://raw.githubusercontent.com/anshlambaoldgit/Adventure-Works-Data-Engineering-Project/main/Data/AdventureWorks_Territories.csv'
]

# for url in csv_urls:
#     table_name = url.split('/')[-1].replace('.csv','').lower()

#     df = pd.read_csv(url, delimiter = ",")
#     print(f"\n file: {table_name}")
#     print("Columns:" ,df.columns.tolist())
#     print("Types:", df.dtypes)


def generate_create_table_sql(table_name, df):
    dtype_map ={
        'object': 'VARCHAR',
        'int64': 'INT',
        'float64': 'FLOAT',
        'datetime64[ns]': 'TIMESTAMP'
    }

    columns_defs =[]
    for col in df.columns:
        pandas_dtype = str(df[col].dtype)
        print(pandas_dtype)
        snowflake_dtype = dtype_map.get(pandas_dtype, 'VARCHAR')
        print(snowflake_dtype)
        columns_defs.append(f'"{col}" {snowflake_dtype}')
        print("Columns list", columns_defs)
    column_names = ",\n ".join(columns_defs)
    # print("Columns definitions:", column_names)
    create_table_sql = f"""
    create or replace table RAW.{table_name}(
    {column_names})
    """
    # print(create_table_sql)
    return create_table_sql

for url in csv_urls:
    table_name = url.split('/')[-1].replace('.csv','').upper()

    try:
        df = pd.read_csv(url, delimiter=",", encoding="latin1") 
        create_table_sql = generate_create_table_sql(table_name, df)
        curs.execute(create_table_sql)

        success, nchunks, nrows ,_ = write_pandas(conn, df, table_name=table_name,schema='RAW')

        print(f"Data loaded into {table_name} successfully: {nrows} rows in {nchunks} chunks.")
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error reading {table_name}: {e}")



