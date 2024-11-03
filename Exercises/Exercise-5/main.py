import psycopg2
import os
from collections import namedtuple

Table = namedtuple("Table", ["create_query", "index_column"])

acccounts = Table(
    '''
    CREATE TABLE IF NOT EXISTS accounts(
        customer_id integer PRIMARY KEY,
        first_name text,
        last_name text,
        address_1 text,
        address_2 text,
        city text,
        state text,
        zip_code integer,
        join_date date
    );
    ''',
    "customer_id"
)

products = Table(
    '''
    CREATE TABLE IF NOT EXISTS products(
        product_id integer PRIMARY KEY,
        product_code text,
        product_description text
    );
    ''',
    "product_id"
)

transactions = Table(
    '''
    CREATE TABLE IF NOT EXISTS transactions(
        transaction_id text PRIMARY KEY,
        transaction_date date,
        product_id integer,
        product_code text,
        product_description text,
        quantity integer,
        account_id integer,
        FOREIGN KEY(product_id) REFERENCES products(product_id),
        FOREIGN KEY(account_id) REFERENCES accounts(customer_id)
    );
    ''',
    "transaction_id"
)

table_dict = {
    "accounts": acccounts,
    "products": products,
    "transactions": transactions
}

def drop_table_if_exists(table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {table_name} CASCADE;"

def copy_data_from_csv(
    csv_file_name: str,
    table_name: str,
    cursor: psycopg2.extensions.cursor
) -> None:
    """
    Copy data from a csv file into a PostgresDB table

    Args:
        csv_file_name (str): CSV file name 
        table_name (str): PostgresDB table name
        cursor (psycopg2.extensions.cursor): The cursor use for executing SQL
    """
    
    csv_file_path = os.path.join("data", csv_file_name)
    with open(csv_file_path, "r") as csv_file:
        next(csv_file)
        cursor.copy_from(csv_file, table_name, sep=',')
        
def create_index(index_name: str, table_name: str, column_names: list[str] | str):
    column_name_list = [column_name for column_name in column_names]
    index_query = (
        f"CREATE INDEX {index_name} ON {table_name}({', '.join(column_name_list)})"
    )
    
    return index_query

def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()
    for table_name in table_dict:
        table = table_dict[table_name]
        
        cursor.execute(drop_table_if_exists(table_name))
        cursor.execute(table.create_query)
        print(f"---- Done create table {table_name} ----")

    print("Start import data")
    for csv_file in os.listdir("data"):
        table_name = csv_file.removesuffix(".csv")
        copy_data_from_csv(csv_file, table_name, cursor)
        create_index(f'{table_name}_idx', table_name, table_dict[table_name].index_column)
    
    print("Commit all change")
    conn.commit()
    cursor.close()
    conn.close()

        


if __name__ == "__main__":
    main()
