import psycopg2
import os
import csv
from tabulate import tabulate

sql_scripts_folder_path = 'scripts'
csv_folder_path = 'data'

host = "postgres"
database = "postgres"
user = "postgres"
password = "postgres"


def connect_to_database():
    try:
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        return connection
    except psycopg2.Error as e:
        print(f"Error: Unable to connect to the database - {e}")
        raise


def execute_sql_file(cursor, file_path):
    try:
        with open(file_path, 'r') as script_file:
            sql_script = script_file.read()
            cursor.execute(sql_script)
        print(f"Executed SQL script from {file_path}")
    except psycopg2.Error as e:
        print(f"Error executing SQL script {file_path}: {e}")
        raise


def table_exists(cursor, table_name):
    cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s);", (table_name,))
    return cursor.fetchone()[0]


def create_tables(cursor):
    tables = []
    for filename in os.listdir(sql_scripts_folder_path):
        if filename.endswith('.sql'):
            file_path = os.path.join(sql_scripts_folder_path, filename)

            table_name = filename[:-4]

            if table_exists(cursor, table_name):
                cursor.execute(f"DROP TABLE {table_name};")
                print(f"Dropped existing table '{table_name}'")
                connection.commit()

            execute_sql_file(cursor, file_path)

            tables.append(table_name)
    return tables


def insert_data_from_csv(cursor, table_name, csv_file_path):
    try:
        with open(csv_file_path, 'r') as csv_file:
            # Use string formatting to insert the table name into the COPY command
            copy_query = f"COPY {table_name} FROM STDIN WITH CSV HEADER"
            cursor.copy_expert(copy_query, csv_file)
        print(f"Inserted data into table '{table_name}' from CSV file {csv_file_path}")
    except (psycopg2.Error, FileNotFoundError) as e:
        print(f"Error inserting data into table '{table_name}': {e}")
        raise


def insert_data(cursor, tables):
    for table_name in tables:
        csv_file_path = os.path.join(csv_folder_path, f"{table_name}.csv")
        if os.path.exists(csv_file_path):
            insert_data_from_csv(cursor, table_name, csv_file_path)


def show_data(cursor, tables):
    for table in tables:
        # Execute a SELECT query to fetch all rows from the table
        cursor.execute(f"SELECT * FROM {table};")

        # Fetch all the rows
        rows = cursor.fetchall()

        # Get the column names
        columns = [desc[0] for desc in cursor.description]

        # Pretty-print the data using the tabulate library
        print(f"\nTable: {table}")
        print(tabulate(rows, headers=columns, tablefmt='psql'))


def main():
    try:
        with connect_to_database() as connection:
            with connection.cursor() as cursor:
                tables = create_tables(cursor)
                insert_data(cursor, tables)
                show_data(cursor, tables)
    except psycopg2.Error:
        # Handle any database-related errors here
        pass


if __name__ == "__main__":
    main()
