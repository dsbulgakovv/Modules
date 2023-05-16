import pandas as pd
import urllib3
import sqlalchemy as sa
import urllib

urllib3.disable_warnings()  # Warnings are disabled now


# Function for connection to database
def db_connect(server_addr, db_name): # Get two parameteres: server_addr - adress of the database server and db_name - database name
    params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                     f"SERVER={server_addr};"
                                     f"DATABASE={db_name};"
                                     "Trusted_Connection=yes;")
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    return engine # Rerurns object - connection engine itself

# Instead of uploading all rows to the RAM instantly, the function will load rows iteratively by clusters from database
def get_stream_conn(engine):
    conn = engine.connect().execution_options(stream_results=True)
    return conn


# Function for sending SQL query (returns nothing) to the server with the help of pandas
def empty_query(query, engine): # Takes two parameters: query - SQL query as a string and engine - engine itself
    try:
        pd.read_sql(query, con=engine)
    except sa.exc.ResourceClosedError:
        print('Completed') # Return nothong and print status message


# Function creates temproary table in the certain database
def create_temp_table(engine, # Takes engine as argument
                      fields, # Takes all needed fields to be loaded in database as a list with strings: ['OldLicenseId NVARCHAR(60)', ...]
                      name='#TEMP'): # Takes temproary table name
    drop_temp_table(name, engine)
    temp_table_query = f'''CREATE TABLE {name} ('''
    for i in range(0, len(fields)):
        if i == 0:
            temp_table_query += f'''{fields[i]},'''
        elif i == len(fields) - 1:
            temp_table_query += f''' {fields[i]})'''
        else:
            temp_table_query += f''' {fields[i]},'''
    try:
        pd.read_sql(temp_table_query, con=engine)
    except sa.exc.ResourceClosedError:
        print('Temporary table was created') # Returns nothing and print status message


# Function for select * from the needed table
def select_all_query(table_name, engine): # Takes table name and engine as arguments
    query_select = f'''SELECT * FROM {table_name}'''
    df = pd.read_sql(query_select, con=engine) 
    return df # Returns pandas dataframe


# Function for select top 100 rows from the needed table
def select_top_100_query(table_name, engine): # Takes table name and engine as arguments
    query_select = f'''SELECT TOP 100 * FROM {table_name}'''
    df = pd.read_sql(query_select, con=engine)
    return df # Returns pandas dataframe


# Function for dropping a temproary table from the database
def drop_temp_table(table_name, engine): # Takes temproary table name and engine as arguments
    query_drop = f'''DROP TABLE IF EXISTS {table_name}'''
    try:
        pd.read_sql(query_drop, con=engine)
    except sa.exc.ResourceClosedError:
        print('The table was dropped') # Returns nothing and print status message


# Function that generate a list with strings with all values from dataframe 
# Transform dataframe to list with strings that are formatted for SQL INSERT query
def df_to_list(init_df): # Takes dataframe as argument
    data = []
    for i in range(0, len(init_df)):
        row = "('"
        for j in range(0, len(init_df.iloc[0])):
            if j != len(init_df.iloc[0]) - 1:
                row += str(init_df.iloc[i][j]) + "', '"
            else:
                row += str(init_df.iloc[i][j]) + "')"
        data.append(row)
    return data # Return a list with strings (all values from df)


# Function creates a string with all column names
def get_columns_str(data): # Takes dataframe as argument
    columns = list(data.columns)
    column_names = '('
    for x in range(0, len(columns)):
        if x != len(columns) - 1:
            column_names += (columns[x] + ', ')
        else:
            column_names += (columns[x] + ')')
    return column_names # Returns string


# Function generates SQL queries and send them to the server for creating temproary table from dataframe (sends 1000 rows at one time)
def df_to_temp_table(data, # Takes dataframe
                     fields, # Takes needed fieds names and its data types: ['OldLicenseId NVARCHAR(60)']
                     engine, # Takes engine for connection to the server
                     table_name='#TEMP'): # Takes table name (have default value)
    create_temp_table(engine, fields, table_name)
    full_data = df_to_list(data)
    insert = f'INSERT INTO {table_name} VALUES '
    tail = len(full_data) - (len(full_data) // 1000) * 1000
    for i in range(0, (len(full_data) // 1000 + 1)):
        query_insert = insert
        if i != len(full_data) // 1000:
            for j in range(0, 1000):
                string = full_data[i * 1000 + j]
                if j != 999:
                    query_insert += string + ','
                else:
                    query_insert += string
            try:
                pd.read_sql(query_insert, con=engine)
            except sa.exc.ResourceClosedError:
                continue
        else:
            if tail != 0:
                for k in range(0, tail):
                    string = full_data[i * 1000 + k]
                    if k != tail - 1:
                        query_insert += string + ', '
                    else:
                        query_insert += string
                try:
                    pd.read_sql(query_insert, con=engine)
                except sa.exc.ResourceClosedError:
                    continue
    print(f'Data migrated to MS SQL Server as temporary table {table_name}') # Return nothing and print status message
