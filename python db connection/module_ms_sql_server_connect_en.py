import pandas as pd
import urllib3
import sqlalchemy as sa
import urllib

urllib3.disable_warnings()  # Отключение предупреждений


# Функция подключения к базе данных
# Принимает два параметра server_addr - адрес сервера с БД и db_name - название БД
# Возвращает объект - сам движок
def db_connect(server_addr, db_name):
    params = urllib.parse.quote_plus("DRIVER={SQL Server};"
                                     f"SERVER={server_addr};"
                                     f"DATABASE={db_name};"
                                     "Trusted_Connection=yes;")
    engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    return engine

# Вместо того, чтобы загружать все строки в память,
# она будет загружать строки из базы данных итеративно
def get_stream_conn(engine):
    conn = engine.connect().execution_options(stream_results=True)
    return conn


# Функция отправления SQL запроса (не возвращающего ничего) на сервер с помощью pandas
# Принимает два параметра: query - SQL запрос в строковом виде и engine - движок
# Ничего не возвращает, только печатает статус выполнения
def empty_query(query, engine):
    try:
        pd.read_sql(query, con=engine)
    except sa.exc.ResourceClosedError:
        print('Completed')


# Функция создает временную таблицу в указанной БД
# Принимает движок
# Принимает поля, которые нжно создать в виде списка со строками ['OldLicenseId NVARCHAR(60)']
# Принимает название временной таблицы
# На выходе печатает статус обработанного запроса
def create_temp_table(engine, fields, name='#TEMP'):
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
        print('Temporary table was created')



# Функция sql запроса со всем содержимым таблицы
# Принимает название таблицы и движок
# Возвращает
def select_all_query(table_name, engine):
    query_select = f'''SELECT * FROM {table_name}'''
    df = pd.read_sql(query_select, con=engine)
    return df


def select_top_100_query(table_name, engine):
    query_select = f'''SELECT TOP 100 * FROM {table_name}'''
    df = pd.read_sql(query_select, con=engine)
    return df


# Функция sql запроса для удаления временной таблицы
# Принимает имя таблицы и движок
def drop_temp_table(table_name, engine):
    query_drop = f'''DROP TABLE IF EXISTS {table_name}'''
    try:
        pd.read_sql(query_drop, con=engine)
    except sa.exc.ResourceClosedError:
        print('The table was dropped')


# Функция генерации списка со всеми значениями датарейма в виде строк
# Принимает датафрейм pandas и привращает в список строк, подходящих для SQL INSERT запроса
# Возвращает списко со строками - значениями
def df_to_list(init_df):
    data = []
    for i in range(0, len(init_df)):
        row = "('"
        for j in range(0, len(init_df.iloc[0])):
            if j != len(init_df.iloc[0]) - 1:
                row += str(init_df.iloc[i][j]) + "', '"
            else:
                row += str(init_df.iloc[i][j]) + "')"
        data.append(row)
    return data


# Функция создает строку с названиями всех колонок датафрейма
# Принимает датафрейм
# Возвращает строку
def get_columns_str(data):
    columns = list(data.columns)
    column_names = '('
    for x in range(0, len(columns)):
        if x != len(columns) - 1:
            column_names += (columns[x] + ', ')
        else:
            column_names += (columns[x] + ')')
    return column_names


# Функция для генерации SQL запросов создания временной таблицы
# на SQL сервере с вводом по 1000 значений за раз


# Принимает full_data - принимает датафрейм
# Принимает поля, которые нжно создать в виде списка со строками ['OldLicenseId NVARCHAR(60)']
# Принимает table_name - принимает название временной таблицы в виде строки
# Принимает engine - переменную с подключением к серверу через sqlalchemy
def df_to_temp_table(data, fields, engine, table_name='#TEMP'):
    create_temp_table(engine, fields, table_name)
    full_data = df_to_list(data)
#    column_names = get_columns_str(data)
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
    print(f'Data migrated to MS SQL Server as temporary table {table_name}')
