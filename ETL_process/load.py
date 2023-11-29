from sqlalchemy import create_engine, inspect

def map_dtype_to_postgres(dtype):
    if 'datetime' in str(dtype):
        return 'TIMESTAMP'
    elif 'int' in str(dtype):
        return 'FLOAT'
    else:
        return 'VARCHAR'

def create_table_if_not_exists(dataframe, table_name):
    
    connection_string = "postgresql://aam.inam7310:PuHBc2Dj9MiN@ep-delicate-mode-58704103.ap-southeast-1.aws.neon.tech/datapipeline?sslmode=require"
    engine = create_engine(connection_string)

    inspector = inspect(engine)
    table_exists = inspector.has_table(table_name)

    if not table_exists:
        dtypes_dict = {col: map_dtype_to_postgres(dataframe[col].dtype) for col in dataframe.columns}

        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} {dtype}' for col, dtype in dtypes_dict.items()])})"

        with engine.connect() as connection:
            connection.execute(create_table_sql)

        print(f"Table '{table_name}' created in PostgreSQL.")

    dataframe.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"DataFrame successfully loaded into the '{table_name}' table in PostgreSQL.")

