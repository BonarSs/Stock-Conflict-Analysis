from sqlalchemy import create_engine, inspect
import pandas as pd
import os

def load_stocks_to_database():
    current_directory = os.path.dirname(os.path.abspath(__file__))

    csv_path = os.path.join(current_directory,'data','stock.csv')
    df = pd.read_csv(csv_path)

    connection_string = "postgresql://aam.inam7310:PuHBc2Dj9MiN@ep-delicate-mode-58704103.ap-southeast-1.aws.neon.tech/datapipeline?sslmode=require"
    engine = create_engine(connection_string)

    df.to_sql('stocks', engine, if_exists='replace', index=False)
