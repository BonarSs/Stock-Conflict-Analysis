import pandas as pd
import os
from sqlalchemy import create_engine


def merge_and_normalize():
    current_directory = os.path.dirname(os.path.abspath(__file__))

    csv_path = os.path.join(current_directory,'data','trends.csv')
    df2 = pd.read_csv(csv_path)


    connection_string = "postgresql://aam.inam7310:PuHBc2Dj9MiN@ep-delicate-mode-58704103.ap-southeast-1.aws.neon.tech/datapipeline?sslmode=require"
    engine = create_engine(connection_string)
    query = f"SELECT * FROM trends WHERE datetime = '{df2.sort_values(by='datetime', ascending=True)['datetime'].iloc[0]}' ;"
    df1 = pd.read_sql(query, engine)


    common_datetime = df2.sort_values(by='datetime', ascending=True)['datetime'].iloc[0]

    col_df1 = df1.loc[df1['datetime'] == common_datetime].drop(columns=['datetime'], axis=1).reset_index(drop=True)
    col_df2 = df2.loc[df2['datetime'] == common_datetime].drop(columns=['datetime'], axis=1).reset_index(drop=True)

    if (col_df2 == 0).any().any():
        print("Error: Denominator is zero, cannot normalize.")
    else:
        ratio = col_df1 / col_df2
        df2.iloc[:, 1:] = df2.iloc[:, 1:].multiply(ratio.values[0])
    
    save_path= os.path.dirname(os.path.abspath(__file__))
    csv_filename = os.path.join(save_path,'data', 'trends.csv')
    os.makedirs(os.path.join(save_path,'data'), exist_ok=True)


    if os.path.exists(csv_filename):
        df2.to_csv(csv_filename, mode='w')
    else:
        df2.to_csv(csv_filename)


