import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def merge_and_normalize(df1, df2, common_column):
    if df1 is None or df1.empty:
        return df2
    
    value_df1_2023_01_10 = df1.loc[df1['datetime'] == common_column, 'int_value'].values[0]
    value_df2_2023_01_10 = df2.loc[df2['datetime'] == common_column, 'int_value'].values[0]

    if value_df2_2023_01_10 != 0:
        ratio = value_df1_2023_01_10 / value_df2_2023_01_10
        df2['int_value'] = df2['int_value'] * ratio

    else:
        print("Error: Denominator is zero, cannot normalize.")

    return df2
