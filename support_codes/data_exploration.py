
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import pandas as pd
import numpy as np

def summarize_data(in_data_org, dict_cols):
    in_data = in_data_org.copy()
    del in_data_org
    for col in dict_cols:
        print(f'*** {col} ***')

        if dict_cols[col] == 'date':
            in_data[col] = pd.to_datetime(in_data[col])
            print(f'Range {col}: \n{in_data[col].min()}, {in_data[col].max()}')
            in_data[col] = in_data[col].dt.year

            df_temp = in_data[col].value_counts().reset_index()
            df_temp = df_temp.sort_values(['index'])
            print(f'Value counts: \n {df_temp}')

        elif dict_cols[col] == 'str':
            print(f'Value counts: \n{in_data[col].value_counts()}')

        elif dict_cols[col] in ('int', 'float'):
            in_data[col] = pd.to_numeric(in_data[col], errors='coerce')
            print(f'Range {col}: {in_data[col].min()}, {in_data[col].max()}')
            print(
                f'Quartiles 25, 50, 75: {np.nanquantile(in_data[col], .25)}, {np.nanquantile(in_data[col], .50)}, {np.nanquantile(in_data[col], .75)}')
        elif dict_cols[col] == 'str_list':
            print(f'Unique values : {list(in_data[col].unique())}')
        else:
            if dict_cols[col] not in ('str_na'):
                print(f"{col} :unknown type")

        no_na = sum(pd.isna(in_data[col]))
        perc_na = round(no_na / in_data.shape[0] * 100, 0)
        print(f'Count of NAs: : {no_na} ({perc_na}%) \n')
