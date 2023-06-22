
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""
import chardet
import pandas as pd

# %% Identify encoding
# with open(file, 'rb') as f:
#     result = chardet.detect(f.read())  # or readline if the file is large
# print(result['encoding'])

# %% Change encoding
ls_files = ['./data/RenewalContract.csv']
for file in ls_files:
    # file = ls_files[0]
    data = pd.read_csv(file, encoding='MacRoman')
    data.to_csv(file, encoding='utf-8', index=False)
    del data
# %%
    pd.read_csv('./data/M2M_data.csv', encoding='MacRoman')
#result['encoding'])
