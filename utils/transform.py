
"""@file



@brief


@details


@copyright 2021 Eaton Corporation. All Rights Reserved.
@note Eaton Corporation claims proprietary rights to the material disclosed
here on. This technical information may not be reproduced or used without
direct written permission from Eaton Corporation.
"""

import numpy as np
import pandas as pd

class Transform:

    @staticmethod
    def prioratized_columns(self, temp_data_org, ls_priority):
        #  df_data = df_data_install.copy()
        # ls_priority = ['ShipTo_Country', 'SoldTo_Country']
        temp_data = temp_data_org.copy()
        del temp_data_org

        temp_data['out'] = np.nan

        for col in ls_priority:
            # col = ls_priority[0]
            flag_na = pd.isna(temp_data['out'])
            flag_empty = temp_data['out'] == ''
            flag_all = (flag_na | flag_empty)
            temp_data.loc[flag_all, 'out'] = temp_data.loc[flag_all, col]

        ar_out = np.array(temp_data['out'])
        return ar_out
