def idetify_product_fr_partnum(self, ar_data):
    # ar_data = list(df_data_install.PartNumber)

    # Input:
    col_ref = 'Part_Number'

    df_data = pd.DataFrame(data={'data': ar_data})

    # Reference Data
    ref_prod = self.ref_lead_opp.copy()
    ref_prod = ref_prod[(ref_prod.flag_keep) & (
        pd.notna(ref_prod.Part_Number))]
    ref_prod = ref_prod[ref_prod.Product.isin(
        ['PDU', 'RPP', 'STS', 'Reactor'])]
    ref_prod.loc[:, col_ref] = ref_prod[col_ref].str.lower()

    # Pre-Process Data
    ls_prod = ref_prod.Product.unique()
    df_data['Product'] = ''
    for prod in ls_prod:
        # prod = ls_prod [0]
        ls_pattern = ref_prod.loc[
            ref_prod.Product == prod, col_ref]
        pat_prod = "(" + '|'.join(ls_pattern) + ")"

        df_data['flag_prod'] = df_data.data.apply(
            lambda x: re.search(pat_prod, x) is not None)
        if any(df_data['flag_prod']):
            df_data.loc[df_data['flag_prod'], 'Product'] = prod

        df_data = df_data.drop('flag_prod', axis=1)

    return df_data['Product']


def idetify_product_fr_prodclass(self, ar_data):
    # ar_data = list(df_data_install.ProductClass)

    # Input:
    col_ref = 'ProductClass'

    df_data = pd.DataFrame(data={'data': ar_data})

    # Reference Data
    ref_prod = pd.read_csv("./references/ref_product_class.csv")
    dict_rename = {
        'fpc_number': 'data',
        'Type': 'product_type',
        'Planning Type': 'product_prodclass'}
    ls_cols = list(dict_rename.values())
    ref_prod = ref_prod.rename(columns=dict_rename)
    ref_prod = ref_prod[ls_cols]
    for col in ls_cols:
        ref_prod.loc[:, col] = ref_prod[col].str.lower()

    ref_prod

    return df_data['Product']

def idetify_product_fr_tln(self, ar_tln):
    # ar_tln = list(df_bom.TLN_BOM)

    # Data to classify
    df_data = pd.DataFrame(data={'key': ar_tln})

    # Reference Data
    ref_prod = self.ref_lead_opp.copy()
    ref_prod = ref_prod[
        (ref_prod.flag_keep) & (pd.notna(ref_prod.PartNumber_TLN))]

    # Pre-Process Data
    ls_prod = ref_prod.Product.unique()
    df_data['Product'] = ''
    for prod in ls_prod:
        # prod = ls_prod [0]
        ls_pattern = ref_prod.loc[
            (ref_prod.Product == prod), 'PartNumber_TLN']

        df_data['flag_prod_beg'] = df_data.key.str.startswith(
            tuple(ls_pattern))
        df_data['flag_prod_con'] = df_data.key.apply(
            lambda x: re.search(f'({str.lower(prod)})', x) is not None)
        df_data['flag_prod'] = (df_data['flag_prod_beg']
                                | df_data['flag_prod_con'])

        df_data.loc[df_data['flag_prod'], 'Product'] = prod
        df_data = df_data.drop(
            ['flag_prod', 'flag_prod_beg', 'flag_prod_con'], axis=1)

    return df_data['Product']