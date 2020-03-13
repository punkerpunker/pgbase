import pandas as pd


def join_tables(df1, df2, lst_column='indexes'):
    df3 = pd.DataFrame({col: np.repeat(df1[col].values, df1[lst_column].str.len())
      for col in df1.columns.drop(lst_column)}).assign(**{
        lst_col: np.concatenate(df1[lst_column].values)})[df1.columns]
    df3 = df3.merge(df2, left_on=lst_column, right_index=True, how='inner')
    df3 = df3.sort_index()
    return df3
