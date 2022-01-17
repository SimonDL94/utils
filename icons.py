'''*adding icons font type icons ✅/❌/⚠️*'''

def add_boolean_mapping(df):
    number_suffix = '_mapping'
    for i in df.columns:
        if df[i].dtype == 'bool':
            df[i + number_suffix] = '❌'
            df.loc[(df[i] == True),  i + number_suffix] = '✅'
    return df

def add_number_mapping(df, number_column):
    number_suffix = '_mapping'
    df.loc[(df[number_column] == 0),  number_column + number_suffix] = '❌'
    df.loc[(df[number_column] == 1),  number_column + number_suffix] = '⚠️'
    df.loc[(df[number_column] == 2),  number_column + number_suffix] = '✅'
    return df

def add_positive_negative_mapping(df, number_column):
    number_suffix = '_mapping'
    df.loc[(df[number_column] < 0),  number_column + number_suffix] = '❌'
    df.loc[(df[number_column] >= 0),  number_column + number_suffix] = '✅'
    return df