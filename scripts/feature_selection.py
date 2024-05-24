import pandas as pd
from sklearn.model_selection import train_test_split
import time

from services.kafka import kafka_producer


def load_data():
    df_2015 = pd.read_csv("../data/2015.csv", delimiter=",")
    df_2016 = pd.read_csv("../data/2016.csv", delimiter=",")
    df_2017 = pd.read_csv("../data/2017.csv", delimiter=",")
    df_2018 = pd.read_csv("../data/2018.csv", delimiter=",")
    df_2019 = pd.read_csv("../data/2019.csv", delimiter=",")
    return df_2015, df_2016, df_2017, df_2018, df_2019

def rename_columns(df, column_mapping):
    df.rename(columns=column_mapping, inplace=True)

def concat_datasets(df_list):
    df_happiness = pd.concat(df_list, ignore_index=True, join="inner")
    return df_happiness

def add_year_column(df_list, years):
    for i, df in enumerate(df_list):
        df['year'] = years[i]

def replace_outliers_with_iqr(df, multiplier=1.5):
    numeric_cols = df.select_dtypes(include=['number'])
    Q1 = numeric_cols.quantile(0.25)
    Q3 = numeric_cols.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    df = df.copy()
    for col in numeric_cols.columns:
        df[col] = df[col].mask((df[col] < lower_bound[col]) | (df[col] > upper_bound[col]), Q1[col] + IQR[col])
    return df

def drop_columns(df, columns_to_drop):
    df.drop(columns=columns_to_drop, inplace=True)


# Define column mappings for each dataset
column_mapping1 = {
    'Country': 'country',
    'Region': 'region',
    'Happiness Rank': 'happiness_rank',
    'Happiness Score': 'happiness_score',
    'Economy (GDP per Capita)': 'gdp_per_capita',
    'Family': 'social_family_support',
    'Health (Life Expectancy)': 'health_life_expectancy',
    'Freedom': 'freedom',
    'Trust (Government Corruption)': 'perceptions_corruption',
    'Generosity': 'generosity',
    'Dystopia Residual': 'dystopia_residual'
}

column_mapping2 = {
    'Country': 'country',
    'Happiness.Rank': 'happiness_rank',
    'Happiness.Score': 'happiness_score',
    'Economy..GDP.per.Capita.': 'gdp_per_capita',
    'Family': 'social_family_support',
    'Health..Life.Expectancy.': 'health_life_expectancy',
    'Freedom': 'freedom',
    'Generosity': 'generosity',
    'Trust..Government.Corruption.': 'perceptions_corruption',
    'Dystopia.Residual': 'dystopia_residual'
}

column_mapping3 = {
    'Overall rank': 'happiness_rank',
    'Country or region': 'country',
    'Score': 'happiness_score',
    'GDP per capita': 'gdp_per_capita',
    'Social support': 'social_family_support',
    'Healthy life expectancy': 'health_life_expectancy',
    'Freedom to make life choices': 'freedom',
    'Generosity': 'generosity',
    'Perceptions of corruption': 'perceptions_corruption'
}


def transform_data():
    df_2015, df_2016, df_2017, df_2018, df_2019 = load_data()
    rename_columns(df_2015, column_mapping1)
    rename_columns(df_2016, column_mapping1)
    rename_columns(df_2017, column_mapping2)
    rename_columns(df_2018, column_mapping3)
    rename_columns(df_2019, column_mapping3)
    add_year_column([df_2015, df_2016, df_2017, df_2018, df_2019], [2015, 2016, 2017, 2018, 2019])
    df_happiness = concat_datasets([df_2015, df_2016, df_2017, df_2018, df_2019])
    df_happiness.dropna(inplace=True)
    df_happiness = replace_outliers_with_iqr(df_happiness)
    drop_columns(df_happiness, ["country", "year", "happiness_rank"])
    return df_happiness


def feature_selection(df):
    features = [
        "gdp_per_capita",
        "social_family_support",
        "health_life_expectancy",
        "life_expectancy",
        "freedom",
        "perceptions_corruption",
        "generosity",
        "happiness_score"
    ]
    df_happiness = df[features]
    y = df_happiness["happiness_score"]
    X = df_happiness.drop(columns=["happiness_score"])
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    return df.loc[X_test.index]



if __name__ == "__main__":
    df_happiness = transform_data()
    df_happiness = feature_selection(df_happiness)
    for index, row in df_happiness.iterrows():
        kafka_producer(row)
        time.sleep(1)
