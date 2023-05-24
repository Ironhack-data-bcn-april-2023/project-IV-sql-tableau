import pandas as pd

# This function performs multiple transformations to clean the data.

def cleaning(df):
    df=df.drop('Column1', axis=1)
    df["Date"]= pd.to_datetime(df[ "Date"])
    df["day"]=df["Date"].dt.day
    df.columns.str.strip()
    df.columns=df.columns.str.lower()
    df=df.drop('index', axis=1)
    df = df.rename(columns={'sub category': 'sub_category'})
    df = df.rename(columns={'unit cost': 'unit_cost'})
    df = df.rename(columns={'customer age': 'customer_age'})
    df = df.rename(columns={'product category': 'product_category'})
    df = df.rename(columns={'customer gender': 'customer_gender'})
    df = df.rename(columns={'unit price': 'unit_price'})
    df['year'] = df['year'].fillna(0)
    df['year'] = df['year'].astype('int64')
    df['year'] = df['year'].fillna(0)
    df['year'] = df['year'].fillna(0).astype('int64')
    df['day'] = df['day'].fillna(0).astype('int64')
    df['customer_age'] = df['customer_age'].fillna(0).astype('int64')
    df['day_of_week'] = df['date'].dt.day_name()
    df=df[['date', 'year', 'month', 'day','day_of_week', 'customer_age', 'customer_gender', 'country', 'state', 'product_category', 'sub_category', 'quantity', 'unit_cost','unit_price', 'cost', 'revenue']]
    df = df.replace(["N/A", "None"], float("nan"))
    df.dropna()
    return df