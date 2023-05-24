import pymysql
import sqlalchemy as alch # python -m pip install --upgrade 'sqlalchemy<2.0'

from getpass import getpass
import pandas as pd
from sqlalchemy import create_engine, text
import time
from dotenv import load_dotenv
import os




def connect(df):
    load_dotenv()
    password = os.getenv("password")
    
    dbName = "retail_sales"
    connectionData=f"mysql+pymysql://root:{password}@localhost/{dbName}"
    engine = alch.create_engine(connectionData)

    
    # Loading to the new info
    table = "retail_sales"
    df.to_sql(table, con=engine, if_exists='replace', index=False)
    
    return engine





def yearly_sales(engine):
    
    drop= "DROP VIEW IF EXISTS yearly_sales;"
    select= "SELECT * from yearly_sales"
    query = """
    CREATE VIEW yearly_sales AS SELECT year, sub_category, SUM(quantity) AS total_quantity FROM retail_sales
    GROUP BY year, sub_category
ORDER BY sub_category;"""



    
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)




def monthly_sales(engine):
    drop= "DROP VIEW IF EXISTS monthly_sales;"
    select= "SELECT * from monthly_sales"
    query = """
    


    CREATE VIEW monthly_sales AS SELECT sub_category, month, SUM(quantity) AS total_quantity FROM retail_sales
    WHERE year=2016
    GROUP BY sub_category, month
    ORDER by sub_category
    LIMIT 15;



    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)



def yearly_growth(engine):
    drop= "DROP VIEW IF EXISTS yearly_growth;"
    select= "SELECT * from yearly_growth"
    query = """
    


    CREATE VIEW yearly_growth AS SELECT
    sub_category,
    SUM(CASE WHEN year = 2015 THEN total_quantity ELSE 0 END) AS total_quantity_2015,
    SUM(CASE WHEN year = 2016 THEN total_quantity ELSE 0 END) AS total_quantity_2016,
    100 * (SUM(CASE WHEN year = 2016 THEN total_quantity ELSE 0 END) - SUM(CASE WHEN year = 2015 THEN total_quantity ELSE 0 END))
    / SUM(CASE WHEN year = 2015 THEN total_quantity ELSE 0 END) AS sales_growth
FROM
    (
        SELECT
            year,
            sub_category,
            SUM(quantity) AS total_quantity
        FROM
            retail_sales
        WHERE
            year IN (2015, 2016)
        GROUP BY
            year,
            sub_category
    ) AS subquery
GROUP BY
    sub_category;



    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)




def total_daily_sales(engine):
    drop= "DROP VIEW IF EXISTS total_daily_sales;"
    select= "SELECT * from total_daily_sales"
    query = """
    


    CREATE VIEW total_daily_sales AS SELECT day_of_week, SUM(quantity) AS total_quantity FROM retail_sales
    WHERE year=2016
    GROUP BY day_of_week


    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)



def unitary_profit_by_sub_product(engine):
    drop= "DROP VIEW IF EXISTS unitary_profit_by_sub_product;"
    select= "SELECT * from unitary_profit_by_sub_product"
    query = """
    


    CREATE VIEW unitary_profit_by_sub_product AS SELECT sub_category, AVG(unit_price-unit_cost) AS profit FROM retail_sales
    GROUP BY sub_category
    ORDER BY profit DESC


    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)



def profit_by_state(engine):
    drop= "DROP VIEW IF EXISTS profit_by_state;"
    select= "SELECT * from profit_by_state"
    query = """
    


    CREATE VIEW profit_by_state AS 
    SELECT rs.state, SUM(rs.revenue - rs.cost) AS profit
    FROM retail_sales rs
    JOIN (
        SELECT state
        FROM retail_sales
        GROUP BY state
        HAVING COUNT(*) > 20
    ) subq ON rs.state = subq.state
    GROUP BY rs.state
    ORDER BY profit DESC;
       


    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)






def profit_margin_state(engine):
    drop= "DROP VIEW IF EXISTS profit_margin_state;"
    select= "SELECT * from profit_margin_state"
    query = """
    


    CREATE VIEW profit_margin_state AS SELECT rs.state, SUM(rs.revenue-rs.cost)*100/SUM(revenue) AS margin FROM retail_sales rs
    JOIN (
        SELECT state
        FROM retail_sales
        GROUP BY state
        HAVING COUNT(*) > 20
    ) subq ON rs.state = subq.state
   
    GROUP BY rs.state
    ORDER BY margin DESC
    


    """
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)
        time.sleep(1)
        

    return pd.read_sql_query(select, engine)


def segmentation(engine):
    
    drop= "DROP VIEW IF EXISTS segmentation;"
    select= "SELECT * from segmentation"
    query = """
    CREATE VIEW segmentation AS SELECT sub_category, avg(customer_age) AS average_age FROM retail_sales
    GROUP BY sub_category
    ORDER BY sub_category;"""



    
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)



def segmentation_2(engine):
    
    drop= "DROP VIEW IF EXISTS segmentation_2;"
    select= "SELECT * from segmentation_2"
    query = """
    CREATE VIEW segmentation_2 AS
SELECT sub_category,
       COUNT(CASE WHEN customer_gender = 'F' THEN customer_gender END) AS total_women,
       COUNT(CASE WHEN customer_gender = 'M' THEN customer_gender END) AS total_men
FROM retail_sales
GROUP BY sub_category
ORDER BY sub_category;"""



    
    drop_view = text(drop)
    create_view = text(query)



    with engine.connect() as connection:
        connection.execute(drop_view)
        connection.execute(create_view)

    return pd.read_sql_query(select, engine)


    

