from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
import pandas as pd

@task()
def load_fact_fo() -> pd.DataFrame:

    query = f"select distinct(order_id),to_char(order_date, 'YYYYMMDD') as date_id, tanggal,order_date, user_id, invoice_code, product_id, distributor_id, principal_id,productbrand_id,depo_id,subtotal,dpp,ppn,discount,golden_promo,total,total_final,total_dpp,total_ppn,total_voucher,total_discount,total_golden_promo from stg.stg_baru order by order_id desc;"
    
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_fact_fo = pd.read_sql(query, con=engine)
        df_fact_fo['date_id'] = df_fact_fo['date_id'].astype(int)
        df_fact_fo['tanggal'] = pd.to_datetime(df_fact_fo['tanggal']).dt.date 
        print(df_fact_fo)
        print(df_fact_fo.dtypes)
        print(df_fact_fo.dtypes)
        return df_fact_fo


@task()
def insert_fact_fo(table_fact_fo: str, schema_dm: str, df_fact_fo):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_fact_fo.to_sql(name=table_fact_fo, schema=schema_dm,
                            if_exists="replace", index=False, con=engine)

@task()
def load_dim_user() -> pd.DataFrame:

    query = f"select distinct(user_id) as id,customer, referral_by from stg.stg_orders order by id asc;"

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_user = pd.read_sql(query, con=engine)
        print(df_dim_user)
        print(df_dim_user.dtypes)

        return df_dim_user


@task()
def insert_dim_user(table_dim_user: str, schema_dm: str, df_dim_user):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_user.to_sql(name=table_dim_user, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)

@task()
def load_dim_product() -> pd.DataFrame:

    query = f"select distinct(product_id) as id, product_name,product_sku from stg.stg_orders order by id asc;"

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_product = pd.read_sql(query, con=engine)
        print(df_dim_product)
        print(df_dim_product.dtypes)

        return df_dim_product


@task()
def insert_dim_product(table_dim_product: str, schema_dm: str, df_dim_product):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_product.to_sql(name=table_dim_product, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)
        
@task()
def load_dim_product_brand() -> pd.DataFrame:

    query = f"select distinct(productbrand_id) as id, productbrand_name from stg.stg_baru where productbrand_id in (324,325,326,327,328,329,330,331,334,335,336,337,338,339,340,341,342,343,356,357,358,359,360,361,362,363,364,365,366,367,373,377) order by id asc;"
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_product_brand = pd.read_sql(query, con=engine)
        print(df_dim_product_brand)
        print(df_dim_product_brand.dtypes)
        return df_dim_product_brand


@task()
def insert_dim_product_brand(table_dim_product_brand: str, schema_dm: str, df_dim_product_brand):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_product_brand.to_sql(name=table_dim_product_brand, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)
@task()
def load_dim_product_brand_nothub() -> pd.DataFrame:

    query = f"select distinct(productbrand_id) as id, productbrand_name from stg.stg_baru where productbrand_id in (2,3,4,5,6,15,33,44,46,48,50,51,53,54,59,60,65,66,67,68,70,71,75,76,81,82,83,84,103,170,183,293,295,297,298,299,300,301,302,303,304,305,306,307,309,310,311,346,349,350,351,352,353,354,355,368,369) order by id asc;"
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_product_brand_nothub = pd.read_sql(query, engine)
        print(df_dim_product_brand_nothub)
        print(df_dim_product_brand_nothub.dtypes)
        return df_dim_product_brand_nothub

@task()
def insert_dim_product_brand_nothub(table_dim_product_brand_nothub: str, schema_dm: str, df_dim_product_brand_nothub):
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine: 
        df_dim_product_brand_nothub.to_sql(name=table_dim_product_brand_nothub, schema=schema_dm,
                              if_exists="replace", index=False,con=engine)
        
@task()
def load_dim_principal() -> pd.DataFrame:

    query = f"select distinct(principal_id) as id, principal_name,principal_code from stg.stg_orders order by id asc;"

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_principal = pd.read_sql(query, con=engine)
        print(df_dim_principal)
        print(df_dim_principal.dtypes)

        return df_dim_principal


@task()
def insert_dim_principal(table_dim_principal: str, schema_dm: str, df_dim_principal):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_principal.to_sql(name=table_dim_principal, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)
        
@task()
def load_dim_distributor() -> pd.DataFrame:

    query = f"select distinct(distributor_id) as id, distributor_name, CASE WHEN distributor_id IN (40, 41, 42, 43, 44) THEN 'Jatim' WHEN distributor_id IN (17, 18, 19, 21, 31) THEN 'Medan' WHEN distributor_id IN (22, 23, 24, 25, 26) THEN 'Palembang' WHEN distributor_id IN (33, 34, 35, 36, 37) THEN 'Botabek' END AS area from stg.stg_orders order by id asc;"

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_distributor = pd.read_sql(query, con=engine)
        print(df_dim_distributor)
        print(df_dim_distributor.dtypes)

        return df_dim_distributor


@task()
def insert_dim_distributor(table_dim_distributor: str, schema_dm: str, df_dim_distributor):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_distributor.to_sql(name=table_dim_distributor, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)

@task()
def load_dim_distributor_principal() -> pd.DataFrame:

    query = f"select distinct(distributor_id) as id, distributor_name, CASE WHEN distributor_id IN (1,2,3) THEN 'SNS' WHEN distributor_id = 7 THEN 'TNS' WHEN distributor_id = 12 THEN 'RKI' WHEN distributor_id = 13 THEN 'INC' WHEN distributor_id = 14 THEN 'MRI' WHEN distributor_id = 10 THEN 'WP' WHEN distributor_id = 27 THEN 'SYB' WHEN distributor_id = 30 THEN 'PRM' WHEN distributor_id = 32 THEN 'GAS' WHEN distributor_id = 39 THEN 'ARTA' ELSE NULL  END AS distributor_principal  FROM stg.stg_baru ORDER BY id ASC;"
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_distributor_principal = pd.read_sql(query, engine)
        print(df_dim_distributor_principal)
        print(df_dim_distributor_principal.dtypes)
        return df_dim_distributor_principal

@task()
def insert_dim_distributor_principal(table_dim_distributor_principal: str, schema_dm: str, df_dim_distributor_principal):
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_distributor_principal.to_sql(name=table_dim_distributor_principal, schema=schema_dm,
                              if_exists="replace", index=False,con=engine)

@task()
def load_dim_depo() -> pd.DataFrame:

    query = f"select distinct(depo_id) as id, depo_name from stg.stg_orders order by id asc;"

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_depo = pd.read_sql(query, con=engine)
        print(df_dim_depo)
        print(df_dim_depo.dtypes)

        return df_dim_depo


@task()
def insert_dim_depo(table_dim_depo: str, schema_dm: str, df_dim_depo):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False)as engine:
        df_dim_depo.to_sql(name=table_dim_depo, schema=schema_dm,
                              if_exists="replace", index=False, con=engine)

@task()
def load_dim_date() -> pd.DataFrame:

    query = '''SELECT DISTINCT(to_char(order_date, 'YYYYMMDD')) AS id,
    date(order_date) AS date, 
    RTRIM(TO_CHAR(generate_series(order_date::date, order_date::date, '1 day'::interval)::date, 'Day')) AS day_of_week, 
    EXTRACT(DAY FROM generate_series(order_date::date, order_date::date, '1 day'::interval)::date) AS day_of_month,
    EXTRACT(WEEK FROM generate_series(order_date::date, order_date::date, '1 day'::interval)::date) AS week_of_year,
    EXTRACT(WEEK FROM order_date) - EXTRACT(WEEK FROM DATE_TRUNC('MONTH', order_date)) + 1 AS week_in_month,
    EXTRACT(MONTH FROM generate_series(order_date::date, order_date::date, '1 day'::interval)::date) AS month, 
    EXTRACT(QUARTER FROM generate_series(order_date::date, order_date::date, '1 day'::interval)::date) AS quarter, 
    EXTRACT(YEAR FROM generate_series(order_date::date, order_date::date, '1 day'::interval)::date) AS year
 
    FROM stg.stg_orders;'''

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_date = pd.read_sql(query, con=engine)
        print(df_dim_date)
        print(df_dim_date.dtypes)
        df_dim_date['id'] = df_dim_date['id'].astype(int)
        df_dim_date['day_of_month'] = df_dim_date['day_of_month'].astype(int)
        df_dim_date['month'] = df_dim_date['month'].astype(int)
        df_dim_date['quarter'] = df_dim_date['quarter'].astype(int)
        df_dim_date['year'] = df_dim_date['year'].astype(int)
        df_dim_date['week_in_month'] = df_dim_date['week_in_month'].astype(int)
        print(df_dim_date)
        print(df_dim_date.dtypes)
        return df_dim_date

@task()
def insert_dim_date(table_dim_date: str, schema_dm: str, df_dim_date):

    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df_dim_date.to_sql(name=table_dim_date, schema=schema_dm,
                           if_exists="replace", index=False, con=engine)
        
@flow()
def insert_to_dm():
    schema_dm = "dm"

    """1. fact_fo"""
    df_fact_fo = load_fact_fo()
    table_fact_fo = "fact_fo"
    insert_fact_fo(table_fact_fo, schema_dm, df_fact_fo)
    
    """2. dim_user"""
    df_dim_user = load_dim_user()
    table_dim_user = "dim_user"
    insert_dim_user(table_dim_user, schema_dm, df_dim_user)
    
    """3. dim_product"""
    df_dim_product = load_dim_product()
    table_dim_product = "dim_product"
    insert_dim_product(table_dim_product, schema_dm, df_dim_product)
    
    """4. dim_product_brand"""
    df_dim_product_brand = load_dim_product_brand()
    table_dim_product_brand = "dim_product_brand"
    insert_dim_product_brand(table_dim_product_brand, schema_dm, df_dim_product_brand)
    
    """5. dim_product_brand_nothub"""
    df_dim_product_brand_nothub = load_dim_product_brand_nothub()
    table_dim_product_brand_nothub = "dim_product_brand_nothub"
    insert_dim_product_brand_nothub(table_dim_product_brand_nothub, schema_dm, df_dim_product_brand_nothub)
    
    """6. dim_principal"""
    df_dim_principal = load_dim_principal()
    table_dim_principal = "dim_principal"
    insert_dim_principal(table_dim_principal, schema_dm, df_dim_principal)

    """7. dim_distributor"""
    df_dim_distributor = load_dim_distributor()
    table_dim_distributor = "dim_distributor"
    insert_dim_distributor(table_dim_distributor, schema_dm, df_dim_distributor)
    
    """8. dim_distributor_principal"""
    df_dim_distributor_principal = load_dim_distributor_principal()
    table_dim_distributor_principal = "dim_distributor_principal"
    insert_dim_distributor_principal(table_dim_distributor_principal, schema_dm, df_dim_distributor_principal)
    
    """9. dim_depo"""
    df_dim_depo = load_dim_depo()
    table_dim_depo = "dim_depo"
    insert_dim_depo(table_dim_depo, schema_dm, df_dim_depo)
    
    """10. dim_date"""
    df_dim_date = load_dim_date()
    table_dim_date = "dim_date"
    insert_dim_date(table_dim_date, schema_dm, df_dim_date)
    
if __name__ == '__main__':
    insert_to_dm()