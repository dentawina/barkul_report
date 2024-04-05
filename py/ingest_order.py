from prefect import flow, task
from sqlalchemy import create_engine
import pytz
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from prefect_sqlalchemy import SqlAlchemyConnector

@task(retries=3, name="***Fetching Data from MsSQL***", log_prints=True)
def fetching_data(yesterday: str, yesterday_7: str) -> pd.DataFrame:
    server = '10.40.16.19'
    database = 'MSIDo'
    username = 'login_fadhil'
    password = 'RTvT9XBtcbjO'
    TrustServerCertificate = 'yes'

    engine = create_engine(
        f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.3.so.2.1&TrustServerCertificate={TrustServerCertificate}")

    query = (f"""
        SELECT 
        FORMAT(o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'SE Asia Standard Time','yyyy-MM-dd') AS tanggal,
        o.created_at as order_date,
        o.id as order_id,
        o.invoice_code,
        o.total,
        o.total_dpp,
        o.total_ppn,
	    total_voucher,
        opd.subtotal,
        u.id as user_id,
        CONCAT(up.first_name ,' ', up.last_name) as customer,
        TRIM(UPPER(u.referral_by)) AS referral_by,
        p.id as product_id,
        p.name as product_name,
        p.sku as product_sku,
        pb.id as productbrand_id,
        pb.name as productbrand_name,
        d.id as depo_id,
        d.name as depo_name,
        prin.id as principal_id,
        prin.name as principal_name,
        prin.code as principal_code,
        dist.id  as distributor_id,
        dist.name as distributor_name
    from orders o
        left join order_productdetail opd
        on o.id = opd.order_id
        left join users u
        on u.id = o.user_id
        left join users_profiles up
        on u.id = up.user_id
        left join products_details pd
        on pd.id = opd.products_detail_id 
        left join products p
        on p.id = pd.product_id 
        left join products_brands pb
        on pb.id = p.products_brand_id
        LEFT join depo d
        on d.id = o.depo_id
        left join principals prin
        on prin.id = pb.principal_id
        left join distributors dist
        on dist.id = prin.distributor_id
    where dist.name like 'HUB%'
    AND CONVERT(date, o.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'SE Asia Standard Time') = CONVERT(date, DATEADD(day, -1, GETDATE()))
    order by o.created_at ASC;"""
    )
    
    df = pd.read_sql(query, engine)
    df['tanggal'] = pd.to_datetime(df['tanggal']).dt.date 
    print(f"fetching data :\n {df}")
    print(f"fetching data :\n {df.dtypes}")
    return df


@task(name="***Write to Parquet***", log_prints=True)
def write_parquet(df: pd.DataFrame, dataset_file: str) -> Path:
    path = Path(f"/home/ubuntu/work/bakul_report/dwh/data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@flow()
def ingest_orders():
    timezone = pytz.timezone('Asia/Jakarta')
    today = datetime.now(timezone).date()
    yesterday = today + timedelta(-1)
    yesterday_7 = yesterday + timedelta(-6)
    df = fetching_data(yesterday, yesterday_7)
    dataset_file = f"order_{yesterday}"
    write_parquet(df, dataset_file)


@task()
def read_parquet(date_key: str) -> pd.DataFrame:
    yesterday = datetime.now() - timedelta(days=1)
    date_keys = yesterday.strftime("%Y-%m-%d")
    path = (f"/home/ubuntu/work/bakul_report/dwh/data/order_{date_keys}.parquet")
    df_new = pd.read_parquet(path)
    print(df_new.dtypes)
    print(df_new)
    return df_new


@task()
def read_existing(df_new) -> pd.DataFrame:
    try:
        connection_block = SqlAlchemyConnector.load("pg-database")
        with connection_block.get_connection(begin=False) as engine:
            df_existing = pd.read_sql_table(
                'stg_orders', schema='stg', con=engine)
            print(df_existing.dtypes)
            print(f"df_existing :\n", df_existing)

            return df_existing
    except ValueError:
        print(f"df_new :\n", df_new)
        return df_new


@task()
def merge_data(df_new, df_existing) -> pd.DataFrame:
    df_inner = df_existing[~df_existing['order_id'].isin(df_new)]
    df_new.merge(df_existing, on=['order_id', 'product_id'], how='left')
    df_concat = pd.concat([df_inner, df_new], ignore_index=True)
    df = df_concat.drop_duplicates(
        subset=['order_id', 'product_id'], keep='last')
    print(df)
    return df


@task()
def insert_to_table(table_name: str, schema_name: str, df):
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, schema=schema_name, con=engine,
                  if_exists='replace', index=False)
   
@flow()
def insert_to_db():
    yesterday = datetime.now() - timedelta(days=1)
    date_keys = yesterday.strftime("%Y-%m-%d")
    df_new = read_parquet(date_keys)
    df_existing = read_existing(df_new)
    df = merge_data(df_new, df_existing)
    table_name = "stg_orders"
    schema_name = "stg"
    insert_to_table(table_name, schema_name, df)


if __name__ == '__main__':
    ingest_orders()
    insert_to_db()