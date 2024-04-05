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
    u.id AS user_id,
    u.bosnet_id,
    u.phone,
    CONCAT(up.first_name, ' ', up.last_name) AS name,
    up.address,
    pc.kode_pos,
    pc.kelurahan,
    pc.kecamatan,
    r.name AS kabupaten,
    p.name AS provinsi,
    TRIM(UPPER(u.referral_by)) AS referral_by,
    CAST(up.location.STX AS NVARCHAR(MAX)) + ', ' + CAST(up.location.STY AS NVARCHAR(MAX)) AS longlat,
    FORMAT(u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'SE Asia Standard Time', 'yyyy-MM-dd HH:mm:ss') AS tanggal_regist 
    FROM users u
    INNER JOIN users_profiles up ON u.id = up.user_id
    LEFT JOIN postal_codes pc ON pc.id = up.postal_code_id 
    LEFT JOIN regencies r ON r.id = pc.regency_id 
    LEFT JOIN provinces p ON p.id = pc.province_id 

    WHERE CONVERT(date, u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'SE Asia Standard Time') = CONVERT(date, DATEADD(day, -1, GETDATE()))
    ORDER BY u.created_at ASC;""")
    df = pd.read_sql(query, engine)
    df['tanggal_regist'] = pd.to_datetime(df['tanggal_regist']).dt.date
    print(f"fetching data :\n {df}")
    print(f"fetching data :\n {df.dtypes}")
    return df

@task(name="***Write to Parquet***", log_prints=True)
def write_parquet(df: pd.DataFrame, dataset_file: str) -> Path:
    path = Path(f"/home/ubuntu/work/bakul_report/dwh/data/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@flow()
def ingest_user():
    timezone = pytz.timezone('Asia/Jakarta')
    today = datetime.now(timezone).date()
    yesterday = today + timedelta(-1)
    yesterday_7 = yesterday + timedelta(-6)
    df = fetching_data(yesterday, yesterday_7)
    dataset_file = f"user_{yesterday}"
    write_parquet(df, dataset_file)


@task()
def read_parquet(date_key: str) -> pd.DataFrame:
    yesterday = datetime.now() - timedelta(days=1)
    date_keys = yesterday.strftime("%Y-%m-%d")
    path = (f"/home/ubuntu/work/bakul_report/dwh/data/user_{date_keys}.parquet")
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
                'stg_user', schema='stg', con=engine)
            print(df_existing.dtypes)
            print(f"df_existing :\n", df_existing)

            return df_existing
    except ValueError:
        print(f"df_new :\n", df_new)
        return df_new


@task()
def merge_data(df_new, df_existing) -> pd.DataFrame:
    df_inner = df_existing[~df_existing['user_id'].isin(df_new)]
    df_new.merge(df_existing, on=['user_id','bosnet_id'], how='left')
    df_concat = pd.concat([df_inner, df_new], ignore_index=True)
    df = df_concat.drop_duplicates(
        subset=['user_id','bosnet_id'], keep='last')
    print(df)
    return df


@task()
def insert_to_table(table_name: str, schema_name: str, df):
    connection_block = SqlAlchemyConnector.load("pg-database")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, schema=schema_name, con=engine,
                  if_exists='replace', index=False)
   
@flow()
def insert_to_postgres_user():
    yesterday = datetime.now() - timedelta(days=1)
    date_keys = yesterday.strftime("%Y-%m-%d")
    df_new = read_parquet(date_keys)
    df_existing = read_existing(df_new)
    df = merge_data(df_new, df_existing)
    table_name = "stg_user"
    schema_name = "stg"
    insert_to_table(table_name, schema_name, df)


if __name__ == '__main__':
    ingest_user()
    insert_to_postgres_user()