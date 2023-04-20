import datetime
import os
import pandas as pd
import sqlalchemy
import argparse
from tqdm import tqdm


def date_range(dt_start, dt_stop, period='daily'):

    date_start = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    date_stop = datetime.datetime.strptime(dt_stop, '%Y-%m-%d')
    dates = []
    while date_start <= date_stop:
        dates.append(date_start.strftime("%Y-%m-%d"))
        date_start += datetime.timedelta(days=1)
    
    if period == 'daily':
        return dates

    elif period == 'monthly':
        return [i for i in dates if i.endswith('01')]


class Ingestor:

    def __init__(self, path, table, key_field):
        self.path = path
        self.engine = self.create_db_engine()
        self.table = table
        self.key_field = key_field


    def create_db_engine(self):
        return sqlalchemy.create_engine(f"sqlite:///{self.path}")


    def import_query(self, path):
        with open(path, 'r') as open_file:
            return open_file.read()


    def table_exists(self):
        with self.engine.connect() as connection:
            tables = sqlalchemy.inspect(connection).get_table_names()
            return self.table in tables


    def execute_etl(self, query):
        with self.engine.connect() as connection:
            df = pd.read_sql_query(query, connection)
        return df


    def insert_table(self, df):
        with self.engine.connect() as connection:
            df.to_sql(self.table, connection, if_exists='append', index=False)
        return True


    def delete_table_rows(self, value):
        sql = f"DELETE FROM {self.table} where {self.key_field}='{value}';"
        with self.engine.connect() as connection:
            connection.execute(sqlalchemy.text(sql))
            connection.commit()
        return True
    

    def update_table(self, raw_query, value):
        
        if self.table_exists():
            self.delete_table_rows(value)
                
        df = self.execute_etl(raw_query.format(date=value))

        self.insert_table(df)


def main():

    ETL_DIR = os.path.dirname(os.path.abspath(__file__))
    LOCAL_DEV_DIR = os.path.dirname(ETL_DIR)
    ROOT_DIR = os.path.dirname(LOCAL_DEV_DIR)
    DATA_DIR = os.path.join(ROOT_DIR, 'data')
    DB_PATH = os.path.join(DATA_DIR, 'olist.db')

    parser = argparse.ArgumentParser()
    parser.add_argument("--table", type=str)
    parser.add_argument("--dt_start", type=str)
    parser.add_argument("--dt_stop", type=str)
    parser.add_argument("--dt_period", type=str)
    args = parser.parse_args()

    dates = date_range(args.dt_start, args.dt_stop, args.dt_period)

    ingestor = Ingestor(DB_PATH, args.table, 'dtReference')
    query_path = os.path.join(ETL_DIR,f"{args.table}.sql")
    query = ingestor.import_query(query_path)

    for i in tqdm(dates):
        ingestor.update_table(query, i)


if __name__ == "__main__":
    main()