from sqlalchemy import create_engine
from utils.utility import get_mysql_connection
import pandas as pd

class Database():
    def __init__(self, conf):
        self.conf           = conf
    
    def initialize_db(self):
        self.create_db()
        self.generate_tables()

    def create_db(self):
        sql_conf = self.conf.get('mysql')

        user     = sql_conf["user"]
        password = sql_conf["password"]
        host     = sql_conf["host"]
        db       = sql_conf["database"]

        #Check if Database is there, deletes it
        try:
            engine_string = f'mysql+pymysql://{user}:{password}@{host}'
            engine = create_engine(engine_string)
            drop_db_sql = f'DROP DATABASE IF EXISTS {db};'
        except Exception as e:
            print(e)
            engine_string = f"mysql+pymysql://{user}:{password}@{host}/{db}"
            engine = create_engine(engine_string)

        with engine.connect() as con:
            con.execute(drop_db_sql)
            con.execute(f"CREATE DATABASE {db};")
            con.execute(f"USE {db};")

    def check_if_db_exists(self):
        sqlConf = self.conf.get('mysql')
        #Check if Database is there, deletes it
        try:
            df = self.get_df_from_server(f'''show databases like '{sqlConf["database"]}' ''')
            if len(df)>0:
                return True
            return False
        except:
            return False

    def get_df_from_server(self,sql):
        with get_mysql_connection(self.conf) as con:
            return pd.read_sql(sql,con)
    
    def push_sql_to_server(self,sql):
        with get_mysql_connection(self.conf) as con:
            return con.execute(sql)
        
    def push_new_df_to_server(self,df,tableName):
        with get_mysql_connection(self.conf) as con:
            df.to_sql(name=tableName,con=con,index=False,if_exists='replace')
        