import pandas_gbq as pd_gbq
import pandas as pd
import pyodbc

class SQLSERVERPOOL:

    def get_connection(self,db_name):
      try:
          pyodbc.drivers()
          conn = pyodbc.connect('DRIVER=SQL Server;SERVER=HYD-LAP-0370\SQLEXPRESS;DATABASE={0};Trusted_Connection=yes;'.format(db_name))

      except Exception as e:
          print(e)
      else:
          return conn

    def read_and_prepare_data(self,db_name,table,conn):
      try:
        query = "SELECT * FROM {0}.dbo.{1};".format(db_name,table)
        df = pd.read_sql(query,conn)

      except Exception as e:
        print(e)
      else:

        return df


    def load_into_bq(self,df,table_id,project_id,schema_json):
      try:
          pd_gbq.to_gbq(df,table_id,
                       project_id=project_id,
                       table_schema=schema_json,
                       if_exists='replace')


      except Exception as e:
          print(e)


