import pandas_gbq as pd_gbq
import pandas as pd
import pymysql


class MSQLPOOl:
  def get_connection(self,db_host,db_user,db_pass):
    try:
      conn = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_pass,
      )
    except Exception as e:
      print(e)
    else:
      return conn

  def read_and_prepare_data(self,db_name,table,conn):
    try:
      query = "SELECT * FROM {0}.{1} ;".format(db_name,table)
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


