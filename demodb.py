import configparser
import os
import pandas_gbq as pd_gbq
import pyodbc
import pandas as pd
import logging

from datetime import date
logdate = date.today()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler(f"C:\\Users\\kprudhvee\\PycharmProjects\\Extractor_BD\\RPA_CMPC_DATABASE_MIGRATIONS\\{logdate}_logfilesDATABASE_MIGRATIONS_LOGS.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_connection(db_server, db_name, db_user, db_pass):
    try:
        logger.info(f"LOCAL SERVER DATABASE CONNECTION STARTED {db_name} ")
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (db_server, db_name, db_user, db_pass))
    except Exception as e:
        logger.exception(e)
    else:
        logger.info(f" SUCCESSFULLY CONNECTED LOCAL SERVER DATABASE {db_name}")
        return conn

def read_and_prepare_data(db_name,table):
    try:
        query2 = "SELECT  TOP 10 * from {0}.dbo.{1};".format(db_name, table)
        logger.info(f"GETTING TABLE DATA FROM DATABASE  {db_name}  AND TABLE {table}")
        df = pd.read_sql(query2, conn)

        df = df.rename(columns={"Descripción": "Descripcion", "Mes_Año": "Mes_Ano","[3PL]":"3PL","[3PLAlexandria]":"3PLAlexandria","[3PLBoise]":"3PLBoise","[3PLPlantas]":"3PLPlantas","[3PLProductos]":"3PLProductos","[3PLReportConsolidado]":"3PLReportConsolidado","[3PLSKU]":"3PLSKU","[3PLWoodgrain]":"3PLWoodgrain","DesviaciónDiasEntrega":"DesviacinDiasEntrega","DesviaciónDiasZarpe":"DesviacionDiasZarpe","DesviaciónDiasProduccion":"DesviacionDiasProduccion","DesviaciónDiasTransitoEnAgua":"DesviacionDiasTransitoEnAgua"}, inplace=False)
    except Exception as e:
        logger.exception(e)
    else:
        logger.info(f"SUCCESSFULY GOT TABLE  DATA FROM DATABASE  {db_name}  AND TABLE {table}")
        return df

def load_into_bq(df, table_id,project_id,schema_json):
    try:
        logger.info(f"LOADING TABLE DATA TO BIGQUERY TABLE_ID {table_id}")
        pd_gbq.to_gbq(df, table_id,
                      project_id=project_id,
                      table_schema=schema_json,
                      if_exists='replace')
        logger.info(f"SUCCESSFULLY LOADED TABLE DATA TO BIGQUERY TABLE_ID {table_id}")
    except Exception as e:
        logger.exception(e)

### CREATING CONFIG OBJECT FOR CALLING EVERY CONFIG FILES
config = configparser.ConfigParser()
###reading databases list config file
try:
    config.read("C:\\Users\\kprudhvee\\PycharmProjects\\Extractor_BD\\RPA_CMPC_DATABASE_MIGRATIONS\\config_folders\\CONFIG_DATABASES.ini")
except FileNotFoundError as e:
    logger.exception(e)

databases = config['DATABASES']['databases']
databases_list=databases.split(",")

for database in databases_list:
    logger.info(f"started {database}")
    logger.info("========================================================================")

    try:
        logger.info(f"GETTING CONFIGORATION FILE FOR DATABASE {database}")
        config.read(f'C:\\Users\\kprudhvee\\PycharmProjects\\Extractor_BD\\RPA_CMPC_DATABASE_MIGRATIONS\\config_folders\\CONFIG_{database}.ini')
        logger.info(f"SUCEESSFULLY LOADED CONFIGORATION FILE FOR DATABASE {database}")
    except FileNotFoundError as e:
        logger.exception(e)
    ### CALLING SCHEMA CSV FILE AND CONVERTING TO DATAFRAME
    try:
        logger.info(f"GETIING SCHEMA FILE FOR DATABASE {database}")
        database_config_df = pd.read_csv(f"C:\\Users\\kprudhvee\\PycharmProjects\\Extractor_BD\\RPA_CMPC_DATABASE_MIGRATIONS\\data_files\\{database}.CSV")
        logger.info(f"SUCEESSFULLY LOADED SCHEMA FILE FOR DATABASE {database}")
    except FileNotFoundError as e:
        logger.exception(e)
    db_user=config['Server_Credentials']['DB_USER']
    db_pass=config['Server_Credentials']['DB_PASSWORD']
    db_server=config['Server_Credentials']['DB_HOST']

    project_id =config['GOOGLE']['PROJECT_ID']
    dataset_id = config['GOOGLE']['DATASET_ID']

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['GOOGLE']['CREDENTIALS']

    db_name=config['Database']['db_name']
    table_names = config['Database']['table_names']
    table_names_list=table_names.split(",")


    conn=get_connection(db_server, db_name, db_user, db_pass)
    cursor = conn.cursor()


    for table in table_names_list:

        print(table)
        df=read_and_prepare_data(db_name,table)



        #CONVERTING SCHEMA DF INTO JSON OBJECT
        get_unique_columns = pd.unique(database_config_df['table'])
        demo_df = database_config_df[database_config_df['table'] == table]
        schema_df = demo_df[["name", "type", "mode"]]
        schema_json = schema_df.to_dict('records')


        # WRITING TO BIGQUERY
        table_id = '{0}.{1}'.format(dataset_id,table)
        load_into_bq(df, table_id,project_id,schema_json)
    logger.info("========================================================================")