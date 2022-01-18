from flask import Flask, flash, redirect, render_template, \
     request, url_for
from onpremisesdatabases.mysql1 import MSQLPOOl
from onpremisesdatabases.postgres1 import POSTGRESPOOL
from onpremisesdatabases.sqlserver1 import SQLSERVERPOOL
import configparser
import pandas as pd
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pickle

app = Flask(__name__)

@app.route('/')
def index():
    return render_template(
        'index.html',
        data=[{'name':'MY SQL'}, {'name':'POSTGRES'}, {'name':'SQL SERVER'}])

@app.route("/test" , methods=['GET', 'POST'])
def test():
    config = configparser.ConfigParser()
    select = request.form.get('comp_select')
    list1=[]
    if select == "MY SQL":
        mysqlpool = MSQLPOOl()
        try:
            config.read("C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\db_config.ini")
        except FileNotFoundError as e:
            print(e)

        databases = config['DATABASES']['mysql_db_list']
        db_lists = databases.split(",")  # ['db1',"db2"]

        for db in db_lists:

            try:

                config.read(f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\{db}_config.ini")

            except FileNotFoundError as e:
                print(e)

            try:

                schema_df = pd.read_csv(
                    f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\schemafiles\\{db}_config.csv")

            except FileNotFoundError as e:
                print(e)

            db_user = config['Server_Credentials']['db_user']
            db_pass = config['Server_Credentials']['db_password']
            db_server = config['Server_Credentials']['db_host']

            db_name = config['Database']['db_name']
            table_name = config['Database']['table_names']
            table_names_list = table_name.split(",")
            print(table_names_list)
            conn = mysqlpool.get_connection(db_server, db_user, db_pass)
            cursor = conn.cursor()
            len_table = len(table_names_list)
            # list1 = []
            for table in table_names_list:
                print(table)

                df = mysqlpool.read_and_prepare_data(db_name, table, conn)

                project_id = config['Google']['PROJECT_ID']
                dataset_id = config['Google']['DATASET_ID']

                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['Google']['CREDENTIALS']
                get_unique_columns = pd.unique(schema_df['table'])
                demo_df = schema_df[schema_df['table'] == table]
                schema_df1 = demo_df[['name', 'type', 'mode']]
                schema_json = schema_df1.to_dict('records')

                table_id = '{0}.{1}'.format(dataset_id, table)
                mysqlpool.load_into_bq(df, table_id, project_id, schema_json)
                print(f"loaded successfully {table}")
                # status=True
                client = bigquery.Client()
                try:
                    client.get_table(table_id)
                    status = True
                except NotFound:
                    status = False

                list = [select, db_name, table, status]
                list1.append(list)

            print(list1)
            filename = 'my_data'
            outfile = open(filename, 'wb')
            pickle.dump(list1, outfile)
            outfile.close()


    elif select == "POSTGRES":
        postgrespool = POSTGRESPOOL()
        try:
            config.read("C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\db_config.ini")
        except FileNotFoundError as e:
            print(e)

        databases = config['DATABASES']['postgres_db_list']
        db_lists = databases.split(",")

        for db in db_lists:

            try:

                config.read(f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\{db}_config.ini")


            except FileNotFoundError as e:
                print(e)

            try:

                schema_df = pd.read_csv(
                    f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\schemafiles\\{db}_config.csv")

            except FileNotFoundError as e:
                print(e)

            db_user = config['Server_Credentials']['db_user']
            db_pass = config['Server_Credentials']['db_password']
            db_server = config['Server_Credentials']['db_host']

            db_name = config['Database']['db_name']
            table_name = config['Database']['table_names']
            table_names_list = table_name.split(",")

            conn = postgrespool.get_connection(db_server, db_user, db_pass, db_name)
            cursor = conn.cursor()

            for table in table_names_list:
                print(table)

                df = postgrespool.read_and_prepare_data(table,conn)


                project_id = config['Google']['PROJECT_ID']
                dataset_id = config['Google']['DATASET_ID']

                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['Google']['CREDENTIALS']
                get_unique_columns = pd.unique(schema_df['table'])
                demo_df = schema_df[schema_df['table'] == table]
                schema_df1 = demo_df[['name', 'type', 'mode']]
                schema_json = schema_df1.to_dict('records')

                table_id = '{0}.{1}'.format(dataset_id, table)
                postgrespool.load_into_bq(df, table_id, project_id, schema_json)
                print(f"loaded successfully {table}")
                client = bigquery.Client()
                try:
                    client.get_table(table_id)
                    status = True
                except NotFound:
                    status = False

                list = [select, db_name, table, status]
                list1.append(list)

            print(list1)
            filename = 'my_data'
            outfile = open(filename, 'wb')
            pickle.dump(list1, outfile)
            outfile.close()

    elif select == "SQL SERVER":
        sqlserverpool=SQLSERVERPOOL()
        try:
            config.read("C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\db_config.ini")
        except FileNotFoundError as e:
            print(e)

        databases = config['DATABASES']['sqlserver_db_list']
        db_lists = databases.split(",")

        for db in db_lists:

            try:

                config.read(f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\configure\\{db}_config.ini")


            except FileNotFoundError as e:
                print(e)

            try:

                schema_df = pd.read_csv(
                    f"C:\\Users\\mfatima\\PycharmProjects\\flaskdatabasemigrations\\schemafiles\\{db}_config.csv")

            except FileNotFoundError as e:
                print(e)


            db_name = config['Database']['db_name']
            table_name = config['Database']['table_names']
            table_names_list = table_name.split(",")

            conn = sqlserverpool.get_connection(db_name)
            cursor = conn.cursor()

            for table in table_names_list:
                print(table)

                df = sqlserverpool.read_and_prepare_data(db_name,table,conn)


                project_id = config['Google']['PROJECT_ID']
                dataset_id = config['Google']['DATASET_ID']

                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['Google']['CREDENTIALS']
                get_unique_columns = pd.unique(schema_df['table'])
                demo_df = schema_df[schema_df['table'] == table]
                schema_df1 = demo_df[['name', 'type', 'mode']]
                schema_json = schema_df1.to_dict('records')

                table_id = '{0}.{1}'.format(dataset_id, table)
                sqlserverpool.load_into_bq(df, table_id, project_id, schema_json)
                print(f"loaded successfully {table}")
                client = bigquery.Client()
                try:
                    client.get_table(table_id)
                    status = True
                except NotFound:
                    status = False

                list = [select, db_name, table, status]
                list1.append(list)

            print(list1)
            filename = 'my_data'
            outfile = open(filename, 'wb')
            pickle.dump(list1, outfile)
            outfile.close()
    return(redirect(url_for("thankyou",items=list1)))


@app.route('/thankyou',methods=['GET', 'POST'])
def thankyou():
    infile = open("my_data", 'rb')
    list = pickle.load(infile)
    infile.close()
    return render_template("thankyou.html",items=list)


if __name__=='__main__':
    app.run(debug=True)