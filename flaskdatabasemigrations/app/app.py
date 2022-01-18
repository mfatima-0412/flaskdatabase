from flask import Flask, flash, redirect, render_template, \
     request, url_for

app = Flask(__name__)

@app.route('/')
def index():
    return render_template(
        'index.html',
        data=[{'name':'MY SQL'}, {'name':'POSTGRES'}, {'name':'SQL SERVER'}])

@app.route("/test" , methods=['GET', 'POST'])
def test():
    select = request.form.get('comp_select')
    return(str(select)) # just to see what select is

#
if __name__=='__main__':
    app.run(debug=True)
#app.run(host='localhost', port=8800)

# @app.route()
# def getting_sql_data():
#     if validate.forms:
#         if vf== postgres:
#             call your postgres.py file
#         elif vf==mysql:
#             call your mysql.py file
#         elif vf==sqlserver:
#             call your sqlserver.py file
#
#
#
from flask import Flask, app, request, render_template, session

