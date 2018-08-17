import findspark
findspark.init('/usr/local/spark')
import pyspark
from pyspark.sql import SparkSession
from flask import Flask, render_template, request, url_for, json,redirect,url_for
import mysql.connector
import pandas as pd

spark = SparkSession.builder.appName("sql_connection").master("yarn").getOrCreate()
app = Flask(__name__)

cnx = mysql.connector.connect(user='root', password='',host='locakhost',database='testDB')


@app.route("/login")
def login():
    return render_template('login.html')

@app.route('/auth', methods=['GET','POST'])
def auth():
    username = request.form['uname']
    password = request.form['password']
    cursor = cnx.cursor()
    select_stmt =("SELECT * from credentials where uname='" + username + "' and password='" + password + "'")
    iterator = cursor.execute(select_stmt)
    row = cursor.fetchone()
    if row is None:
        return render_template('login.html')
    else:
        return redirect(url_for('index'))

""""@app.route('/login')
def login():
    render_template('login.html')

@app.route('/auth')
def auth():
    uname = request.form['uname']
    password = request.form['password']
    cursor = cnx.cursor()
    cursor.execute("SELECT * FROM credentials WHERE username='" + uname + "' AND password='" + password + "'")
    data = cursor.fetchone()
    if data is None:
        return render_template('login.html')
    else:
        return redirect(url_for('index'))
#    select_stmt =( "SELECT * FROM testDB.credentials WHERE username='"+uname+"' AND password = '"+password="'" )
#    iterator = cursor.execute(select_stmt)
#    row = iterator.fetchone()
#    while row is not None:
#        return render_template('index.html')"""


@app.route('/index')
def index():
    render_template('index.html')


@app.route('/input_connection')
def input_connection():
    dataframe =  spark.read.option("inferSchema","True").option("header","True").csv("file:///path/to/data")
    dataframe.createOrReplaceTempView("Data")
    Query = request.form['query']
    query_df = spark.sql(Query)
    pandas_df = query_df.toPandas()
    pandas_html = pandas_df.to_html('table.html')
    render_template('query.html', pandas_html = pandas_html )


@app.route('/write')
def write():
    sql_df = query_df.write.format("jdbc").options(url = "jdbc:mysql://localhost:3306",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "testDB.Data",
    user = "root",
    password = "").option("inferSchema","True").save()
    render_template('query.html')


if __name__ == '__main__':
    app.run(debug= True)
