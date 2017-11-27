# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 22:01:25 2017

@author: user
"""

from flask import Flask, render_template, json, request, redirect, session
from flaskext.mysql import MySQL
from datetime import date, timedelta, datetime

app = Flask(__name__)
app.debug = True
app.secret_key = 'the secret to the universe is 42'
mysql = MySQL()

app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'passwd'
app.config['MYSQL_DATABASE_DB'] = 'eng'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)

conn = mysql.connect()
cursor = conn.cursor()

start_date = datetime(2017,10,15)
day_count = 14
operation1 = "select No, DiscNo from ent_df where DateTime >= %s and DateTime < %s"
operation2 = "select * from ent_cont where eng.ent_cont.Index = %s"
operation3 = "select * from ent_top where ent_top.Date = %s"
operation4 = "select PosScore, NegScore from ent_top where ent_top.Date = %s and Word = %s"

    
@app.route("/", methods=['GET', 'POST'])
def main(result=None):
    finalRes = list()

    if request.method == 'POST':
        result = datetime.strptime(request.form['inputDate'], "%Y-%m-%d")
        print("post")
    else:
        result = datetime(2017,10,28)
        print("get")
    print(result)
        
    for single_date in (result + timedelta(n) for n in range(day_count)):  
        cursor.execute(operation3, (single_date))
        fdist = list(cursor.fetchall())
        
        content = '<a href="/" class="list-group-item disabled">'
        content += ('<b>' + str(single_date)[:10] + '</b>' + '</a>')
        
        for row in fdist:
            directory = '/detail/' + str(single_date)[:10] + "/" + row[0]
            content += '<a href=' 
            content += ('"' + directory + '"')
            content += ' class="list-group-item">'
            content += row[0]
            content += "</a>"
        
        finalRes.append(content)
   
    return render_template('index.html', text = finalRes)


@app.route('/detail/<dateX>/<word>')
def detail(dateX, word):
    cursor.execute(operation4, (dateX, word))
    score = list(cursor.fetchall())
    posScore = score[0][0]
    negScore = score[0][1]
    
    single_date = datetime.strptime(dateX, "%Y-%m-%d")
    double_date = single_date + timedelta(1)
    cursor.execute(operation1, (single_date, double_date))
    data1 = list(cursor.fetchall())
    
    data2 = list()
    for row in data1:
        index = str(row[1]) + '/' + str(row[0])
        cursor.execute(operation2, (index))
        newRes = list(cursor.fetchone())
        if word in newRes[1]:
            data2.append(newRes[1])
    
    df = ""
    for row in data2:
        df += '<li class="list-group-item">'
        df += row
        df += '</li>'
    
        
        
    return render_template('detail.html', text = word, posScore = posScore, 
                           negScore = negScore, data = df)
		
if __name__ == "__main__":
    app.run()