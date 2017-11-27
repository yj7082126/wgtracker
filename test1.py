# -*- coding: utf-8 -*-
"""
Created on Sat Nov 11 22:01:25 2017

@author: user
"""
from bs4 import BeautifulSoup
import requests
import re
import urllib
from datetime import date, timedelta, datetime
import numpy as np
import pandas as pd
from sqlalchemy import Table, create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import nltk
from nltk.corpus import sentiwordnet as swn
from nltk.corpus import stopwords


headers = {'User-Agent': "Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36"}

emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags=re.UNICODE)

default_stopwords = set(stopwords.words('english'))
#%%
#DCinside

def timediff_dc(limit, newtime):
    return (limit - datetime.strptime(newtime, '%Y.%m.%d')).total_seconds()/86400

def getData_dc(bb, limit, limit2):
    df = pd.DataFrame(columns = ['No', 'Category', 'Title', 'Comment', 'User', 'Date', 'Hit', 'Likes'])
    today = date.today().strftime('%Y.%m.%d')    
    tmp = True
    i = 0
    x = 1
    while tmp:
        invurl = 'http://m.dcinside.com/list.php?id=' + bb + '&page=' + str(x)
        print(invurl)
        req = requests.Session().get(invurl, headers=headers)
        html = req.text
        soup = BeautifulSoup(html, 'lxml')
        table = soup.find_all('ul', {'class': 'list_best'})[0].find_all('a')
        y = 0
        while tmp and y < len(table):
            row = table[y]
            y += 1
            no = int(row['href'].split('&')[1].split('=')[1])
            
            icon = row.find_all('span')[1]['class'][1].split('_')
            if(icon[1] == 't'):
                if(len(icon) == 2):
                    category = '글'
                elif(icon[2] == 'c'):
                    category = '개념글'
                else:
                    category = '글?'
            elif(icon[1] == 'p'):
                if(icon[2] == 'y'):
                    category = '그림'
                elif(icon[2] == 'c'):
                    category = '개념글'
                else:
                    category = '그림?'
            else:
                category = '?'
                
            title = emoji_pattern.sub('', row.find_all('span')[2].get_text()).encode('utf-8')
            
            comment = row.find_all('em')[0].get_text()
            if (comment):
                comment = int(re.sub('[^\w]', '', comment))    
            else:
                comment = 0 
                
            user = row.find_all('span', {'class' : 'name'})[0].get_text()
            
            blanklist = row.find_all('span', {'class' : ''})
            date_tmp = blanklist[0].get_text()
            time_tmp = " 00:00:00"
            hit = int(blanklist[2].get_text())
            like = int(blanklist[4].get_text())
            
            if(':' not in date_tmp):                              
                if(int(timediff_dc(limit, date_tmp)) > 0 | int(timediff_dc(limit2, date_tmp)) < 0):
                    if(timediff_dc(limit, table[y+1].find_all('span', {'class' : ''})[0].get_text()) > 0):
                        tmp = False   
                date_tmp = date_tmp + time_tmp 
            else:
                date_tmp = today + " " + date_tmp + ":00"
            if tmp:
                df.loc[i] = [no, category, title, comment, user, date_tmp, hit, like]
                
            i += 1
        x += 1
    return df
#%%
#ENT

def timediff_ent(limit, newtime):
    return (datetime.strptime(limit, '%m/%d/%Y') - datetime.strptime(newtime, '%m/%d/%Y')).total_seconds()/86400

def getData_ent(limit, limit2):
    df = pd.DataFrame(columns = ('No', 'DiscNo', 'DiscTitle', 'DateTime', 'User', 'PrevNo'))
    tmp = True
    i = 0   
    x = 1 
    contList = dict()
    while tmp: 
        invurl = 'http://www.teradevtracker.com/archive/' + '?page=' + str(x)
        print(invurl)
        page = urllib.request.urlopen(invurl)
        soup = BeautifulSoup(page, 'lxml')
        table = soup.find_all('div', {'class' : 'centerdiv'})[4].find_all('div', {'class' : 'post'})
        y = 0
        while tmp and y < len(table):
            row = table[y]
            y = y+1
            link = row.find_all('a', {'class' : 'visitedlink'})[0]['href'].split('/')
            if(link[5] == 'comment'):
                no = int(link[6])
            else:
                no = 0
            
            disc = row.find_all('a', {'class' : 'visitedlink'})[1]
            discno = int(disc['href'].split('=')[1].split('#')[0])
            discname = disc.get_text() 
            
            timeline = row.find_all('span', {'class' : 'posttime'})[0].get_text().split(',')
            tdate = timeline[0]
            ttime = timeline[1].strip()
            
            user = row.find_all('a')[2].get_text()
            if '(' in user:
                user = user.split(' (')[0] + '_Dev'
            
            record = row.find_all('a', {'class' : 'QuoteLink'})
            #print(record)
            if(record):
                if('_' in record[0]['href']):
                    prevNo = int(record[0]['href'].split('_')[1])
            else:
                prevNo = 0
            
            content = row.find_all('div', {'class' : 'postcontent'})[0]
            
            if('blockquote' in str(content)):
                content_string = str(content).split('</blockquote>')[-1].split('>')[1].split('<')[0]
            else:
                content_string = str(content.get_text())
            
#            score = sentiment_calculator(content_string)
#            posScore = score[0]
#            negScore = score[1]
            
            content_key = str(discno) + '/' + str(no)
            
            if((timediff_ent(limit, tdate) > 0)):
                tmp = False
            elif((timediff_ent(limit2, tdate) < 0)):
                i += 1
            else:
                tdtime = datetime.strptime(tdate + " " + ttime, "%m/%d/%Y %I:%M %p")
                df.loc[i] = [no, discno, discname, tdtime, user, prevNo]
                contList[content_key] = content_string
                i += 1
        x += 1
    return df, contList
#%%
    
bb = 'tera'       
limit = datetime.strptime('2017-11-13', '%Y-%m-%d')
limit2 = datetime.strptime('2017-11-21', '%Y-%m-%d')
dc_tera = getData_dc(bb, limit, limit2)

#%%

limit = '07/13/2017'
limit2 = '11/26/2017'
df, contList = getData_ent(limit, limit2)
#%%

def words():
    text = ""
    for key in contList:
        line = contList[key]
        text += " "
        text += line
     
    word = nltk.word_tokenize(text)
    word = [w for w in word if len(w) > 1]
    word = [w.lower() for w in word]
    stemmer = nltk.stem.snowball.SnowballStemmer('english')
    word = [stemmer.stem(w) for w in word]
    fdist = nltk.FreqDist(word)
    return text, fdist.most_common(10)

def words_noun(startdate, enddate, num):
    text = ""
    days = (int)(timediff_ent(enddate, startdate)) + 1
    for x in range(days):
        date = (datetime.strptime(startdate, '%m/%d/%Y') + timedelta(x))
        date2 = (datetime.strptime(startdate, '%m/%d/%Y') + timedelta(x+1))
        for index, row in df.loc[(df['DateTime'] >= date) & (df['DateTime'] < date2), ['No', 'DiscNo']].iterrows():
            index = str(row['DiscNo']) + '/' + str(row['No'])
            line = contList[index]
            text += " "
            text += line
            
    word = nltk.word_tokenize(text)
    tags = nltk.pos_tag(word)
    tags = [item[0] for item in tags if item[1][0] == 'N']
    tags = [w for w in tags if len(w) > 1]
    tags = [w.lower() for w in tags]
    fdist = nltk.FreqDist(tags).most_common(num)
    
    return text, fdist

def words_noun_sentiment(startdate, enddate, fdist):
    word_df = pd.DataFrame()
    i = 0
    for x in fdist:
        word_df.loc[i, 'Word'] = x[0]
        word_df.loc[i, 'Freq'] = x[1]
        word_df.loc[i, 'PosScore'] = 0
        word_df.loc[i, 'NegScore'] = 0
        i += 1
    
    days = (int)(timediff_ent(enddate, startdate)) + 1
    for x in range(days):
        date = (datetime.strptime(startdate, '%m/%d/%Y') + timedelta(x)).strftime('%m/%d/%Y')
        for index, row in df.loc[df['Date'] == date, ['No', 'DiscNo']].iterrows():
            index = str(row['DiscNo']) + '/' + str(row['No'])
            for word in [x for x in list(word_df['Word']) if (x in contList[index])]:
                score = sentiment_calculator(contList[index])
                word_df.loc[word_df['Word'] == word, 'PosScore'] += score[0]
                word_df.loc[word_df['Word'] == word, 'NegScore'] += score[1]
    
    return word_df

#%%
    
def word_sentiment_calculator(word,tag):
    pos_score=0
    neg_score=0
    
    if 'NN' in tag and len(list(swn.senti_synsets(word,'n')))>0:
        syn_set = list(swn.senti_synsets(word,'n'))
    elif 'VB' in tag and len(list(swn.senti_synsets(word,'v')))>0:
        syn_set = list(swn.senti_synsets(word,'v'))
    elif 'JJ' in tag and len(list(swn.senti_synsets(word,'a')))>0:
        syn_set = list(swn.senti_synsets(word,'a'))
    elif 'RB' in tag and len(list(swn.senti_synsets(word,'r')))>0:
        syn_set = list(swn.senti_synsets(word,'r'))
    else:
        return (0,0)
    
    for syn in syn_set:
        pos_score += syn.pos_score()
        neg_score += syn.neg_score()
    return (pos_score/len(syn_set),neg_score/len(syn_set))

def getList(textList, forumKey):
    forumKey = str(forumKey)
    wordList = []
    for i in textList:
        if(i.split('/')[0] == forumKey):
            print(textList[i])
            wordList.append(textList[i])
    return wordList

def sentiment_calculator(pos_tags):
    pos_score = 0
    neg_score = 0
    s_tk = nltk.word_tokenize(pos_tags)
    pos_tags = nltk.pos_tag(s_tk)
    for word, tag in pos_tags:
        pos_score += word_sentiment_calculator(word, tag)[0]
        neg_score += word_sentiment_calculator(word, tag)[1]
    
    return (pos_score, neg_score)


def sentiment_calculator2(pos_list):
    total_pos = 0
    total_neg = 0
    for text in pos_list:
        score = sentiment_calculator(text)
        if(score[0] > score[1]):
            total_pos += 1
        else:
            total_neg += 1
    return (total_pos, total_neg)
#%%
    
contList2 = pd.DataFrame.from_dict(contList, orient="index")
contList2.columns = ['Content']
contList2['PosScore'] = 0
contList2['NegScore'] = 0
for index, row in contList2.iterrows():
    print(index)
    contList2.loc[index]['PosScore'] = sentiment_calculator(row['Content'])[0]
    contList2.loc[index]['NegScore'] = sentiment_calculator(row['Content'])[1]

contList2.to_csv('ent_cont.csv')
#%%
text, frequency = words_noun("10/28/2017", "10/28/2017", 30)    
frequency_sent = words_noun_sentiment("10/28/2017", "10/28/2017", frequency)

df_forum = df.loc[df['No'] == 0, ['DiscNo', 'DiscTitle', 'Date', 'Time']]

for index, row in df_forum.iterrows():
    wordList = getList(contList, row['DiscNo'])
    df_forum.loc[index, 'Comments'] = len(wordList)
    df_forum.loc[index, 'posScore'] = sentiment_calculator2(wordList)[0]
    df_forum.loc[index, 'negScore'] = sentiment_calculator2(wordList)[1]

df_11051 = pd.DataFrame(columns = ('Title', 'posScore', 'negScore'))
wordList = getList(contList, 11051)
i = 0
for word in wordList:
    df_11051.loc[i, 'Title'] = word
    df_11051.loc[i, 'posScore'] = sentiment_calculator(word)[0]
    df_11051.loc[i, 'negScore'] = sentiment_calculator(word)[1]
    i += 1    
    
df2 = pd.DataFrame(list(contList.items()))
df2.columns = ['Index', 'Content']
#%%

eng = create_engine("mysql://root:passwd@localhost:3306/eng?charset=utf8")
metadata = MetaData(bind=eng)
users = Table('ent_df', metadata, autoload=True)
content = Table('ent_cont', metadata, autoload=True)

start_date = datetime(2017, 7, 13)
j = 0
word_df = pd.DataFrame()
for single_date in (start_date + timedelta(n) for n in range(133)):
    double_date = single_date + timedelta(1)
    print(single_date)
    currList = pd.DataFrame(list(users.select((users.c.DateTime >= single_date) & (users.c.DateTime < double_date)).execute()))
    currList.columns = ['No', 'DiscNo', 'DiscTitle', 'DateTime', 'User', 'PrevNo']

    contList = pd.DataFrame(columns = ('Index', 'Content'))
    contText = ""
    i = 0

    for i, r in currList.iterrows():
        index = str(r['DiscNo']) + '/' + str(r['No'])
        rowX = content.select((content.c.Index == index)).execute().first()
        if(rowX):
            contList.loc[i] = [rowX[0], " " + rowX[1]]
            contText += (" " + rowX[1])
            i += 1
        else:
            print(r['DateTime'])
            continue

    word = nltk.word_tokenize(contText)
    tags = nltk.pos_tag(word)
    tags = [w for w in tags if ('\n' not in w)]
    tags = [item[0] for item in tags if item[1][0] == 'N']
    tags = [w for w in tags if len(w) > 1]
    tags = [w.lower() for w in tags]
    fdist = nltk.FreqDist(tags).most_common(30)
    
    
    
    for x in fdist:
        word_df.loc[j, 'Word'] = x[0]
        word_df.loc[j, 'Hit'] = x[1]
        word_df.loc[j, 'Date'] = single_date
        word_df.loc[j, 'PosScore'] = 0
        word_df.loc[j, 'NegScore'] = 0
        j += 1
                
    for index, row in currList[['No', 'DiscNo']].iterrows():
        idx = str(row['DiscNo']) + '/' + str(row['No'])
        newStr = str(contList.loc[contList['Index'] == idx]['Content'])
        for word in [x for x in list(word_df['Word']) if (x in newStr)]:
            score = sentiment_calculator(newStr)
            if (score[0] >= score[1]):
                word_df.loc[(word_df['Word'] == word) & (word_df['Date'] == single_date), 'PosScore'] += 1
            else:
                word_df.loc[(word_df['Word'] == word) & (word_df['Date'] == single_date), 'NegScore'] += 1
#%%
for index, row in df.iterrows():
    row['DateTime'] = row['DateTime'].strftime('%Y-%m-%d %H:%M:%S')
    
try:
    df.to_sql(con=eng, name="ent_df", if_exists='append', index=False)
    print("Successful")
except:
    print("Unsuccessful")
    
df2.to_sql(con=eng, name="ent_cont", if_exists='append', index=False)

#%%
start_date = datetime(2017,10,15)
day_count = 14
operation1 = "select No, DiscNo from ent_df where DateTime >= %s and DateTime < %s"
operation2 = "select * from ent_cont where eng.ent_cont.Index = %s"
eng = create_engine("mysql://root:passwd@localhost:3306/eng?charset=utf8")

finalRes = list()
for single_date in (start_date + timedelta(n) for n in range(day_count)):
    double_date = single_date + timedelta(1)
    
    cursor.execute(operation1, ( single_date, double_date))
    data1 = list(cursor.fetchall())
    
    data2 = list()
    text1 = ""
    for row in data1:
        index = str(row[1]) + '/' + str(row[0])
        cursor.execute(operation2, (index))
        newRes = list(cursor.fetchone())
        data2.append(newRes)
        text1 += (" " + newRes[1])
    
    word = nltk.word_tokenize(text1)
    tags = nltk.pos_tag(word)
    tags = [w for w in tags if ('\n' not in w)]
    tags = [item[0] for item in tags if item[1][0] == 'N']
    tags = [w for w in tags if len(w) > 1]
    tags = [w.lower() for w in tags]
    fdist = nltk.FreqDist(tags).most_common(30)

    res = pd.DataFrame(list(fdist))
    res.columns = ['Word', 'Hit']
    res['Date'] = single_date
    
    word_df.to_sql(con=eng, name="ent_top", if_exists='append', index=False)
    
    content = '<a href="/" class="list-group-item disabled">'
    content += ('<b>' + str(single_date)[:10] + '</b>' + '</a>')
    
    for row in fdist:
        directory = '/detail/' + row[0]
        content += '<a href=' 
        content += ('"' + directory + '"')
        content += ' class="list-group-item">'
        content += row[0]
        content += "</a>"
    
    finalRes.append(content)