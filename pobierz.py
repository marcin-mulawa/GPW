import requests
import pandas as pd
from bs4 import BeautifulSoup
import urllib
import re
import wget
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import pickle
from os import walk
from datetime import datetime

user=""
host=""
password=""
port=1234
database=""



link = 'https://info.bossa.pl/pub/intraday/mstock/daily/'
f = requests.get(link)
html = f.text
soup = BeautifulSoup(html, 'lxml')

href_tags = list(soup.find_all(href=True))
href_tags = [ str(x) for x in href_tags]
files=[x for x in href_tags if re.match(r".*\d\d\d\d-\d\d-\d\d.zip.*",x)]
files = [ x.split('"') for x in files]
files = [x[1] for x in files]


engine = create_engine("mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"
                       .format(user=user,
                               pw=password,
                               host=host,
                               port = port,
                               db=database))

connection = pymysql.connect(host=host,
                             port = port,
                             user=user,
                             password=password,
                             db=database)
my_cursor = connection.cursor()

my_cursor.execute("SELECT id_company,name FROM gpw.company")
company_ids = my_cursor.fetchall()
company_ids = pd.DataFrame(company_ids, columns =['id', 'name']) 

sql = "SELECT MAX(date) FROM gpw.historic_data"

my_cursor.execute(sql)

date = my_cursor.fetchall()

date =date[0][0].strftime('%Y-%m-%d')

id = files.index(f'{date}.zip')
files = files[:id]
files.reverse()
print(files)
update=[]
for f in files:
    update.append(wget.download(f'https://info.bossa.pl/pub/intraday/mstock/daily/{f}',f'data/{f}'))

df=pd.DataFrame([])
for f in update:
    df2 = pd.read_csv(f'{f}',names=['name','0','date','hour','open','high','low','close','volume'],index_col=False)
    df2.drop('0', axis=1, inplace=True)
    
    df2['date'] = df2['date'].astype(str)
    df2['hour'] = df2['hour'].astype(str)
    df2['date'] = df2['date']+df2['hour']
    df2['date'] =  pd.to_datetime(df2['date'], format='%Y%m%d%H%M%S')
    df2.drop('hour', axis=1, inplace=True)
    df2['date'] =df2['date'].astype(str)
    frames = [df2,df]
    df = pd.concat(frames)
df =pd.merge(df, company_ids, on='name')
df.drop('name', axis=1, inplace=True)
df = df[['id','date','open','high','low','close','volume']]
df.rename(columns={'id': 'id_company'}, inplace=True)
df.to_sql('historic_data', con = engine, if_exists = 'append', chunksize = 100000, index=False)