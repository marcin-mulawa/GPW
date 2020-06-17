import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from datetime import datetime, timedelta
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import plotly.graph_objs as go


host = ''
port = 1234
user = ''
password = ''
db = ''


def connnect_database(host, port, user, password, db):
    connection = pymysql.connect(host=host,
                             port = port,
                             user= user,
                             password= password,
                             db= db)
    cursor = connection.cursor()

    return cursor,connection


def read_data(company,start,end):
    
    my_cursor,connection = connnect_database(host,port,user,password,db)
    sql ="""SELECT date,close,volume FROM historic_data 
    JOIN company USING (id_company) 
    where company.name = '{}' 
    and historic_data.date between '{}' and '{}' """.format(company,start,end)
    my_cursor.execute(sql)
    result = my_cursor.fetchall()
    transactions = pd.DataFrame(result,columns=['date','close','volume'])
    transactions.sort_values(by='date', inplace=True)
    connection.close()

    return transactions


app = dash.Dash()
server = app.server

my_cursor, connection = connnect_database(host,port,user,password,db)
my_cursor.execute("SELECT name FROM gpw.company")
options = my_cursor.fetchall()
options = [{'label': name[0],'value': name[0]} for name in options]
my_cursor.execute("SELECT dates FROM gpw.no_gpw_dates where dates <= curdate()")
dates = my_cursor.fetchall()
dates = [str(date[0]) for date in dates] 
connection.close()

app.layout = html.Div([
    html.H1('Wykres GPW'),
    html.Div([
        html.H3('Wybierz spółkę:', style={'paddingRight':'30px'}),
        dcc.Dropdown(
            id='my_ticker_symbol',
            options=options,
            value=['CDPROJEKT'],
            multi=True
        )
    ], style={'display':'inline-block', 'verticalAlign':'top', 'width':'30%'}),
    html.Div([
        html.H3('Wybierz datę początkową i końcową:'),
        dcc.DatePickerRange(
            id='my_date_picker',
            min_date_allowed=datetime(2020, 1, 1),
            max_date_allowed=datetime.today(),
            start_date=datetime.today() - timedelta(days=7),
            end_date=datetime.today()
        )
    ], style={'display':'inline-block'}),
    html.Div([
        html.Button(
            id='submit-button',
            n_clicks=0,
            children='Zatwierdź',
            style={'fontSize':24, 'marginLeft':'30px'}
        ),
    ], style={'display':'inline-block'}),
    dcc.Graph(
        id='my_graph',
        figure={
            'data': [
                {'x': [1,1.5,2,2.5,3,3.5,4,4,5,5,4,5.5,5.5,6,5.5,5.5,6,6.5,6.5,6.7,6.7,7.2,7.2], 'y': [1,3,2,3,1,3,1,3,3,2,2,1,3,3,3,1,1,3,1,1,3,1,3]}
            ]
        }
    )
])


@app.callback(
    Output('my_graph', 'figure'),
    [Input('submit-button', 'n_clicks')],
    [State('my_ticker_symbol', 'value'),
    State('my_date_picker', 'start_date'),
    State('my_date_picker', 'end_date')])
def update_graph(n_clicks, stock_ticker, start_date, end_date):

    df = read_data(stock_ticker[0],start_date,end_date)
    df.sort_values(by='date', ascending=False, inplace=True)
    
    fig = {
        'data': [{'x':df.date,'y': df.close, 'type': 'line', 'name': stock_ticker[0]},
                {'x':df.date,'y': df.volume, 'type': 'bar','yaxis':'y2', 'name': 'Wolumen'}],
                
        'layout': go.Layout(
            xaxis = dict(title='Time', rangebreaks=[
                        dict(bounds=['sat', 'mon']),
                        dict(values=dates),
                        dict(pattern='hour', bounds=[17.25, 8.9])
            ]),
            yaxis = {'title': 'Kurs [zł]'},
            yaxis2 = {'title': 'Wolumen', 'anchor': 'x', 
                        'overlaying': 'y', 'side': 'right'},
                        
            hovermode='x unified',
            height=600
        )
    }

    return fig


if __name__ == '__main__':
    app.run_server()
