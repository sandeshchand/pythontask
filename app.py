import pandas as pd
import base64
from io import BytesIO
from db import sqlitedb
from flask import Flask
from matplotlib.figure import Figure
import argparse

app = Flask(__name__)

# Define the command line arguments
parser = argparse.ArgumentParser(description="Flask application")
parser.add_argument('--host', type=str, default='localhost', help='the host to run the app on')
parser.add_argument('--port', type=int, default=5000, help='the port to run the app on')
args = parser.parse_args()


@app.route("/")
def index():

    sqlitedb.insert_data()
    dataframe = pd.read_json(sqlitedb.get_data().to_json())

    # Number of articles mentioned Justin Trudeau from 01.01.2018 till today
    dataframe['webPublicationDate'] = pd.to_datetime(dataframe['webPublicationDate']).dt.date
    dataframe_trudeau = dataframe[dataframe['webTitle'].str.contains('Justin Trudeau', case=False) & (
                dataframe['webPublicationDate'] >= pd.to_datetime('2018-01-01').date())]

    # group by date and count number of articles
    dataframe_count = dataframe_trudeau.groupby('webPublicationDate')['webTitle'].count().reset_index()
    dataframe_count.columns = ['Date', 'Number of articles']

    # Show the time distribution of the amount of article
    fig = Figure()

    # Add plot to figure
    ax = fig.add_subplot(111)
    fig.set_size_inches(12, 6)

    # Create a line plot of the number of articles by date
    ax.plot(dataframe_count['Date'], dataframe_count['Number of articles'])

    # Set the x-axis label to 'Date'
    ax.set_xlabel('Date')

    # Set the y-axis label to 'Number of articles'
    ax.set_ylabel('Number of articles')

    # Set the title of the plot to 'Time Distribution of the Number of Articles'
    ax.set_title('Time Distribution of the Number of Articles')

    #Save it to a temporary buffer
    buf = BytesIO()
    fig.savefig(buf, format="png")

    # Embed the result in the html output
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return f"<img src='data:image/png;base64,{data}'/>"


app.run(host=args.host, port=args.port)
