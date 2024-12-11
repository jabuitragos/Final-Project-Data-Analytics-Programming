from flask import Flask, jsonify, request
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from bs4 import BeautifulSoup
import requests
import nest_asyncio
import os

# Apply the necessary patch for Flask to run in Jupyter (only needed in Jupyter, but safe here)
nest_asyncio.apply()

# Flask application setup
app = Flask(__name__)

# MongoDB connection
client = MongoClient(os.getenv('MONGO_URI'))  # Using environment variable for Mongo URI
db = client['AnimatedFilms']
collection = db['Films']

# Function to fetch and upload data
def fetch_and_upload_data():
    url = 'https://en.wikipedia.org/wiki/List_of_highest-grossing_animated_films'
    page = requests.get(url)
    soup = BeautifulSoup(page.text, 'html.parser')
    table = soup.find_all('table')[1]

    rows = table.find_all('tr')
    data = []

    for row in rows:
        cells = row.find_all(['td', 'th'])
        cells_text = [cell.get_text(strip=True) for cell in cells]
        data.append(cells_text)

    df = pd.DataFrame(data)

    df.columns = df.iloc[0]
    df = df[1:]
    df.reset_index(drop=True, inplace=True)
    df.dropna(how='all', inplace=True)

    df['Title'] = df['Title'].str.replace(r'[†]', '', regex=True)
    df['Title'] = df['Title'].str.replace(r'\[nb \d+\]', '', regex=True).str.strip()

    df = df[['Title', 'Year', 'Worldwide gross']]
    df['Worldwide gross'] = df['Worldwide gross'].str.replace(r'[\$,]', '', regex=True).astype(float)

    records = df.to_dict(orient='records')

    for record in records:
        collection.update_one({'Title': record['Title']}, {'$set': record}, upsert=True)


# Scheduler to fetch data every 24 hours
scheduler = BackgroundScheduler()
scheduler.add_job(fetch_and_upload_data, 'interval', hours=24)
scheduler.start()

@app.route('/')
def home():
    return '''
    <h1>Hi! This is the Animated Films Website</h1>
    <p>Visit <a href="/films">/films</a> to see all films.</p>
    <h2>Filter Films</h2>
    <p>Use the button below to filter films by year:</p>
    <form action="/films/2020" method="get">
        <button type="submit">Filter by 2020</button>
    </form>
    <h2>Search Films</h2>
    <p>Are you looking for a specific film? Search here:</p>
    <form action="/search" method="get">
        <input type="text" name="q" placeholder="Search by Title">
        <button type="submit">Search</button>
    </form>
    '''


@app.route('/films', methods=['GET'])
def get_films():
    films = list(collection.find({}, {'_id': 0}))
    return jsonify(films)

@app.route('/films/<int:year>', methods=['GET'])
def get_films_by_year(year):
    films = list(collection.find({'Year': str(year)}, {'_id': 0}))
    if films:
        return jsonify(films)
    return jsonify({'error': f'No films found for this year {year}'}), 404

@app.route('/search', methods=['GET'])
def search_films():
    query = request.args.get('q', '')
    films = list(collection.find({'Title': {'$regex': query, '$options': 'i'}}, {'_id': 0}))
    return jsonify(films)

@app.route('/films/<title>', methods=['GET'])
def get_film_by_title(title):
    film = collection.find_one({'Title': title}, {'_id': 0})
    if film:
        return jsonify(film)
    return jsonify({'error': 'Film not found in our list'}), 404

def start_flask():
    app.run(host='0.0.0.0', port=5000)  # This ensures the app is publicly accessible

if __name__ == '__main__':
    start_flask()
