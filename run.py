import bonobo
import pandas as pd
import time
import json
import requests

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

def extract_info_from_artist(artist_names):

    for name in artist_names:
        # extract for all artists' informations from last fm and store as a dict
        url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + str(name) + (
                '&api_key=') + str(LASTFM_API_KEY) + ('&format=json')
        artist_info = requests.get(url).json()
        print('Search information for artist {} ...'.format(str(name)))
        yield artist_info['artist']['bio']['content']

def extract_titles_from_artist(name: str):
    # get the artist id from artist name
    url = 'https://api.discogs.com/database/search?q=' + str(name) + (
        '&{?type=artist}&token=') + DISCOGS_API_KEY
    discogs_artist_info = requests.get(url).json()
    id = discogs_artist_info['results'][0]['id']

    print('Search releases from discogs.com for artist {} ...'.format(str(name)))

    # with id get artist's releases
    url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
    releases = requests.get(url).json()
    releases_df = pd.json_normalize(releases['releases'])

    # store the tracks info in a list
    title_info, colab_info, year_info, format_info, price_info = [], [], [], [], []
    for index, url in enumerate(releases_df['resource_url'].values):
        source = requests.get(url).json()
        # search if exists track's price
        print('Found ' + str((index + 1)) + ' titles!')
        time.sleep(3)
        if 'lowest_price' in source.keys():
           yield [source['title'], source['year']]

def print_data(title):
    print(title)

if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())

    graph = bonobo.Graph(
        extract_titles_from_artist(artist_names[1]),
        print_data
    )
    bonobo.run(graph)
