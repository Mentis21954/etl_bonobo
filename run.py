import bonobo
import pandas as pd
import time
import json
import requests

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

def extract_info_from_artist(artists_names):
    # extract for all artists' informations from last fm and store as a dict
    for name in artists_names:
        url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + str(name) + (
            '&api_key=') + str(LASTFM_API_KEY) + ('&format=json')
        artist_info = requests.get(url).json()
        yield artist_info['artist']['bio']['content']

def print_data(content: str):
    print(content)

if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())

    graph = bonobo.Graph(
        extract_info_from_artist(artist_names[2]),
        print_data
    )
    bonobo.run(graph)
