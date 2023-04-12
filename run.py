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
        url = 'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=' + str(name) + (
            '&api_key=') + str(LASTFM_API_KEY) + '&format=json'
        artist_info = requests.get(url).json()
        print('Search information for artist {} ...'.format(str(name)))
        yield {name: artist_info['artist']['bio']['content']}


def extract_titles_from_artist(artist_names):
    for name in artist_names:
        # get the artist id from artist name
        url = 'https://api.discogs.com/database/search?q=' + str(name) + (
            '&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']

        print('Search releases from discogs.com for artist {} ...'.format(str(name)))

        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()

        # store the tracks info in a list
        title_info, colab_info, year_info, format_info, price_info = [], [], [], [], []
        for index in range(len(releases['releases'])):
            url = releases['releases'][index]['resource_url']
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():

                title_info.append(source['title'])
                colab_info.append(releases['releases'][index]['artist'])
                year_info.append(source['year'])
                price_info.append(source['lowest_price'])
                if 'formats' in source.keys():
                    format_info.append(source['formats'][0]['name'])
                else:
                    format_info.append(None)
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(1)

        print('Find tracks from artist ' + str(name) + ' with Discogs ID: ' + str(id))
        yield {'Title': title_info, 'Collaborations': colab_info, 'Year': year_info,
               'Format': format_info, 'Discogs Price': price_info}


def print_data(data):
    print(data)


if __name__ == '__main__':
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())

    graph = bonobo.Graph()
    graph.add_chain(extract_info_from_artist(artist_names[:1]), print_data)
    graph.add_chain(extract_titles_from_artist(artist_names[:1), print_data)

    bonobo.run(graph)
