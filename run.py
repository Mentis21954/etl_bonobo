import bonobo
import pandas as pd
import requests
import time

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

def extract_info_from_artist(artist_names: list):
    # extract for all artists' informations from last fm and store as a dict
    for name in artist_names:
        url = 'https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=' + str(name) + (
            '&api_key=') + str(LASTFM_API_KEY) + '&format=json'
        artist_info = requests.get(url).json()
        print('Search information for artist {} ...'.format(str(name)))
        yield {name: artist_info['artist']['bio']['content']}

def extract_titles_from_artist(artist_names: list):

    for name in artist_names:
        # get the artist id from artist name
        url = ('https://api.discogs.com/database/search?q=') + name + ('&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']
        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()

        print('Found releases from discogs.com for artist ' + str(name) + ' with Discogs ID: ' + str(id))

        yield {name: releases['releases']}

def extract_info_for_titles(releases: dict):
    # store the releases/tracks info in a list
    releases_info = []

    key = list(releases.keys())
    artist = str(key[0])
    for index in range(len(releases[artist])):
        url = releases[artist][index]['resource_url']
        source = requests.get(url).json()
        # search if exists track's price
        if 'lowest_price' in source.keys():
            if 'formats' in source.keys():
                releases_info.append({'Title': source['title'],
                                      'Collaborations': releases[artist][index]['artist'],
                                      'Year': source['year'],
                                      'Format': source['formats'][0]['name'],
                                      'Discogs Price': source['lowest_price']})
            else:
                releases_info.append({'Title': source['title'],
                                      'Collaborations': releases[artist][index]['artist'],
                                      'Year': source['year'],
                                      'Format': None,
                                      'Discogs Price': source['lowest_price']})
        print("Found informations from discogs.com for {} {}'s titles".format(str((index + 1)), artist))
        # sleep 3 secs to don't miss requests
        time.sleep(3)

    # return artist's tracks for transform stage
    yield releases_info

def extract_playcounts_from_titles_by_artist(releases: dict):
    # initialize list for playcounts for each title
    playcounts = []
    # find playcounts from lastfm for each release title
    key = list(releases.keys())
    artist = str(key[0])
    for index in range(len(releases[artist])):
        title = releases[artist][index]['title']
        print(title)
        url = 'https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=' + LASTFM_API_KEY + '&artist=' + artist + '&track=' + title + '&format=json'

        try:
            source = requests.get(url).json()
            if 'track' in source.keys():
                playcounts.append({'Title': source['track']['name'],
                                   'Lastfm Playcount': source['track']['playcount']})
                print('Found playcount from last.fm for title {}'.format(title))
            else:
                print('Not found playcount from last.fm for title {}'.format(title))
        except:
            print('Not found playcount from last.fm for title {}'.format(title))
            continue

    return playcounts

def print_data(data):
    print(data)


if __name__ == '__main__':
    # find names from csv file
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())
    artist_names = artist_names[:2
                   ]
    graph = bonobo.Graph()
    graph.add_chain(extract_info_from_artist(artist_names), print_data)
    releases = extract_titles_from_artist(artist_names)
    graph.add_chain(releases, extract_playcounts_from_titles_by_artist, print_data)
    graph.add_chain(extract_info_for_titles, print_data, _input=releases)

    bonobo.run(graph)
