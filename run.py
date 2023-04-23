import bonobo
import pandas as pd
import requests
import json
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


def extract_info_and_listeners_for_titles(artist_names: list):
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
        #time.sleep(1)

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

    #with open('playcounts.json', 'w') as outfile:
        #outfile.write(json.dumps(playcounts))
    yield playcounts

def clean_the_artist_content(content: dict):
    content_df = pd.DataFrame(content.values(), columns=['Content'], index=content.keys())

    # remove new line commands, html tags and "", ''
    content_df['Content'] = content_df['Content'].replace(r'\r+|\n+|\t+', '', regex=True)
    content_df['Content'] = content_df['Content'].replace(r'<[^<>]*>', '', regex=True)
    content_df['Content'] = content_df['Content'].replace(r'"', '', regex=True)
    content_df['Content'] = content_df['Content'].replace(r"'", '', regex=True)
    print('Clean the informations text')

    yield content_df.to_dict(orient='index')


def remove_wrong_values(releases: dict):
    df = pd.DataFrame(releases)
    # find and remove the rows/titles where there are no selling prices in discogs.com
    df = df[df['Discogs Price'].notna()]
    print('Remove releases where there no selling price in discogs.com')
    # keep only the rows has positive value of year
    df = df[df['Year'] > 0]
    print('Remove releases where have wrong year value in discogs.com')

    #with open('releases_info.json', 'w') as outfile:
        #outfile.write(df.to_json(orient='columns', compression='infer'))
    yield df.to_dict(orient='records')

def merge_titles_data():
    #releases_df = pd.read_json('releases_info.json')
    playcounts_df = pd.read_json('playcounts.json')
    print(playcounts_df.head())
    #df = pd.merge(releases_df, playcounts_df, on='Title')
    #print('Merge releases and playcounts data for artist {}'.format(self.name))
    print('Merge releases and playcounts data for artist')

    yield playcounts_df.to_dict(orient='records')

def print_data(data):
    print(data)

if __name__ == '__main__':
    # find names from csv file
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())
    artist_names = artist_names[:1]

    # define graph
    graph = bonobo.Graph()
    #graph.add_chain(extract_info_from_artist(artist_names), clean_the_artist_content, print_data)

    # set _input to None, so merge won't start on ts own but only after it receives input from the other chains.
    graph.add_chain(print_data, _input=None)
    releases = extract_titles_from_artist(artist_names)
    graph.add_chain(releases, extract_playcounts_from_titles_by_artist, _output= print_data)
    graph.add_chain(extract_info_for_titles, remove_wrong_values, _input= releases, _output=print_data)



    bonobo.run(graph)
