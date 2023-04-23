import bonobo
import pandas as pd
import requests
import time
import pymongo

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


def extract_info_and_listeners_for_titles_by_artist(artist_names: list):
    for name in artist_names:
        # get the artist id from artist name
        url = ('https://api.discogs.com/database/search?q=') + name + ('&{?type=artist}&token=') + DISCOGS_API_KEY
        discogs_artist_info = requests.get(url).json()
        id = discogs_artist_info['results'][0]['id']
        # with id get artist's releases
        url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
        releases = requests.get(url).json()
        releases = releases['releases']

        print('Found releases from discogs.com for artist ' + str(name) + ' with Discogs ID: ' + str(id))

        # store the releases/tracks info in a list with discogs price and lastfm playcount
        releases_info = []
        for index in range(len(releases)):
            # find playcounts from lastfm for each release title
            title = releases[index]['title']
            lastfm_url = 'https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=' + LASTFM_API_KEY + '&artist=' + name + '&track=' + title + '&format=json'

            try:
                lastfm_source = requests.get(lastfm_url).json()
                if 'track' in lastfm_source.keys():
                    discogs_url = releases[index]['resource_url']
                    discogs_source = requests.get(discogs_url).json()
                    # search if exists track's price
                    if 'lowest_price' in discogs_source.keys():
                        if 'formats' in discogs_source.keys():
                            releases_info.append({'Title': title,
                                                  'Collaborations': releases[index]['artist'],
                                                  'Year': discogs_source['year'],
                                                  'Format': discogs_source['formats'][0]['name'],
                                                  'Discogs Price': discogs_source['lowest_price'],
                                                  'Lastfm Playcount': lastfm_source['track']['playcount']})
                        else:
                            releases_info.append({'Title': title,
                                                  'Collaborations': releases[index]['artist'],
                                                  'Year': discogs_source['year'],
                                                  'Format': None,
                                                  'Discogs Price': discogs_source['lowest_price'],
                                                  'Lastfm Playcount': lastfm_source['track']['playcount']})
                        print(
                            'Found playcount from last.fm and informations from discogs.com for title {}'.format(title))
                        # sleep 3 secs to don't miss requests
                        # time.sleep(1)
                else:
                    print('Not found playcount from last.fm for title {}'.format(title))
            except:
                print('Not found playcount from last.fm for title {}'.format(title))
                continue

        yield {name: releases_info}


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
    key = list(releases.keys())
    artist = str(key[0])

    df = pd.DataFrame(releases[artist])
    # find and remove the rows/titles where there are no selling prices in discogs.com
    df = df[df['Discogs Price'].notna()]
    print('Remove releases where there no selling price in discogs.com')
    # keep only the rows has positive value of year
    df = df[df['Year'] > 0]
    print('Remove releases where have wrong year value in discogs.com')

    yield {artist: df.to_dict(orient='records')}


def drop_duplicates_titles(releases: dict):
    key = list(releases.keys())
    artist = str(key[0])

    df = pd.DataFrame(releases[artist])
    df = df.drop_duplicates(subset=['Title'])
    print('Find and remove the duplicates titles if exist!')
    df = df.set_index('Title')

    yield {artist: df.to_dict(orient='index')}


def print_data(data):
    print(data)
    yield data


def load_to_database(data):
    client = pymongo.MongoClient(
        "mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
    db = client["mydatabase"]
    artists = db['artists']

    artists.insert_one(data)
    print('Artist {} insert to DataBase!'.format(data['Artist']))


if __name__ == '__main__':
    # find names from csv file
    df = pd.read_csv('spotify_artist_data.csv')
    artist_names = list(df['Artist Name'].unique())
    artist_names = artist_names[:1]

    # define graph
    graph = bonobo.Graph()

    # set _input to None, so the function won't start on ts own but only after it receives input from the other chains.
    graph.add_chain(print_data, _input=None)
    graph.add_chain(extract_info_from_artist(artist_names), clean_the_artist_content, _output=print_data)
    graph.add_chain(extract_info_and_listeners_for_titles_by_artist(artist_names), remove_wrong_values,
                    drop_duplicates_titles, _output=print_data)

    bonobo.run(graph)
