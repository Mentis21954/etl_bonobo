import bonobo
import requests
import csv

def extract():
    response = requests.get('https://jsonplaceholder.typicode.com/users')
    data = response.json()
    for row in data:
        print('Data before transform \n', row)
        yield row

def transform(row):
    row['address'] = row['address']['street'] + ', ' + row['address']['city']
    row.pop('company', None)
    print('Data after transform \n', row)
    return row


graph = bonobo.Graph(
    extract,
    transform
)

if __name__ == '__main__':
    bonobo.run(graph)


