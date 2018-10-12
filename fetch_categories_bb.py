import configparser

from handlers import *

config = configparser.ConfigParser()
config.read('../account.ini')
bb_fetcher = BigBuyFetcher(config['MANNE'])
db_handler = DBHandler(config['DB'])

data = bb_fetcher.fetch_data_from_bb('rest/catalog/categories.json?isoCode=de')


def create_db_object(item):
    return item['id'], item['name'], item['parentCategory']


if 'code' not in data:
    query_data = list(map(create_db_object, data))
    query = 'INSERT INTO bb_categories VALUES (%s, %s, %s)'

    db_handler.write_to_table(query, query_data, 'bb_categories')
