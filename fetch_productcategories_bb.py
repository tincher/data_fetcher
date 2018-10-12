import configparser
from time import sleep

from handlers import *

config = configparser.ConfigParser()
config.read('../account.ini')
bb_fetcher = BigBuyFetcher(config['MANNE'])
db_handler = DBHandler(config['DB'])

iso_code = 'de'
query = 'INSERT INTO bb_item_categories VALUES (%s, %s, %s)'


def create_db_object(obj):
    return obj['id'], obj['product'], obj['category']


products = db_handler.get_all_from_table('bb_items', 'id')

data = []
for item in products:
    category_url = 'rest/catalog/productcategories/' + str(item[0]) + '.json'
    item_category = bb_fetcher.fetch_data_from_bb(category_url)
    while 'code' in item_category:
        item_category = bb_fetcher.fetch_data_from_bb(category_url)
        sleep(5)
    data.append(item_category)
    for cat in item_category:
        db_handler.add_to_table(query, create_db_object(cat))


query_data = list(map(create_db_object, data))

db_handler.write_to_table(query, query_data, 'bb_item_categories')
