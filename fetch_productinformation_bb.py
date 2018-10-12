import configparser
from time import sleep

from handlers import *

config = configparser.ConfigParser()
config.read('../account.ini')
bb_fetcher = BigBuyFetcher(config['MANNE'])
db_handler = DBHandler(config['DB'])

iso_codes = ['de', 'es']
query = 'INSERT INTO bb_item_information VALUES (%s, %s, %s, %s)'


def create_db_object(object):
    return object['id'], object['name'], object['sku'], object['isoCode']


products = db_handler.get_all_from_table('bb_items', 'id')

for iso_code in iso_codes:
    data = []
    for item in products:
        item_url = 'rest/catalog/productinformation/' + str(item[0]) + '.json?isoCode=' + iso_code
        item_info = bb_fetcher.fetch_data_from_bb(item_url)
        while 'code' in item_info:
            item_info = bb_fetcher.fetch_data_from_bb(item_url)
            sleep(5)
        data.append(item_info)
        db_handler.add_to_table(query, create_db_object(item_info))
    query_data = list(map(create_db_object, data))
    db_handler.write_to_table(query, query_data, 'bb_item_information')
