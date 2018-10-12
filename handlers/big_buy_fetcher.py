import json

import requests


class BigBuyFetcher:
    bb_url = 'https://api.bigbuy.eu/'
    bb_header = ''
    cursor = {}

    def __init__(self, config):
        self.bb_header = {'Authorization': 'Bearer ' + config['bigbuy_api_key']}

    def fetch_data_from_bb(self, url):
        bb_url = self.bb_url + url
        data = json.loads(requests.get(bb_url, headers=self.bb_header).text)
        return data

