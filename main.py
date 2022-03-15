from tokenize import group
import pandas as pd
import logging
import requests
import json

from functools import cache, cached_property

from collections import defaultdict

logger = logging.getLogger(__name__)

API_ENDPOINT = "https://blockstream.info/api"

class Transactions:
    def __init__(self):
        self.api = API_ENDPOINT
        self.transactions = []
    
    @staticmethod
    def chunklist(records: list, chunksize: int) -> list:
        for i in range(0, len(records), chunksize):
            yield records[i: i + chunksize]

    @cache
    def get_block(self, block_height):
        try:
            url = f"{self.api}/blocks/{block_height}"
            r = requests.get(url)
            if r.status_code == 200:
                res_data = r.json()[0]
                self.block_hash = res_data['id']
                return res_data
            else:
                raise ValueError("Error")
        except Exception as e:
            logger.exception(e)
            raise ValueError(e)

    @cache
    def get_block_transactions(self,start_index):
        try:
            url = f"{self.api}/block/{self.block_hash}/txs/{start_index}"
            print(url)
            r = requests.get(url)
            if r.status_code == 200:
                res_data = r.json()
                self.transactions.extend(res_data)
            else:
                raise ValueError(r.content)
        except Exception as e:
            logger.exception(e)

    def process_block(self, block_height):
        self.block = self.get_block(block_height)
        print(self.block)
        total_tran_count = self.block['tx_count']
        for i in range(25, total_tran_count, 25):
            self.get_block_transactions(i)
        
        df = pd.DataFrame(self.transactions)                
        df1 = df.explode('vin')
        print(df1.columns)
        df1['parent'] = df1['vin'].apply(lambda x: x['txid'])
        df2 = df1[['txid','parent']]
        df2['exists'] = df2.txid.isin(df2.parent)
        df3 = df2[df2['exists'] == True]
        final_trans = defaultdict(int)

        for ix, row in df3.iterrows():
            if (df3['txid'] == row['parent']).any():
                final_trans[row['txid']] += 1

        sorted_trans = sorted(final_trans.items(),key=lambda x: x[1], reverse=True)

        return sorted_trans[:10]

obj = Transactions()
max_ancestors = obj.process_block(680000)

print(max_ancestors)

