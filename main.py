from tokenize import group
import pandas as pd
import logging
import requests
import json

from functools import cache, cached_property

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

    def preprocess_inputs(self):
        # df = pd.read_csv('tran.csv')
        # self.df = df
        recs = self.df['vin'].to_list()
        print(type(recs))
        # inputs = [list(eval(rec)) for rec in recs]
        input_trxns = []
        # for r in inputs:
        for r in recs:
            txns = []
            for l in r:
                txns.append(l['txid'])
            input_trxns.append(txns)
        self.child_trxns = pd.DataFrame(input_trxns)


    def process_block(self, block_height):
        self.block = self.get_block(block_height)
        print(self.block)
        total_tran_count = self.block['tx_count']
        for i in range(25, total_tran_count, 25):
            self.get_block_transactions(i)
        
        self.df = pd.DataFrame(self.transactions)
        # df.to_csv('tran.csv')
        self.preprocess_inputs()
        df_child = self.child_trxns

        child_list = []

        for ix, row in self.df.iterrows():
            temp_df = df_child[df_child.isin([row['txid']]).any(axis=1)]
            if not temp_df.empty:
                child_list.append({
                    'id': row['txid'],
                    'children': list(temp_df.stack())
                })

        df1 = pd.DataFrame(child_list)
        df1.to_csv('processed_tran.csv')
        return 



obj = Transactions()
obj.process_block(680000)

