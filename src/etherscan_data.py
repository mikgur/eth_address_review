from datetime import datetime
from pathlib import Path
import pickle

import pandas as pd
import requests


class EtherscanData:
    def __init__(self, address: str, api_key: str,
                 etherscan_url: str = "https://api.etherscan.io/api"):
        self.api_key = api_key
        self.address = address
        self.etherscan_url = etherscan_url
        self.etherscan = EtherscanDataRetriever(api_key)
        self.load_transactions()

    def load_transactions(self):
        # transactions
        params = {
            "module": "account",
            "action": "txlist",
            "address": self.address,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "asc",
            "apikey": self.api_key
        }
        txs_list = self.etherscan.get_data(params)
        self.txs = txs_list

        # internal transactions
        params["action"] = "txlistinternal"
        internal_txs_list = self.etherscan.get_data(params)
        self.internal_txs = internal_txs_list

        # erc20 token transactions
        params["action"] = "tokentx"
        token_txs_list = self.etherscan.get_data(params)
        self.token_txs = token_txs_list

        # erc721 token transactions
        params["action"] = "tokennfttx"
        nft_txs_list = self.etherscan.get_data(params)
        self.nft_txs = nft_txs_list

        # erc1155 token transactions
        params["action"] = "token1155tx"
        erc155_txs_list = self.etherscan.get_data(params)
        self.erc1155_txs = erc155_txs_list

    @property
    def all_txs(self):
        return (self.txs + self.internal_txs + self.token_txs
                + self.nft_txs + self.erc1155_txs)

    def get_summary(self, prefix: str = ""):
        hashes = set()
        hashes_with_ts = []
        for tx in self.all_txs:
            if tx["hash"] in hashes:
                continue
            hashes.add(tx["hash"])
            hashes_with_ts.append((tx["hash"], int(tx["timeStamp"])))
        hashes_with_ts = sorted(hashes_with_ts, key=lambda x: x[1])

        data = pd.DataFrame.from_records(hashes_with_ts,
                                         columns=["hash", "timestamp"])
        data["datetime"] = data["timestamp"].apply(datetime.fromtimestamp)
        data["date"] = data["datetime"].apply(datetime.date)
        if prefix:
            normal = f"{prefix}_normal"
            has_internal = f"{prefix}_has_internal"
            erc20 = f"{prefix}_erc20"
            erc721 = f"{prefix}_erc721"
            erc1155 = f"{prefix}_erc1155"
        else:
            normal = "normal"
            has_internal = "has_internal"
            erc20 = "erc20"
            erc721 = "erc721"
            erc1155 = "erc1155"
        data[normal] = 0
        data[has_internal] = 0
        data[erc20] = 0
        data[erc721] = 0
        data[erc1155] = 0

        for tx in self.txs:
            n_txs = len([a for a in self.txs if a["hash"] == tx["hash"]])
            data.loc[data["hash"] == tx["hash"], normal] = n_txs
        for tx in self.internal_txs:
            n_txs = len([a for a in self.internal_txs
                         if a["hash"] == tx["hash"]])
            data.loc[data["hash"] == tx["hash"], has_internal] = n_txs
        for tx in self.token_txs:
            n_txs = len([a for a in self.token_txs if a["hash"] == tx["hash"]])
            data.loc[data["hash"] == tx["hash"], erc20] = n_txs
        for tx in self.nft_txs:
            n_txs = len([a for a in self.nft_txs if a["hash"] == tx["hash"]])
            data.loc[data["hash"] == tx["hash"], erc721] = n_txs
        for tx in self.erc1155_txs:
            n_txs = len([a for a in self.erc1155_txs
                         if a["hash"] == tx["hash"]])
            data.loc[data["hash"] == tx["hash"], erc1155] = n_txs
        return data


class EtherscanDataRetriever:
    def __init__(self, api_key: str,
                 etherscan_url: str = "https://api.etherscan.io/api",
                 cache_path: str = "data/cache"):
        self.api_key = api_key
        self.etherscan_url = etherscan_url
        self.cache_path = Path(cache_path)
        self.cache_path.mkdir(parents=True, exist_ok=True)
        self.account_actions = ["txlist", "txlistinternal",
                                "tokentx", "tokennfttx", "token1155tx"]

    def get_data(self, params):
        result = self.get_from_cache(params)
        if result is not None:
            return result

        response = requests.get(self.etherscan_url, params=params,
                                headers={'Content-Type': 'application/json'})
        result = response.json()["result"]
        self.update_cache(params, result)
        return result

    def get_from_cache(self, params):
        cache_name = f"{params['module']}_{params['action']}.pkl"
        cache_path = self.cache_path / cache_name
        if not cache_path.exists():
            return None
        with open(cache_path, "rb") as f:
            cache_data = pickle.load(f)
        if (params["module"] == "account"
                and params["action"] in self.account_actions):
            if params["address"] in cache_data:
                print(f"Found in cache: {params}")
                return cache_data[params["address"]]
        return None

    def update_cache(self, params, result):
        cache_name = f"{params['module']}_{params['action']}.pkl"
        cache_path = self.cache_path / cache_name
        if not cache_path.exists():
            cache_data = {}
        else:
            with open(cache_path, "rb") as f:
                cache_data = pickle.load(f)
        if (params["module"] == "account"
                and params["action"] in self.account_actions):
            cache_data[params["address"]] = result
        else:
            print(f"Cannot update cache for {params}")
        with open(cache_path, "wb") as f:
            pickle.dump(cache_data, f)
