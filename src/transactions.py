from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from .etherscan_data import EtherscanData


def token_txs_by_tx_hash(eth_data, hash):
    return [a for a in eth_data.token_txs if a["hash"] == hash]


def internal_txs_by_tx_hash(eth_data, hash):
    return [a for a in eth_data.internal_txs if a["hash"] == hash]


@dataclass
class AssetMovement:
    name: str
    value: float
    from_address: str
    to_address: str


class TransactionResult:
    def __init__(self, hash: str, ts: int,
                 from_address: str = "", to_address: str = ""):
        self.hash = hash
        self.ts = ts
        self.from_address = from_address
        self.to_address = to_address
        self.contract = ""
        self.function = ""
        self.external = False
        self.eth_movements = []
        self.tokens_movements = []
        self.error = False

    def add_token_movement(self, token_name: str, value: float,
                           from_address: str, to_address: str):
        movements = AssetMovement(token_name, value, from_address, to_address)
        self.tokens_movements.append(movements)

    def add_eth_movement(self, value: float,
                         from_address: str, to_address: str):
        movements = AssetMovement("ETH", value, from_address, to_address)
        self.eth_movements.append(movements)

    @property
    def datetime(self):
        return datetime.fromtimestamp(self.ts)


class TransactionExplorer:
    def __init__(self,
                 address_etherscan_data: EtherscanData,
                 ds_proxy_data: EtherscanData,
                 address_book: dict,
                 known_contracts: dict):
        self.address_data = address_etherscan_data
        self.ds_proxy_data = ds_proxy_data
        self.address_book = address_book
        self.known_contracts = known_contracts

    def get_address_name(self, hash: str):
        return self.address_book.get(hash, hash)

    def make_token_message(self, token_tx):
        value = float(token_tx["value"]) / 10 ** int(token_tx["tokenDecimal"])
        token_name = token_tx["tokenName"]
        token_from = self.get_address_name(token_tx["from"])
        token_to = self.get_address_name(token_tx["to"])
        if token_from == "ZERO Address":
            return f"{value:.3f} {token_name} minted to {token_to}"
        return f"{value:.3f} {token_name} transferred from {token_from} to {token_to}" # noqa E501

    def contract_explorer(self, contract_hash: str):
        def decorator(func):
            def transaction_explorer(*args, **kwargs):
                print(f"{self.address_book[contract_hash]} - {contract_hash}")
                return func(*args, **kwargs)
            return transaction_explorer
        return decorator

    def process_transaction(self, tx_row: pd.Series, n: int = 0):
        hash = tx_row["hash"]
        result = TransactionResult(tx_row["hash"], tx_row["timestamp"])
        if tx_row["normal"] < 1:
            result.external = True
            token_txs = token_txs_by_tx_hash(self.address_data, tx_row["hash"])
            for token_tx in token_txs:
                value = float(token_tx["value"])
                value = value / 10 ** int(token_tx["tokenDecimal"])
                token_name = token_tx["tokenSymbol"]
                token_from = self.get_address_name(token_tx["from"])
                token_to = self.get_address_name(token_tx["to"])
                result.add_token_movement(
                    token_name, value, token_from, token_to)
            internal_txs = internal_txs_by_tx_hash(
                self.address_data, tx_row["hash"])
            for internal_tx in internal_txs:
                value = float(internal_tx["value"]) / 10 ** 18
                internal_from = self.get_address_name(internal_tx["from"])
                internal_to = self.get_address_name(internal_tx["to"])
                result.add_eth_movement(value, internal_from, internal_to)
            return result
        tx = [a for a in self.address_data.txs if a["hash"] == hash][0]
        result.from_address = self.get_address_name(tx["from"])
        result.to_address = self.get_address_name(tx["to"])
        # Eth movement
        if tx["input"] == "0x":
            value = float(tx["value"]) / 10 ** 18
            result.add_eth_movement(
                value,
                self.get_address_name(tx["from"]),
                self.get_address_name(tx["to"])
                )
            return result
        if self.get_address_name(tx["from"]) == "ADDRESS_OF_INTEREST":
            if tx["to"] not in self.known_contracts:
                print(f"NEW CONTRACT: {tx['to']}")
                print(tx)
                return
            return self.process_contract_transaction(tx_row, tx, result)
        return result

    def process_contract_transaction(self, tx_row: pd.Series, tx: dict,
                                     result: TransactionResult):
        contract_name = self.address_book[tx["to"]]
        result.contract = contract_name
        if contract_name == "Maker: Proxy Registry":
            return result
        # check for ETH sent to contract
        func_name = tx["functionName"].split("(")[0]
        result.function = func_name
        msg = f"Transaction with {contract_name} - {func_name}:"
        if int(tx["isError"]) > 0:
            result.error = True
            return result
        value = float(tx["value"]) / 10**18
        if value > 0:
            msg = "\n".join([msg, f"OUTFLOW ETH {value:0.3f} to DSProxy"])
            result.add_eth_movement(
                value,
                self.get_address_name(tx["from"]),
                self.get_address_name(tx["to"])
                )
        # token txes
        token_txs = token_txs_by_tx_hash(self.address_data, tx_row["hash"])
        internal_txs = internal_txs_by_tx_hash(
            self.address_data, tx_row["hash"])
        ds_token_txs = token_txs_by_tx_hash(self.ds_proxy_data, tx_row["hash"])
        if len(token_txs) + len(ds_token_txs) == 0:
            msg = f"{msg} no tokens transactions!"
        for token_tx in token_txs:
            value = float(token_tx["value"])
            value = value / 10 ** int(token_tx["tokenDecimal"])
            token_name = token_tx["tokenSymbol"]
            token_from = self.get_address_name(token_tx["from"])
            token_to = self.get_address_name(token_tx["to"])
            result.add_token_movement(
                token_name, value, token_from, token_to)
        # Internal txes
        for internal_tx in internal_txs:
            value = float(internal_tx["value"]) / 10 ** 18
            internal_from = self.get_address_name(internal_tx["from"])
            internal_to = self.get_address_name(internal_tx["to"])
            result.add_eth_movement(value, internal_from, internal_to)
        # DSProxy token txes
        for token_tx in ds_token_txs:
            value = float(token_tx["value"])
            value = value / 10 ** int(token_tx["tokenDecimal"])
            token_name = token_tx["tokenSymbol"]
            token_from = token_tx["from"]
            token_to = token_tx["to"]
            from_address = self.get_address_name(token_from)
            if from_address == "ADDRESS_OF_INTEREST":
                continue
            to_address = self.get_address_name(token_to)
            if to_address == "ADDRESS_OF_INTEREST":
                continue
            result.add_token_movement(
                token_name, value, from_address, to_address)
        return result
