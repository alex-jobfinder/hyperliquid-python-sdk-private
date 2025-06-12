import os
import yaml

def load_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
    return None

def print_assets(config):
    if config and 'addresses' in config:
        assets = config['addresses']
        print("Assets:")
        for asset in assets:
            print(f"- {asset}")
    else:
        print("No assets found in the configuration.")

# Get the file path for config.yml two levels up from the current directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(os.path.dirname(current_directory))
config_file_path = os.path.join(parent_directory, 'hyperliquid/hyperliquid-python-sdk/config_whales.yml')

# Load and print assets
config = load_config(config_file_path)
# print_assets(config)

from typing import Optional
from pydantic import BaseModel
from decimal import Decimal

### THIS IS THE FULL JSON
"""
{
  "user_id": "0x5078c2fbea2b2ad61bc840bc023e35fce56bedb6",
  "assetPositions": [
    {
      "position": {
        "coin": "ETH",
        "cumFunding": {
          "allTime": "514.085417",
          "sinceChange": "0.0",
          "sinceOpen": "0.0"
        },
        "entryPx": "2986.3",
        "leverage": {
          "rawUsd": "-95.059824",
          "type": "isolated",
          "value": 20
        },
        "liquidationPx": "2866.26936529",
        "marginUsed": "4.967826",
        "maxLeverage": 50,
        "positionValue": "100.02765",
        "returnOnEquity": "-0.0026789",
        "szi": "0.0335",
        "unrealizedPnl": "-0.0134"
      },
      "type": "oneWay"
    }
  ],
  "crossMaintenanceMarginUsed": "0.0",
  "crossMarginSummary": {
    "accountValue": "13104.514502",
    "totalMarginUsed": "0.0",
    "totalNtlPos": "0.0",
    "totalRawUsd": "13104.514502"
  },
  "marginSummary": {
    "accountValue": "13109.482328",
    "totalMarginUsed": "4.967826",
    "totalNtlPos": "100.02765",
    "totalRawUsd": "13009.454678"
  },
  "time": 1708622398623,
  "withdrawable": "13104.514502"
}
"""

## THIS IS THE PARTIAL JSON
##   "assetPositions": ['ETH', BTC'],
"""
    "user_id": "0xa44481a6454f4FD0899e261Aa941323f2b11A09b",
    "marginSummary": {
        "accountValue": "5973.448774",
        "totalNtlPos": "263604.530898",
        "totalRawUsd": "266601.589076",
        "totalMarginUsed": "6801.22743"
    },
    "crossMarginSummary": {
        "accountValue": "5973.448774",
        "totalNtlPos": "263604.530898",
        "totalRawUsd": "266601.589076",
        "totalMarginUsed": "6801.22743"
    },
    "crossMaintenanceMarginUsed": "3400.613712",
    "withdrawable": "0.0",
  "time": 1708622398623,
"""

## positions
"""
    "user_id": "0xa44481a6454f4FD0899e261Aa941323f2b11A09b",
  "time": 1708622398623,
    "assetPositions": [
        {
            "type": "oneWay",
            "position": {
                "coin": "BTC",
                "szi": "-2.49213",
                "leverage": {
                    "type": "cross",
                    "value": 40
                },
                "entryPx": "104362.7",
                "positionValue": "260776.4832",
                "unrealizedPnl": "-691.03839",
                "returnOnEquity": "-0.1062786717",
                "liquidationPx": "105659.6384884307",
                "marginUsed": "6519.41208",
                "maxLeverage": 40,
                "cumFunding": {
                    "allTime": "-440.955065",
                    "sinceOpen": "-0.013001",
                    "sinceChange": "0.0"
                }
            }
        },
        {
            "type": "oneWay",
            "position": {
                "coin": "ETH",
                "szi": "0.006",
                "leverage": {
                    "type": "cross",
                    "value": 25
                },
                "entryPx": "2462.1",
                "positionValue": "15.0828",
                "unrealizedPnl": "0.3102",
                "returnOnEquity": "0.5249583689",
                "liquidationPx": null,
                "marginUsed": "0.603312",
                "maxLeverage": 25,
                "cumFunding": {
                    "allTime": "-20.251126",
                    "sinceOpen": "0.001848",
                    "sinceChange": "0.001848"
                }
            }
        }
    ],
    "time": 1749221763901
"""

from typing import Optional, List
from pydantic import BaseModel
from decimal import Decimal


class CumFunding(BaseModel):
    allTime: Decimal
    sinceOpen: Decimal
    sinceChange: Decimal


class Leverage(BaseModel):
    type: str
    value: int
    rawUsd: Optional[Decimal] = None  # only present in isolated leverage


class Position(BaseModel):
    coin: str
    szi: Decimal
    leverage: Leverage
    entryPx: Decimal
    positionValue: Decimal
    unrealizedPnl: Decimal
    returnOnEquity: Decimal
    liquidationPx: Optional[Decimal]
    marginUsed: Decimal
    maxLeverage: int
    cumFunding: CumFunding


class AssetPositionEntry(BaseModel):
    type: str
    position: Position


class Summary(BaseModel):
    accountValue: Decimal
    totalMarginUsed: Decimal
    totalNtlPos: Decimal
    totalRawUsd: Decimal


class ClearinghouseState(BaseModel):
    user_id: str
    time: int
    marginSummary: Summary
    crossMarginSummary: Summary
    crossMaintenanceMarginUsed: Decimal
    withdrawable: Decimal
    assetPositions: List[AssetPositionEntry]

def parse_data(raw_data: dict) -> tuple[dict, list[dict]]:
    parsed = ClearinghouseState(**raw_data)

    # Flatten margin summary row
    margin_summary = {
        "user_id": parsed.user_id,
        "time": parsed.time,
        "account_value": parsed.marginSummary.accountValue,
        "total_ntl_pos": parsed.marginSummary.totalNtlPos,
        "total_raw_usd": parsed.marginSummary.totalRawUsd,
        "total_margin_used": parsed.marginSummary.totalMarginUsed,
        "cross_maintenance_margin_used": parsed.crossMaintenanceMarginUsed,
        "withdrawable": parsed.withdrawable,
    }

    # Flatten asset-level rows
    asset_positions = []
    for ap in parsed.assetPositions:
        p = ap.position
        asset_positions.append({
            "user_id": parsed.user_id,
            "time": parsed.time,
            "coin": p.coin,
            "szi": p.szi,
            "leverage_type": p.leverage.type,
            "leverage_value": p.leverage.value,
            "leverage_raw_usd": p.leverage.rawUsd if p.leverage.rawUsd is not None else None,
            "entry_px": p.entryPx,
            "position_value": p.positionValue,
            "unrealized_pnl": p.unrealizedPnl,
            "return_on_equity": p.returnOnEquity,
            "liquidation_px": p.liquidationPx,
            "margin_used": p.marginUsed,
            "max_leverage": p.maxLeverage,
            "cum_funding_all_time": p.cumFunding.allTime,
            "cum_funding_since_open": p.cumFunding.sinceOpen,
            "cum_funding_since_change": p.cumFunding.sinceChange,
        })

    return margin_summary, asset_positions


import sqlite3

DB_PATH = "/home/alex/prod_trading/hyperliquid/" \
          "hyperliquid-python-sdk/hyperliquid_etl/data/pnl.db"

class SqlLogger:
    def __init__(self, table: str, columns: list[str]):
        self.table = table
        self.columns = columns
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.ensure_table()  # must exist inside class

    def ensure_table(self):
        cols = ", ".join(
            f"{col} TEXT" for col in self.columns
        )
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({cols})"
        self.conn.execute(create_sql)
        self.conn.commit()

    def insert_row(self, values: dict):
        keys = [k for k in self.columns if k in values]
        cols = ", ".join(keys)
        qmarks = ", ".join(["?"] * len(keys))
        vals = [str(values[k]) for k in keys]
        sql = f"INSERT INTO {self.table} ({cols}) VALUES ({qmarks})"
        self.conn.execute(sql, vals)
        self.conn.commit()

        
"""
curl -X POST https://api.hyperliquid.xyz/info \                                                                                                                                               ─╯
  -H "Content-Type: application/json" \
  -d '{
    "type": "clearinghouseState",
    "user": "0xa44481a6454f4FD0899e261Aa941323f2b11A09b"
  }'

"""


import requests
from typing import Optional
from time import sleep


class HyperliquidClient:
    URL = "https://api.hyperliquid.xyz/info"
    

    @staticmethod
    def fetch_state(
        query_type: str,
        user: Optional[str] = None
    ) -> Optional[dict]:
        payload = {"type": query_type}
        if user:
            payload["user"] = user
        try:
            response = requests.post(
                HyperliquidClient.URL,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            json_data = response.json()
            print("Full response JSON:", json_data)  # 👈 print full response
            return json_data  # ✅ return full JSON, not json_data.get("response")
        except requests.RequestException as e:
            print(f"API request failed: {e}")
            return None



    def ensure_table(self):
        cols = ", ".join(
            f"{col} TEXT" for col in self.columns
        )
        create_sql = f"CREATE TABLE IF NOT EXISTS {self.table} ({cols})"
        self.conn.execute(create_sql)
        self.conn.commit()

    def insert_row(self, values: dict):
        keys = [k for k in self.columns if k in values]
        cols = ", ".join(keys)
        qmarks = ", ".join(["?"] * len(keys))
        vals = [str(values[k]) for k in keys]
        sql = f"INSERT INTO {self.table} ({cols}) VALUES ({qmarks})"
        self.conn.execute(sql, vals)
        self.conn.commit()


def write_to_sqlite(margin: dict, assets: list[dict]):
    margin_logger = SqlLogger(
        table="margin_summary",
        columns=list(margin.keys())
    )
    margin_logger.insert_row(margin)

    if assets:
        asset_logger = SqlLogger(
            table="asset_positions",
            columns=list(assets[0].keys())
        )
        for asset in assets:
            asset_logger.insert_row(asset)


import json
from decimal import Decimal

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def print_json(obj):
    print(json.dumps(obj, indent=2, default=decimal_default))

def main(user: str):
    data = HyperliquidClient.fetch_state(
        query_type="clearinghouseState",
        user=user
    )
    if data:
        # Inject user_id so the model has it
        data["user_id"] = user
        
        # Parse into models
        margin, assets = parse_data(data)

        print("\n--- Margin Summary ---")
        print(json.dumps(margin, indent=2, default=decimal_default))

        print("\n--- Asset Positions ---")
        for asset in assets:
            print_json(asset)

        # Write to SQLite
        write_to_sqlite(margin, assets)
    else:
        print("No usable data received.")


if __name__ == "__main__":
    user_ids = [
        "0x5b5d51203a0f9079f8aeb098a6523a13f298c060",
        "0xb83de012dba672c76a7dbbbf3e459cb59d7d6e36",
        "0xcb92c5988b1d4f145a7b481690051f03ead23a13",
        "0x1d52fe9bde2694f6172192381111a91e24304397",
        "0x880ac484a1743862989a441d6d867238c7aa311c",
        "0x10f1d87fbb7617df5641d1c2bcb26f143f43202f",
        "0xa312114b5795dff9b8db50474dd57701aa78ad1e",
        "0x162cc7c861ebd0c06b3d72319201150482518185",
        "0xbca629222b99714a54e51bd6d2edfbdbab72ddfe"
    ]

    for user_id in user_ids:
        print(f"\n--- Fetching state for {user_id} ---")
        main(user_id)

    def preview_db(table: str, limit=5):
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute(
            f"SELECT * FROM {table} ORDER BY rowid DESC LIMIT {limit}"
        ).fetchall()
        for row in rows:
            print(row)
        conn.close()
