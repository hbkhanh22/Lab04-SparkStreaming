import requests
import json
import os
from confluent_kafka import Producer
from datetime import datetime, timezone
from typing import Dict, Any, List
import time


class BinanceFetcher:
    def __init__(self) -> None:
        """
        Initialize the BinanceFetcher with the symbol and URL for fetching BTC price.
        """
        self.symbol = "BTCUSDT"
        self.url = f"https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

    def fetch_price(self) -> Dict[str, Any]:
        """
        Fetch the current price of BTC from Binance API.
        """
        try:
            response = requests.get(self.url, timeout=3)  # Set a timeout for the request
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()

            if "symbol" in data and "price" in data:
                try:
                    price = float(data["price"])
                except ValueError:
                    raise ValueError("Price is not a valid float")
                
                return {
                    "symbol": data["symbol"],
                    "price": price,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }

            else:
                raise ValueError("Invalid response format.")
            
        except Exception as e:
            print(f"Error fetching data from Binance: {e}")
            return None


class KafkaProducer:
    """
    Kafka Producer for sending messages to a Kafka topic.
    """
    def __init__(self) -> None:
        self.topic = "btc-price"
        self.producer = Producer({"bootstrap.servers": "localhost:9092"})

    def produce(self, price_data: Dict[str, Any]) -> None:
        try:
            if price_data:
                msg = json.dumps(price_data)
                self.producer.produce(self.topic, key=price_data['symbol'], value=msg.encode('utf-8'))
                self.producer.flush()  # Ensure all messages are sent

        except Exception as e:
            print(f"Error producing message to Kafka: {e}")


class PriceCrawler:
    """
    Price Crawler for fetching and sending BTC price data to Kafka. 
    """
    def __init__(self, interval_ms: int = 100) -> None:
        self.fetcher = BinanceFetcher()
        self.producer = KafkaProducer()
        self.interval = interval_ms / 1000  # Convert milliseconds to seconds

    def run(self) -> None:
        print("Starting price crawler...")
        try:
            while True:
                price_data = self.fetcher.fetch_price()
                if price_data:
                    self.producer.produce(price_data)
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("Price crawler stopped by user.")



if __name__ == "__main__":
    crawler = PriceCrawler(interval_ms=100, )
    crawler.run()
