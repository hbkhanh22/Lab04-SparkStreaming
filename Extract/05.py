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
                # self.producer.flush()  # Ensure all messages are sent

        except Exception as e:
            print(f"Error producing message to Kafka: {e}")

    def flush(self) -> None:
        """
        Expose the underlying producer's flush method.
        """
        self.producer.flush()

class PriceCrawler:
    """
    Price Crawler for fetching and sending BTC price data to Kafka. 
    """
    def __init__(self, interval_ms: int = 100) -> None:
        self.fetcher = BinanceFetcher()
        self.producer = KafkaProducer()
        self.interval = interval_ms / 1000  # Convert milliseconds to seconds
        self.start_time = datetime.now(timezone.utc)
    def run(self) -> None:
        print("Starting price crawler...")
        try:
            while True:
                self.current_time = datetime.now(timezone.utc)
                start = time.time()
                price_data = self.fetcher.fetch_price()
                end = time.time()
                print(f"Time taken to fetch price: {end - start} seconds")
                if price_data:
                    self.producer.produce(price_data)
                    print(f"Produced message: {price_data} at {datetime.now(timezone.utc).isoformat()}")
                if (self.current_time - self.start_time).total_seconds() >= self.interval:
                    self.start_time = self.current_time
                    self.producer.flush()
        except KeyboardInterrupt:
            print("Price crawler stopped by user.")

if __name__ == "__main__":
    crawler = PriceCrawler(interval_ms=100)
    crawler.run()
