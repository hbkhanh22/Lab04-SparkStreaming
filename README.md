# Lab04 - Spark Streaming

This lab demonstrates the use of Spark Streaming with Kafka and MongoDB.

## 🐘 Start Zookeeper

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

## 🔁 Start Kafka Server

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

## 🗃️ Start MongoDB

```bash
mongosh --host localhost --port 27017
```

### 🔧 Useful `mongosh` Commands

```bash
show dbs       # List all databases
use your_db    # Switch to your desired database
```

## 📡 Subscribe to a Kafka Topic

To consume messages from the `btc-price` topic:

```bash
~/kafka/bin/kafka-console-consumer.sh \
  --topic btc-price \
  --from-beginning \
  --bootstrap-server localhost:9092
```