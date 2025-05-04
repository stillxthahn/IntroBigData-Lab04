# Lab 04: Spark Streaming

## Prerequisites
- Docker 26.1.1 (or higher)
- Python 3.x

## How-to guide
### 1. Install requirements
- Install python packages:
```python
pip install requirements.txt
```
### 2. Kafka Setup
- This lab uses Kafka for streaming BTC price data. We use the official [Apache Kafka Docker image](https://hub.docker.com/r/apache/kafka/).

- The script will:

 - Check if Docker is installed.

 - Pull the latest Apache Kafka image.

 - Run a Kafka container.

 - Create the following topics: **btc-price**, **btc-price-moving** and **btc-price-zscore**.

- To set up Kafka, simply run:
```bash
.\src\Extract\kafka_setup.bat
```
---

<p>&copy; 2025</p>