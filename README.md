# Workshop 003 ETL

# Prediction of Happiness using Kafka and Python

This project aims to demonstrate how to use Kafka and Python for happiness prediction. It provides data analysis and a basic prediction model using happiness data.

## Introduction

The project utilizes Kafka as a messaging platform for data streaming, and Python for data processing and analysis, as well as for building a happiness prediction model. It starts from the assumption that a better understanding of the factors influencing happiness can help improve people's quality of life.

## How to Run the Code

```bash
# Set up Environment with Docker
docker-compose up

# Access Kafka Container
docker exec -it kafka-workshop3 bash

# Create Topic in Kafka
kafka-topics --bootstrap-server kafka-workshop3:9092 --create --topic kafka-happiness-workshop

# Run Kafka Consumer
python kafka_consumer.py

# Run Kafka Producer
python feature_selection.py
