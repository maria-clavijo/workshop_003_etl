from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
import joblib
import pandas as pd
from scripts.db_load_mysql import insert_data



file_path = '../model/rmforest_regressor.pkl'
model_rf = joblib.load(file_path)


def kafka_producer(row):
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092'],
    )
    message = row.to_dict()
    producer.send("kafka-happiness-workshop", value=message)
    producer.flush()
    print("Message sent")



def kafka_consumer():
    consumer = KafkaConsumer(
        'kafka-happiness-workshop',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
        )

    for message in consumer:
        df = pd.json_normalize(data=message.value)
        df['happiness_prediction'] = model_rf.predict(df[['gdp_per_capita', 'social_family_support', 
                                                          'health_life_expectancy', 'freedom', 
                                                          'perceptions_corruption', 'generosity']])
        insert_data(df.iloc[0])
        print("Data inserted into Database dbhappiness.")
        print("Data:\n", df)