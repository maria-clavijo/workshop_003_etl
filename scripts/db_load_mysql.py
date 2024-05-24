import configparser
import mysql.connector
import pandas as pd


def create_connection():
    try:
        config = configparser.ConfigParser()
        config.read('db_config.ini')
        connection = mysql.connector.connect(
            host=config['mysql']['host'],
            user=config['mysql']['user'],
            password=config['mysql']['password']
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def create_table():
    create_db_query = "CREATE DATABASE IF NOT EXISTS dbhappiness"
    use_db_query = "USE dbhappiness"
    create_table_query = """
    CREATE TABLE IF NOT EXISTS happiness (
        id INT AUTO_INCREMENT PRIMARY KEY,
        gdp_per_capita FLOAT,
        social_family_support FLOAT,
        health_life_expectancy FLOAT,
        freedom FLOAT,
        perceptions_corruption FLOAT,
        generosity FLOAT,
        happiness_score FLOAT,
        happiness_prediction FLOAT
    )
"""
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(create_db_query)
            cursor.execute(use_db_query)
            cursor.execute(create_table_query)
            connection.commit()
            print('Table created successfully')
        except mysql.connector.Error as err:
            print(f"Error creating table: {err}")
        finally:
            cursor.close()
            connection.close()


def insert_data(row):
    insert_query = """
        INSERT INTO happiness (gdp_per_capita, social_family_support, health_life_expectancy, 
        freedom, perceptions_corruption, generosity, happiness_score, happiness_prediction)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("USE dbhappiness")
            cursor.execute(insert_query, row)
            connection.commit()
            print('Data inserted successfully')
        except mysql.connector.Error as err:
            print(f"Error when inserting the data: {err}")
        finally:
            cursor.close()
            connection.close()


if __name__ == "__main__":
    create_table()