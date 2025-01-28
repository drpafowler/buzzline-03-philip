"""
csv_consumer_philip_v2.py

Consume json messages from a Kafka topic and process them.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", 'HourlyDryBulbTemperature': 22.0, 'HourlyWetBulbTemperature': 18.0}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json
from collections import deque

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Import modules for Calculation of Dewpoint
import psychrolib as psy

# Import modules for plotting - mostly moved to separate app.py
import datetime as dt
import pandas as pd

#####################################
# Load Environment Variables
#####################################

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Function to process a single message
######################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message.
    
    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of dry and wet bulb  readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        drybulb_f = data.get("HourlyDryBulbTemperature")
        wetbulb_f = data.get("HourlyWetBulbTemperature")
        pressure_hg = data.get("HourlyStationPressure")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if drybulb_f is None or wetbulb_f is None or pressure_hg is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        #Convert the temperatures to Celsius
        drybulb_c = (drybulb_f - 32) * 5/9
        wetbulb_c = (wetbulb_f - 32) * 5/9

        # Convert the pressure to HectoPascals
        pressure_hpa = pressure_hg * 3386.389

        # Append the temperature reading to the rolling window
        rolling_window.append(drybulb_c)
        rolling_window.append(wetbulb_c)
        rolling_window.append(pressure_hpa)

        # Parse the JSON string into a dataframe
        # Define the CSV file path
        csv_file_path = 'data/hourly_data_received.csv'

        # Check if the CSV file exists
        file_exists = os.path.isfile(csv_file_path)

        # Create a DataFrame from the message data
        df = pd.DataFrame({
            'timestamp': [timestamp],
            'drybulb_c': [drybulb_c],
            'wetbulb_c': [wetbulb_c],
            'pressure_hpa': [pressure_hpa]
        })

        # Append the data to the CSV file, create the file if it does not exist
        df.to_csv(csv_file_path, mode='a', header=not file_exists, index=False)


    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls and processes messages from the Kafka topic.
    - Sets up and runs the plot animation.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
