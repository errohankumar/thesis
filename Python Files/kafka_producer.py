import os
import sys
import json
import pandas as pd

from config import *
from datetime import datetime
from datetime import timedelta
from confluent_kafka import Producer
from preprocessing import PreProcessing
from entsoe_downloader import EntsoeDownloader

def get_data():
    """Uses EntsoeDownloader and PreProcessing class to initiate the scraping and preprocessing process
    Parameters
    ----------
    Returns
    -------
    df
        A dataframe in form of json in order to dispatch to next services
    prices
        A list of electricity prices of the past 24 hours
    timestamps
        A list of timestamps of the past 24 hours and the next 24 hours (following day)
    """

    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("Todays date & type: ", date, " & ", type(date))

    # Initiate Web Scraping
    # Feed ENTSOE Account details into the EntsoeDownloader class
    downloader = EntsoeDownloader(date, "username", "password").setup(headless=True)
    downloader.login_and_download()

    # Initiate PreProcessing
    pre_processing = PreProcessing()
    pre_processing.start_preprocess()

    df = pd.read_csv(os.getcwd() + "/download/final_dataset_kafka.csv")

    # Get Day-Ahead price and generate new dates
    temp = df[[df.columns[0], "Day-ahead Price [EUR/MWh]"]].dropna()
    temp.rename(columns={df.columns[0]: "cet_timestamp"}, inplace=True)
    temp["cet_timestamp"] = pd.to_datetime(temp["cet_timestamp"], format="%Y-%m-%d %H:%M")
    temp.set_index("cet_timestamp", inplace=True)

    time = df[df.columns[0]][-24:].values
    last_date = temp.index[-1:][0]

    timestamp_list = list(time)
    for i in range(1, 25):
        last_date += timedelta(hours=1)
        timestamp_list.append(last_date.strftime("%Y-%m-%d %H:%M:%S"))

    df = df.to_json(orient="split")
    price_list = list(temp["Day-ahead Price [EUR/MWh]"][-24:].values)

    return df, price_list, timestamp_list

def delivery_report(err, msg):
    """Reports error message for abortive message delivery
    Parameters
    ----------
    err : str
        A string holding the reason for failure
    msg : bool, optional
        A flag used to print the columns to the console (default is
        False)
    """

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to topic "{}" in partition: [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":
    config = producer_local  # default option for producer
    topic = local_topic  # default topic for producer
    argv = sys.argv  # system argument vector

    """ 
    On a production environment this script should run with the 'global' and 'path of csv files' arguments
    """
    if len(argv) > 0 and argv[0] == "global":
        # pfile_name = argv[0]

        config = producer_global
        topic = global_topic

    df, prices, timestamps = get_data()

    kafka_data = {
        "dataset": df,
        "timestamps": timestamps,
        "day_ahead_price": prices
    }
    print(kafka_data)
    message_key = "Dataset"

    p = Producer(config)
    p.produce(topic, json.dumps(kafka_data), message_key, 0,
              callback=delivery_report)  # produce(topic, data, key, partition, callback)
   # produce(topic, data, key, partition, callback)
    p.flush()                            # Partition is set to 1 for each consumer to prevent overload