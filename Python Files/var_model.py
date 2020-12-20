"""VAR Forecasting
functions:
    * VAR - returns a list of predicted electricity prices for next 24 hours
    * delivery_report - callback function to handle failed message deliveries
    * main - the main function of the script
"""

import sys
import json
import pandas as pd

from config import *
from datetime import datetime
from datetime import timedelta
from confluent_kafka import Consumer
from confluent_kafka import Producer
from confluent_kafka import TopicPartition
from statsmodels.tsa.api import VAR


def invert_transformation(df_train, df_forecast, second_diff=False):
    """Revert back the differencing to get the forecast to original scale."""

    df_fc = df_forecast.copy()
    columns = df_train.columns
    for col in columns:
        # Roll back 2nd Diff
        if second_diff:
            df_fc[str(col)] = (df_train[col].iloc[-1]-df_train[col].iloc[-2]) + df_fc[str(col)].cumsum()
        # Roll back 1st Diff
        df_fc[str(col)] = df_train[col].iloc[-1] + df_fc[str(col)].cumsum()
    return df_fc

def VAR_forecast(df):
    """Initiates Forecasting using Forecast on the passed dataset
    Parameters
    ----------
    df
        The dateset containing historical-observations on day-ahead prices
    Returns
    -------
    forecasted
        A list of forecasted electricity prices for the next 24 hours
    """

    column_name = "Day-ahead Price [EUR/MWh]"

    # Open CSV File and set timestamp column as index
    df.rename(columns={df.columns[0]: "cet_timestamp"}, inplace=True)
    df["cet_timestamp"] = pd.to_datetime(df["cet_timestamp"], format="%Y-%m-%d %H:%M")
    df.set_index("cet_timestamp", inplace=True)

    # Impute and get only one column
    df_diff = df.diff().dropna()

    # Generate new dates
    dates = list()
    last_date = df.index[-1:][0]
    for i in range(1, 25):
        last_date += timedelta(hours=1)
        dates.append(last_date.strftime("%Y-%m-%d %H:%M:%S"))

    var_model = VAR(df_diff).fit(26)
    var_forecast = var_model.forecast(y = var_model.y, steps=24)
    var_forecast_df = pd.DataFrame(var_forecast, columns=df.columns, index= dates)
    var_forecast_df = invert_transformation(df, var_forecast_df)
    '''
    # For mean absolute error
    last_24hours = last_date - timedelta(hours=24)
    # History - 24hours
    history = df_diff[df_diff.index <= last_24hours]
    var_mae_model = VAR(history).fit(26)
    var_mae_forecast = var_mae_model.forecast(y = var_mae_model.y, steps=24)
    mae = mean_absolute_error(history['Day-ahead Price [EUR/MWh]'].values, np.array(var_mae_forecast))
    print(mae)
    '''
    return list(var_forecast_df[column_name].values), datetime.now().strftime("%Y-%m-%d")


def delivery_report(err, msg):
    """Reports error message for abortive message delivery
    Parameters
    ----------
    err : str
        A string holding the reason for failure
    msg : bool, optional
        A flag used to print the columns to the console (default is
        False)
    Returns
    -------
    """

    # Called once for each message produced to indicate delivery result.
    #   Triggered by poll() or flush().

    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


if __name__ == "__main__":
    p_config = producer_local  # default option for producer
    c_config = consumer_var_local  # default option for consumer
    topic = local_topic  # default topic for producer
    argv = sys.argv  # system argument vector

    if len(argv) > 1 and argv[1] == "global":
        pfile_name = argv[0]
        option = argv[1]

        p_config = producer_global  # default option for producer
        c_config = consumer_global  # default option for consumer
        topic = global_topic

    # Kafka Producer
    p = Producer(p_config)

    # Kafka Consumer
    c = Consumer(c_config)
    c.subscribe([topic])

    low, high = c.get_watermark_offsets(TopicPartition(topic, partition=0))
    print("low offset: ", low)
    print("high offset: ", high)

    c.assign([TopicPartition(topic, partition=0, offset=high)])

    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        if msg.key() is None:
            print('Received message \nKey: {} \nValue: {}'.format(msg.key().decode('utf-8'),
                                                                  len(msg.value().decode("utf-8"))))

        # Process received message and forecast the next 24 hours
        if msg.key() != None and msg.key().decode("utf-8") == "Dataset":
            print('Received message \nValue: {} \nValue_Length: {}'.format(msg.value().decode("utf-8"),
                                                                           len(msg.value().decode("utf-8"))))
            json_df = json.loads(msg.value().decode("utf-8"))["dataset"]
            print(len(json_df))

            forecasted, delivery_date = VAR_forecast(pd.read_json(json_df, orient="split"))

            var_data ={
                "forecast": forecasted,
                "delivery_date": delivery_date
            }

            # produce(topic, data, key, callback)
            p.produce(topic, json.dumps(var_data), "VAR", callback=delivery_report)
            p.flush()

    c.close()