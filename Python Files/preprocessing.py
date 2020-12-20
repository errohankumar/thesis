import os
import pandas as pd
from datetime import datetime
from preprocessing_service import PreProcessingService


class PreProcessing(PreProcessingService):
    """
    A class used to automatically scrape CSV files from ENTSOE Transparecny Platform
    ...
    Attributes
    ----------
    _preProcessing : str
        A PreProcessingService instance for handling CSV files
    _preProcessing2: str
        A PreProcessingService instance for handling CSV files
    Methods
    -------
    find_time_column(df)
        A function to find the timestamp column for a given CSV files
    start_preprocess()
        Initializes pre-processing
    """

    def __init__(self):
        super().__init__()
        self._preProcessing = PreProcessingService()
        self._preProcessing2 = PreProcessingService()

    @staticmethod
    def find_time_column(df):
        """Finds the timestamp column for a given dataset.
        Parameters
        ----------
        df : DataFrame
            The dataset containing historical-observations
        Raises
        ------
        ValueError
            If no timestamp column is found.
        """

        for column in df.get_columns():
            try:
                found = datetime.strptime(df.df[column][0][:10], "%d.%m.%Y")
                return column
            except:
                continue

        raise ValueError("Timestamp column not found!")

    @staticmethod
    def last_date_for_column(df, column_name):
        """ Return last date of a specific column
        """

        return df[column_name].dropna().index[-1:][0]

    def start_preprocess(self):
        """Initiates a sequence of pre-processing tasks
        """

        # Get all the CSV files in the directory
        file_directory = os.getcwd() + "/download/"
        file_names = os.listdir(file_directory)

        actual_columns   = ['Solar  - Actual Aggregated [MW]',
                            'Wind Offshore  - Actual Aggregated [MW]',
                            'Wind Onshore  - Actual Aggregated [MW]',
                            'Actual Total Load [MW] - BZN|DE-LU']

        forecast_columns = ['Generation - Solar  [MW] Day Ahead/ BZN|DE-LU',
                            'Generation - Wind Offshore  [MW] Day Ahead/ BZN|DE-LU',
                            'Generation - Wind Onshore  [MW] Day Ahead/ BZN|DE-LU',
                            'Day-ahead Total Load Forecast [MW] - BZN|DE-LU']

        filter_columns =  ['Day-ahead Price [EUR/MWh]',
                           'Generation - Solar  [MW] Day Ahead/ BZN|DE-LU',
                           'Generation - Wind Offshore  [MW] Day Ahead/ BZN|DE-LU',
                            'Generation - Wind Onshore  [MW] Day Ahead/ BZN|DE-LU',
                            'Solar  - Actual Aggregated [MW]',
                            'Wind Offshore  - Actual Aggregated [MW]',
                          'Wind Onshore  - Actual Aggregated [MW]',
                          'Day-ahead Total Load Forecast [MW] - BZN|DE-LU',
                          'Actual Total Load [MW] - BZN|DE-LU']
        for i in range(0, len(file_names)):

            # First CSV is used as the main file to combine the rest of the files
            if i == 0:
                self._preProcessing.open_csv(file_directory + file_names[i])

                # Find time column and set it as index
                time_column = self.find_time_column(self._preProcessing)
                self._preProcessing.reformat_time(colname=time_column)
                self._preProcessing.rename_colname(time_column, "cet_timestamp")
                self._preProcessing.set_index(colname="cet_timestamp")

                # Convert arguments (non numeric values) to invalid NAN
                for j in self._preProcessing.get_columns():
                    self._preProcessing.df[j] = pd.to_numeric(self._preProcessing.df[j], errors='coerce')

                # Drop first duplicate
                self._preProcessing.drop_duplicates(keep="first", inplace=True)

                # Resample the quarterly values in the 'Total Load' and 'Actual Generation' to hourly
                if ("Actual" or "Load" or "Forecasts") in file_names[i]:
                    self._preProcessing = self._preProcessing.resample_rows()

            else:
                self._preProcessing2.open_csv(file_directory + file_names[i])

                # Find time column and set it as index
                time_column = self.find_time_column(self._preProcessing2)
                self._preProcessing2.reformat_time(colname=time_column)
                self._preProcessing2.rename_colname(time_column, "cet_timestamp")
                self._preProcessing2.set_index(colname="cet_timestamp")

                # Drop all the columns that are not numeric
                for column_index in self._preProcessing2.get_columns():
                    try:
                        float(self._preProcessing2.df[column_index][0])
                    except:
                        self._preProcessing2.df.drop(columns=column_index, inplace=True)
                        continue

                # Convert the non-numeric items to Nans
                # If ‘coerce’, then invalid parsing will be set as NaN.
                for column_name in self._preProcessing2.get_columns():
                    self._preProcessing2.df[column_name] = pd.to_numeric(self._preProcessing2.df[column_name],
                                                                         errors='coerce')

                # To resample quarter hour to hour if present
                self._preProcessing2 = self._preProcessing2.resample_rows()

                # concatenate all the columns in one single dataset
                print(self._preProcessing2.df)
                self._preProcessing.df = pd.concat([self._preProcessing.df, self._preProcessing2.df], axis=1,
                                                   sort=False)

        # Filter and drop all rows with four NAN values (to remove NAN values beyond latest time of available
        # day-ahead price)

        self._preProcessing.df = self._preProcessing.df[filter_columns].dropna(thresh=4, axis=0)
        print(self._preProcessing.df.columns)
        # Get three month of past observations
        if len(self._preProcessing.df) >= 2160:
            self._preProcessing.df = self._preProcessing.df[-2160:]

        # the missing values in the Actuals can be replaced by the generation forecast
        # the last date of the actuals column just before the missing values can be replaced with the values from the
        # last date of the forecast columns
        for column_name, forecast_column_name in zip(actual_columns, forecast_columns):
            last_date = self.last_date_for_column(self._preProcessing.df, column_name)
            self._preProcessing.df[column_name][last_date:] = self._preProcessing.df[forecast_column_name][
                                                              last_date:].values

        # Generate and add 'WeekDays' feature
        week_day_col = self._preProcessing.df.index.weekday
        self._preProcessing.df["WeekDays"] = week_day_col

        # Imputation through a interpolation method called 'cubic' to fulfill forecasting method criteria
        self._preProcessing = self._preProcessing.interpolate_ts()

        # Remove forecast columns, since they are not needed anymore
        self._preProcessing.remove_columns(cols = ['Generation - Solar  [MW] Day Ahead/ BZN|DE-LU',
                                                    'Generation - Wind Offshore  [MW] Day Ahead/ BZN|DE-LU',
                                                    'Generation - Wind Onshore  [MW] Day Ahead/ BZN|DE-LU',
                                                    'Day-ahead Total Load Forecast [MW] - BZN|DE-LU'])

        self._preProcessing.df.to_csv(os.getcwd() + "/download/final_dataset_kafka.csv")
