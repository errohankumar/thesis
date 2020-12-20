import pandas as pd
class PreProcessingService:
    """
     Class for methods to do variety of tasks
     ...
     Attributes
     ----------
     df : None

     Methods
     -------
     set_index(colname, colindex)
         Function for setting a column to an index
     get_column(index)
         Function for returning column name by index
     get_columns()
         Function for returning column names
    open_csv(path, columns, separator, names)
        Function to open and read csv files
    clean_nan_and_none()
        Function to drop from index
    rename_colname(colname, newcolname)
        Function for renaming the columns
    rename_colnames(coldict)
        Function to rename multiple column names
    interpolate_ts(method_type="cubic")
        Function for cubic interpolation
    drop_duplicates()
        Function to drop all the duplicates except the first ones
    reformat_time()
        Function for formatting time
     """
    def __init__(self, path=None, columns=None, separator=None):
        self.df = None

    def set_index(self, colname=None, colindex=None):
        if isinstance(colindex, int):
            self.df = self.df.set_index(self.df.columns[colindex])  # set time as index
        elif isinstance(colname, str):
            self.df = self.df.set_index(colname)  # set time as index

    def get_column(self, index):
        return self.df.columns[index]

    def get_columns(self):
        return self.df.columns

    def open_csv(self, path, columns=None, separator=None, names=None):
        if columns != "":
            self.df = pd.read_csv(path, delimiter=separator, usecols=columns, names=names)
        else:
            self.df = pd.read_csv(path, delimiter=separator, usecols=None, names=names)

        return self

    def clean_nan_and_none(self):
        if self.df.isna().sum().sum() == 0:
            return None

        self.df = self.df.dropna(axis=0)

        return self

    def rename_colname(self, colname, newcolname):

        self.df.rename(columns={colname: newcolname}, inplace=True)

        return self

    def rename_colnames(self, coldict):
        self.df.rename(columns=coldict, errors="raise", inplace=True)

        return self


    def drop_duplicates(self, keep="first", inplace=True):

        self.df = self.df[~self.df.index.duplicated(keep='first')]

        return self

    def interpolate_ts(self, method_type="cubic"):
        self.df = self.df.interpolate(method=method_type)

        return self

    def reformat_time(self, colname=None, colindex=None):

        if isinstance(colindex, int):
            self.df[self.df.columns[colindex]] = self.df[self.df.columns[colindex]].replace('\ -(.*)', '', regex=True)
            self.df[self.df.columns[colindex]] = pd.to_datetime(self.df[self.df.columns[colindex]],
                                                                format="%d.%m.%Y %H:%M")
        elif isinstance(colname, str):
            self.df[colname] = self.df[colname].replace('\ -(.*)', '', regex=True)
            self.df[colname] = pd.to_datetime(self.df[colname], format="%d.%m.%Y %H:%M")

        return self

    def remove_columns(self, cols):

        self.df = self.df.drop(columns = cols)

        return self

    def resample_rows(self, freq = 'H'):
        self.df = self.df.resample(freq).mean()

        return self

