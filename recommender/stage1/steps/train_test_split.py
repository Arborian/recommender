"""
Split events into training and test sets

Example usage:
>>> import pandas as pd
>>> from recommender.stage1.train_test_split import TrainTestSplit
>>> df = pd.read_csv('workflow_dev/mock_data/events.csv')
>>> step = TrainTestSplit(train_percentile=90)
>>> (df_train, df_test) = step.execute(df)
"""
import pickle
import numpy as np

class TrainTestSplit():
    """Workflow step that splits an event stream into two event streams based on a particular
    column in the stream.

    Init parameters:
        column_name: string representing the column to partition based on
        train_percentile: percentage of the values in the given column
            to be partitioned into the training set

    Execution Args:
        df: Pandas DataFrame containing an event stream with (at least) a column
            with the specified column name

    Execution Returns:
        Tuple of (training, test) dataframes

    Persist Strategy:
        Pickle the tuple of dataframes
    """

    def __init__(
            self,
            column_name='visitorid',
            train_percentile=90,
        ):
        self.column_name = column_name
        self.train_percentile = train_percentile

    def execute(self, df):
        col = df[self.column_name]
        split_value = np.percentile(col, self.train_percentile)
        train = df[col < split_value]
        test = df[col >= split_value]
        return train, test

    def persist(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(f, data)

    def execute_persist(self, path, df):
        result = self.execute(df)
        self.persist(path, result)
        return result