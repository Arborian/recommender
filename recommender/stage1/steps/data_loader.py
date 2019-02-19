"""
Load event data

Example usage:
>>> from recommender.stage1.data_loader import DataLoader
>>> step = DataLoader(event_filename='mock_data/events.csv')
>>> df = step.execute()
"""
import pickle
import numpy as np

class DataLoader():
    """Workflow step that loads event data.

    Init parameters:
        event_filename: filename for event data

    Execution Args:
        none

    Execution Returns:
        New dataframe

    Persist Strategy:
        Store a csv of the dataframe
    """

    def __init__(
            self,
            event_filename,
        ):
        self.event_filename = event_filename

    def execute(self):
        return pd.read_csv(self.event_filename)

    def persist(self, path, data):
        data.to_csv(path, index=False)

    def execute_persist(self, df, path):
        result = self.execute(df)
        self.persist(path, result)
        return result