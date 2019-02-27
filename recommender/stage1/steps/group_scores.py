"""
Encode categorical column
"""
import pickle


class ScoreGrouper():
    def __init__(
            self,
            key='groupkey',
            column_name='score',
    ):
        self.key = key
        self.column_name = column_name

    def execute(self, df):
        return df.groupby(self.key)[self.column_name].sum()

    def persist(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(f, data)

    def execute_persist(self, path, df):
        result = self.execute(df)
        self.persist(path, result)
        return result
