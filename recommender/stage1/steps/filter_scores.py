"""
Encode categorical column
"""
import pickle

class ScoreFilter():
    def __init__(
            self,
            min_score=10,
            column_name='score',
    ):
        self.min_score = min_score
        self.column_name = column_name

    def execute(self, df):
        col = df[self.column_name]
        return df[col >= self.min_score]

    def persist(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(f, data)

    def execute_persist(self, path, df):
        result = self.execute(df)
        self.persist(path, result)
        return result
