"""
Encode categorical column
"""
import pickle
from sklearn import preprocessing

class ColumnEncoder():
    def __init__(
            self,
            column_name='colname',
        ):
        self.column_name = column_name

    def execute(self, df):
        enc = preprocessing.LabelEncoder()
        new_col = enc.fit_transform(df[self.column_name])
        new_df = df.drop(column_name, axis=1)
        return {
            'encoder': enc,
            'df': pd.concat([new_df, new_col]),
        }

    def persist(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(f, data)

    def execute_persist(self, path, df):
        result = self.execute(df)
        self.persist(path, result)
        return result