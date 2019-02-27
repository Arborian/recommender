"""
Score events based on a categorical column

Example usage:
>>> import pandas as pd
>>> from recommender.stage1.score_events import ScoreEvents
>>> df = pd.read_csv('workflow_dev/mock_data/events.csv')
>>> step = ScoreEvents()
>>> df_scored = step.execute(df)
"""
import pandas as pd


class ScoreEvents():
    """Workflow step that splits an event stream into two event streams based on a particular
    column in the stream.

    Init parameters:
        input_column_name: string representing the column with categorical data
        score_column_name: name of the new column to create
        score_map: mapping from categories in the column_name to score values

    Execution Args:
        df: Pandas DataFrame containing an event stream with (at least) a column
            with the specified column name

    Execution Returns:
        New dataframe with 'score' column

    Persist Strategy:
        Store a csv of the dataframe
    """

    def __init__(
            self,
            input_column_name='event',
            score_column_name='score',
            score_map=None,
    ):
        self.input_column_name = input_column_name
        self.score_column_name = score_column_name
        if score_map is None:
            score_map = {
                'view': 1,
                'addtocart': 5,
                'transation': 10,
            }
        self.score_map = score_map

    def execute(self, df):
        col = df[self.input_column_name]
        score = col.apply(
            lambda value: self.score_map.get(value, 0)
        ).rename(self.score_column_name)
        return pd.concat([df, score], axis=1)

    def persist(self, path, data):
        data.to_csv(path, index=False)

    def execute_persist(self, df, path):
        result = self.execute(df)
        self.persist(path, result)
        return result
