"""
Load data, extract features, and prepare for fitting.

Example usage:
>>> from recommender.workflow_dev.workflow_stage1 import Stage1
>>> st1 = Stage1(config)
>>> st1.execute_persist()
"""
import pickle

from recommender.stage1.steps.data_loader import DataLoader
from recommender.stage1.steps.score_events import ScoreEvents
from recommender.stage1.steps.group_scores import ScoreGrouper
from recommender.stage1.steps.filter_scores import ScoreFilter
from recommender.stage1.steps.column_encoder import ColumnEncoder


class Stage1():
    """This stage loads the event and fits the scoring model.

    It consists of the following steps:
    - load the data
    - scoring events
    - group scores by visitorid/itemid
    - finding relevant visitors
    - encoding items for relevant visitors with a LabelEncoder

    Example config:
    cfg = {
        'data_loader': {
            'event_filename': 'mock_data/events.csv',
        },
        'score_events': {
            'input_column_name': 'event',
            'score_column_name': 'score',
            'score_map': {
                    'view': 1,
                    'addtocart': 5,
                    'transaction': 10,
            },
        },
        'encode_itemids': {
            'column_name': 'itemid',
        },
        'group_scores': {
            'key': 'visitorid itemid'.split(),
        },
        'score_filter': {
            'min_score': 10,
        },
    }

    Execution Args:
        None

    Execution Returns:
        Python Dictionary with the following keys:
            - item_encoder: LabelEncoder() with encoded relevant itemids
            - scores: Pandas Series indexed by (visitorid, encoded itemid) with the score
                for each relevant visitorid/item
        }

    Persist strategy:
        Pickle the result
    """

    def __init__(self, config):
        self.config = config

    def _load_data(self):
        step = DataLoader(**self.config.get('data_loader', {}))
        return step.execute()

    def _score_events(self, df_events):
        step = ScoreEvents(**self.config.get('score_events', {}))
        return step.execute(df_events)

    def _group_scores(self, df_scored_events):
        step = ScoreGrouper(**self.config.get('group_scores', {}))
        return step.execute(df_scored_events)

    def _filter_scores(self, s_grouped_scores):
        step = ScoreFilter(**self.config.get('event_filter', {}))
        return step.execute(s_grouped_scores)

    def _encode_itemids(self, s_filtered_scores):
        step = ColumnEncoder(**self.config.get('encode_itemids', {}))
        return step.execute(s_filtered_scores)

    def execute(self):
        df_events = self._load_data()
        df_scored_events = self._score_events(df_events)
        s_grouped_scores = self._group_scores(df_scored_events)
        df_filtered_scores = self._filter_scores(s_grouped_scores.reset_index())
        d_encoder = self._encode_itemids(df_filtered_scores)
        return {
            'item_encoder': d_encoder['encoder'],
            'scores': d_encoder['df'],
        }

    def persist(self, path, data):
        with open(path, 'wb') as f:
            pickle.dump(f, data)

    def execute_persist(self, path):
        data = self.execute()
        self.persist(path, data)
        return data

