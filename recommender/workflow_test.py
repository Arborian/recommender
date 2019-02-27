from unittest import TestCase


import recommender.constants as C
from recommender.stage1.main import Stage1


class TestDevWorkflow(TestCase):

    def setUp(self):
        self.config_stage1 = {
            'data_loader': {
                'event_filename': 'recommender/mock_data/events.csv',
            },
            'score_events': {
                'input_column_name': C.EVENT_COLUMN_NAME,
                'score_column_name': C.SCORE_COLUMN_NAME,
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

    def test_stage_1(self):
        stage = Stage1(self.config_stage1)
        result = stage.execute()
