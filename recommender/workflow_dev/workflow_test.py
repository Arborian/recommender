from unittest import TestCase

import numpy as np
import pandas as pd

from recommender.workflow_dev.workflow_stage1 import Stage1

class TestDevWorkflow(TestCase):

    def setUp(self):
        self.config_stage1 = {
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
                'key': 'groupid itemid',
            },
            'score_filter': {
                'min_score': 10,
            },
        }

    def test_stage_1(self):
        stage = Stage1(self.config_stage1)
        result = stage.execute()
