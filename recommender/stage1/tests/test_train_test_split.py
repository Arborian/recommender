from unittest import TestCase

import numpy as np
import pandas as pd

from recommender.stage1.steps.train_test_split import TrainTestSplit

class TestTestTrainSplit(TestCase):

    def setUp(self):
        values = np.r_[:100]
        self.df = pd.DataFrame({
            'visitorid': values,
            'anotherid': values[::-1],
        })

    def test_execute_defaults(self):
        step = TrainTestSplit()
        df_train, df_test = step.execute(self.df)
        self.assertEqual(len(df_train), 90)
        self.assertEqual(df_test.visitorid.min(), 90)

    def test_execute_50(self):
        step = TrainTestSplit(train_percentile=50)
        df_train, df_test = step.execute(self.df)
        self.assertEqual(len(df_train), 50)
        self.assertEqual(df_test.visitorid.min(), 50)

    def test_execute_othercol(self):
        step = TrainTestSplit(column_name='anotherid')
        df_train, df_test = step.execute(self.df)
        self.assertEqual(len(df_train), 90)
        self.assertEqual(df_test.visitorid.min(), 0)
        self.assertEqual(df_test.anotherid.min(), 90)
