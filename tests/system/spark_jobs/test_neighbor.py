#MIT License
#
#Copyright (c) 2017 Willian Fuks
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.

"""
This is system tests and should only be run if the environment has pyspark
and a spark cluster installed to receive on-demand jobs
"""

import os
import unittest
import sys
import mock
import json
import datetime
import pyspark
import math
import glob
import shutil
from collections import namedtuple
import numpy as np

from pyspark.sql import types as stypes
sys.path.append('./spark_jobs')

py_files = ['./spark_jobs/neighbor.py',
            './spark_jobs/base.py',
            './spark_jobs/factory.py']


class Test_neighbor(unittest.TestCase):

    _sc = pyspark.SparkContext(pyFiles=py_files)
    _session = pyspark.sql.SparkSession(_sc)
    _to_delete_uris = []


    @staticmethod
    def _get_target_class():
        from neighbor import MarrecoNeighborJob


        return MarrecoNeighborJob


    @staticmethod
    def _delete_dirs(*args):
        for arg in args:
            if os.path.isdir(arg):
                shutil.rmtree(arg)


    def _prepare_daily_data(self):
        for i in [1, 2]:
           uri = 'tests/system/data/neighbor/train/{}/train.json'.format(
               i)
           data = self._sc.textFile(uri)
           formatted_day = (datetime.datetime.now() -
                datetime.timedelta(days=i)).strftime('%Y-%m-%d')

           save_uri = 'tests/system/data/neighbor/train/{}/train.json'.format(
               formatted_day)
           self._delete_dirs(save_uri)
           self._to_delete_uris.append(os.path.dirname(save_uri))
           data.saveAsTextFile(save_uri)


    def _delete_uris(self):
        for uri in self._to_delete_uris:
            self._delete_dirs(uri)
        self._to_delete_uris = []


    def test_process_datajet_day_no_force(self):
        klass = self._get_target_class()()
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 0.1)
        inter_uri = 'tests/system/data/dj'
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))

        test_days = (datetime.datetime.now()
                      - datetime.datetime(*[2017, 8, 13])).days

        klass._process_datajet_day(self._sc,
            'tests/system/data/datajet_test.json',
            inter_uri,
            args,
            mode=None,
            compression=None)

        expected = [str({"user_id": "25e35a54c8cace51",
                    "interacted_items":[{"key":"MA042APM76IPJ",
                    "score": float(str(args.w_browse * math.exp(
                        -args.decay * test_days))[:9])}]}),
                    str({"user_id": "610574c802ba3b33",
                    "interacted_items":[{"key":"DA923SHF35RHK",
                    "score": float(str(args.w_purchase * math.exp(
                        -args.decay * test_days))[:9])},
                    {"key": "VI618SHF69UQC",
                    "score": float(str(args.w_purchase * math.exp(
                        -args.decay * test_days))[:9])}]})]

        result = [json.loads(i) for i in ''.join([open(e).read()
                   for e in glob.glob(inter_uri + '/*.json')]).split('\n') if i]
        for e in result:
            for item in e['interacted_items']:
                item['score'] = float(str(item['score'])[:9])

        self.assertEqual(expected, [str(e) for e in result])


    def test_process_datajet_day_yes_force(self):
        klass = self._get_target_class()()
        spark = pyspark.sql.SparkSession(self._sc)
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 0.1)
        inter_uri = '/tests/system/data/dj'
        self._delete_dirs(inter_uri)

        test_days = (datetime.datetime.now()
                      - datetime.datetime(*[2017, 8, 13])).days

        klass._process_datajet_day(self._sc,
            'tests/system/data/datajet_test.json',
            inter_uri,
            args,
            mode=None,
            compression=None)

        self.assertTrue(os.path.isdir(inter_uri))

        klass._process_datajet_day(self._sc,
            'tests/system/data/datajet_test.json',
            inter_uri,
            args,
            mode='overwrite',
            compression=None)

        expected = [str({"user_id": "25e35a54c8cace51",
                    "interacted_items":[{"key":"MA042APM76IPJ",
                    "score": float(str(args.w_browse * math.exp(
                        -args.decay * test_days))[:9])}]}),
                    str({"user_id": "610574c802ba3b33",
                    "interacted_items":[{"key":"DA923SHF35RHK",
                    "score": float(str(args.w_purchase * math.exp(
                        -args.decay * test_days))[:9])},
                    {"key": "VI618SHF69UQC",
                    "score": float(str(args.w_purchase * math.exp(
                        -args.decay * test_days))[:9])}]})]

        result = [json.loads(i) for i in ''.join([open(e).read()
                   for e in glob.glob(inter_uri + '/*.json')]).split('\n') if i]
        for e in result:
            for item in e['interacted_items']:
                item['score'] = float(str(item['score'])[:9])

        self.assertEqual(expected, [str(e) for e in result])
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))


    def test_transform_data_no_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'w_browse',
                                   'w_purchase',
                                   'force',
                                   'source_uri',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'decay'])

        self._prepare_daily_data()

        args = Args(2, 1, 0.5, 6, 'no',
            'tests/system/data/neighbor/train/{}/train.json',
            inter_uri,
            'tests/sytem/data/neighbor/result',
            0.0, 0.0)
        klass.transform_data(self._sc, args)

        data1_uri = 'tests/system/data/neighbor/transformed_1.json'
        data2_uri = 'tests/system/data/neighbor/transformed_2.json' 

        expected = {2: sorted(self._session.read.json(data2_uri,
                        schema = klass._load_users_matrix_schema()) \
                        .toJSON().collect()),
                    1: sorted(self._session.read.json(data1_uri,
                        schema = klass._load_users_matrix_schema()) \
                        .toJSON().collect())}

        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = klass.get_formatted_date(day)
            result = sorted(self._session.read.json(
                      inter_uri.format(formatted_day),
                       schema=klass._load_users_matrix_schema())\
                      .toJSON().collect())
            for i in range(len(result)):
                result_i = json.loads(result[i])
                result_i['interacted_items'] = sorted(
                    result_i['interacted_items'], key=lambda x: x['key'])
                expected_i = json.loads(expected[day][i])
                expected_i['interacted_items'] = sorted(
                    expected_i['interacted_items'], key=lambda x: x['key'])
                self.assertEqual(expected_i, result_i)

        for day in [2, 1]:
            formatted_day = klass.get_formatted_date(day)
            self._delete_dirs(inter_uri.format(formatted_day))
            self.assertFalse(os.path.isdir(inter_uri.format(formatted_day)))
        self._delete_uris()
        self.assertEqual(self._to_delete_uris, [])


    def test_transform_data_yes_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'w_browse',
                                   'w_purchase',
                                   'force',
                                   'source_uri',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'decay'])

        self._prepare_daily_data()

        args = Args(2, 1, 0.5, 6, 'no',
            'tests/system/data/neighbor/train/{}/train.json',
            inter_uri,
            'tests/sytem/data/neighbor/result',
            0.0, 0.0)
        klass.transform_data(self._sc, args)

        args = Args(2, 1, 0.5, 6, 'yes',
            'tests/system/data/neighbor/train/{}/train.json',
            inter_uri,
            'tests/sytem/data/neighbor/result',
            0.0, 0.0)
        klass.transform_data(self._sc, args)

        data1_uri = 'tests/system/data/neighbor/transformed_1.json'
        data2_uri = 'tests/system/data/neighbor/transformed_2.json'

        expected = {2: sorted(self._session.read.json(data2_uri,
                        schema = klass._load_users_matrix_schema()) \
                        .toJSON().collect()),
                    1: sorted(self._session.read.json(data1_uri,
                        schema = klass._load_users_matrix_schema()) \
                        .toJSON().collect())}

        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = klass.get_formatted_date(day)
            result = sorted(self._session.read.json(
                      inter_uri.format(formatted_day),
                       schema=klass._load_users_matrix_schema())\
                      .toJSON().collect())
            for i in range(len(result)):
                result_i = json.loads(result[i])
                result_i['interacted_items'] = sorted(
                    result_i['interacted_items'], key=lambda x: x['key'])
                expected_i = json.loads(expected[day][i])
                expected_i['interacted_items'] = sorted(
                    expected_i['interacted_items'], key=lambda x: x['key'])
                self.assertEqual(expected_i, result_i)

        for day in [2, 1]:
            formatted_day = klass.get_formatted_date(day)
            self._delete_dirs(inter_uri.format(formatted_day))
            self.assertFalse(os.path.isdir(inter_uri.format(formatted_day)))
        self._delete_uris()
        self.assertEqual(self._to_delete_uris, [])


    def test_build_marreco(self):
        klass = self._get_target_class()()
        result_uri = 'tests/system/data/neighbor/result/similarity'
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        users_matrix_uri = 'tests/system/data/neighbor/result/users'

        self._delete_dirs(result_uri, users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(users_matrix_uri))

        self._prepare_daily_data()

        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'w_browse',
                                   'w_purchase',
                                   'force',
                                   'source_uri',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'decay'])

        args = Args(2, 1, 0.5, 6, 'no',
            'tests/system/data/neighbor/train/{}/train.json',
            inter_uri,
            'tests/sytem/data/neighbor/result',
            0.0, 0.0)

        klass.transform_data(self._sc, args)

        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'users_matrix_uri'])

        args = Args(2, 1, inter_uri, result_uri, 0.0, users_matrix_uri)

        klass.build_marreco(self._sc, args)
        result = self._session.read.json(result_uri).collect()
       
        a = np.array([[0.5, 1., 0.5, 2.],
                      [1., 2., 1., 0.5],
                      [6., 1., 0.5, 0.5],
                      [1., 1., 6., 6.]])
        n = np.linalg.norm(a, axis=0).reshape(1, a.shape[1])

        expected = a.T.dot(a) / n.T.dot(n)

        for row in result:
            key1 = row.item_key
            for inner_row in row.similarity_items:
                np.testing.assert_almost_equal(
                    expected[int(key1), int(inner_row.key)],
                           inner_row.score, decimal=6)
        
        for day in [2, 1]:
            formatted_day = klass.get_formatted_date(day)
            self._delete_dirs(inter_uri.format(formatted_day))
            self.assertFalse(os.path.isdir(inter_uri.format(formatted_day)))
        self._delete_dirs(result_uri)
        self.assertFalse(os.path.isdir(result_uri))


    def test_build_marreco_with_threshold(self):
        klass = self._get_target_class()()
        result_uri = 'tests/system/data/neighbor/result/similarity'
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        users_matrix_uri = 'tests/system/data/neighbor/result/users'

        self._prepare_daily_data()

        self._delete_dirs(result_uri, users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(users_matrix_uri))

        Args = namedtuple('args', ['days_init',
                           'days_end',
                           'w_browse',
                           'w_purchase',
                           'force',
                           'source_uri',
                           'inter_uri',
                           'neighbor_uri',
                           'threshold',
                           'decay'])

        args = Args(2, 1, 0.5, 6, 'no',
            'tests/system/data/neighbor/train/{}/train.json',
            inter_uri,
            'tests/sytem/data/neighbor/result',
            0.0, 0.0)

        klass.transform_data(self._sc, args)

        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'users_matrix_uri'])

        args = Args(2, 1, inter_uri, result_uri, 0.11, users_matrix_uri)

        klass.build_marreco(self._sc, args)
        result = self._session.read.json(result_uri).collect()

        a = np.array([[0.5, 1., 0.5, 2.],
              [1., 2., 1., 0.5],
              [6., 1., 0.5, 0.5],
              [1., 1., 6., 6.]])

        n = np.linalg.norm(a, axis=0).reshape(1, a.shape[1])

        expected = a.T.dot(a) / n.T.dot(n)

        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'inter_uri',
                                   'neighbor_uri',
                                   'threshold',
                                   'users_matrix_uri'])

        args = Args(2, 1, inter_uri, result_uri, 0.11, users_matrix_uri)

        klass.build_marreco(self._sc, args)
        result = self._session.read.json(result_uri).collect()

        for row in result:
            key1 = row.item_key
            for inner_row in row.similarity_items:
                actual = expected[int(key1), int(inner_row.key)]
                print('expected: ', actual)
                print('estimate: ', inner_row.score)
                self.assertTrue((actual - 
                                  inner_row.score) / actual < 0.2)


        for day in [2, 1]:
            formatted_day = klass.get_formatted_date(day)
            self._delete_dirs(inter_uri.format(formatted_day))
            self.assertFalse(os.path.isdir(inter_uri.format(formatted_day)))


        self._delete_dirs(result_uri,  users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(users_matrix_uri))
