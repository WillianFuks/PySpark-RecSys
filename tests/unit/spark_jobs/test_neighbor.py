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
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
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
py_files = [
 './spark_jobs/neighbor.py',
 './spark_jobs/base.py',
 './spark_jobs/factory.py']

class Test_neighbor(unittest.TestCase):
    _sc = pyspark.SparkContext(pyFiles=py_files,)
    _session = pyspark.sql.SparkSession(_sc)

    @staticmethod
    def _get_target_class():
        from neighbor import MarrecoNeighborJob
        return MarrecoNeighborJob

    @staticmethod
    def _delete_dirs(*args):
        for arg in args:
            if os.path.isdir(arg):
                shutil.rmtree(arg)

    def test_process_datajet_day_no_force(self):
        klass = self._get_target_class()()
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 0.1)
        inter_uri = 'tests/system/data/dj'
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))
        test_days = (datetime.datetime.now() - datetime.datetime(*[2017, 8, 13])).days
        klass._process_datajet_day(mode=self._sc,compression='tests/system/data/datajet_test.json',)
        expected = [
         str({'fullvisitor_id':'25e35a54c8cace51', 'interacted_items':[
           {'key':'MA042APM76IPJ',
            'score':float(str(args.w_browse * math.exp(-args.decay * test_days))[:9])
            
            }]
          
          }),
         str({'fullvisitor_id':'610574c802ba3b33', 'interacted_items':[
           {'key':'DA923SHF35RHK',
            'score':float(str(args.w_purchase * math.exp(-args.decay * test_days))[:9])
            
            },
           {'key':'VI618SHF69UQC',
            'score':float(str(args.w_purchase * math.exp(-args.decay * test_days))[:9])
            
            }]
          
          })]
        result = [json.loads(i) for i in ''.join([open(e).read() for e in glob.glob(inter_uri + '/*.json')]).split('\n') if i]
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
        test_days = (datetime.datetime.now() - datetime.datetime(*[2017, 8, 13])).days
        klass._process_datajet_day(mode=self._sc,compression='tests/system/data/datajet_test.json',)
        self.assertTrue(os.path.isdir(inter_uri))
        klass._process_datajet_day(mode=self._sc,compression='tests/system/data/datajet_test.json',)
        expected = [
         str({'fullvisitor_id':'25e35a54c8cace51', 'interacted_items':[
           {'key':'MA042APM76IPJ',
            'score':float(str(args.w_browse * math.exp(-args.decay * test_days))[:9])
            
            }]
          
          }),
         str({'fullvisitor_id':'610574c802ba3b33', 'interacted_items':[
           {'key':'DA923SHF35RHK',
            'score':float(str(args.w_purchase * math.exp(-args.decay * test_days))[:9])
            
            },
           {'key':'VI618SHF69UQC',
            'score':float(str(args.w_purchase * math.exp(-args.decay * test_days))[:9])
            
            }]
          
          })]
        result = [json.loads(i) for i in ''.join([open(e).read() for e in glob.glob(inter_uri + '/*.json')]).split('\n') if i]
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
        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
        args = Args(2, 1, 0.5, 6, 'no', 'tests/system/data/neighbor/train/{}/train.json', inter_uri, 'tests/sytem/data/neighbor/result', 0.0, 0.0)
        klass.transform_data(self._sc, args)
        data1_uri = 'tests/system/data/neighbor/transformed_1.json'
        data2_uri = 'tests/system/data/neighbor/transformed_2.json'
        expected = {2:open(data2_uri).read().strip(), 1:open(data1_uri).read().strip()
         
         }
        for day in range(args.days_init, args.days_end - 1, -1):
            result = self._session.read.json(schema=inter_uri.format(day),).toJSON().collect()
            self.assertEqual(str(result), expected[day])

    def test_transform_data_yes_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
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
        args = Args(2, 1, 0.5, 6, 'no', 'tests/system/data/neighbor/train/{}/train.json', inter_uri, 'tests/sytem/data/neighbor/result', 0.0, 0.0)
        klass.transform_data(self._sc, args)
        args = Args(2, 1, 0.5, 6, 'yes', 'tests/system/data/neighbor/train/{}/train.json', inter_uri, 'tests/sytem/data/neighbor/result', 0.0, 0.0)
        klass.transform_data(self._sc, args)
        data1_uri = 'tests/system/data/neighbor/transformed_1.json'
        data2_uri = 'tests/system/data/neighbor/transformed_2.json'
        expected = {2:open(data2_uri).read().strip(), 1:open(data1_uri).read().strip()
         
         }
        for day in range(args.days_init, args.days_end - 1, -1):
            result = self._session.read.json(schema=inter_uri.format(day),).toJSON().collect()
            self.assertEqual(str(result), expected[day])

    def test_build_marreco(self):
        klass = self._get_target_class()()
        result_uri = 'tests/system/data/neighbor/result/similarity'
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        users_matrix_uri = 'tests/system/data/neighbor/result/users'
        self._delete_dirs(result_uri, inter_uri.format(1), inter_uri.format(2), users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
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
        args = Args(2, 1, 0.5, 6, 'no', 'tests/system/data/neighbor/train/{}/train.json', inter_uri, 'tests/sytem/data/neighbor/result', 0.0, 0.0)
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
        a = np.array([[0.5, 1.0, 0.5, 2.0],
         [
          1.0, 2.0, 1.0, 0.5],
         [
          6.0, 1.0, 0.5, 0.5],
         [
          1.0, 1.0, 6.0, 6.0]])
        n = np.linalg.norm(axis=a,).reshape(1, a.shape[1])
        expected = a.T.dot(a) / n.T.dot(n)
        for row in result:
            key1 = row.item_key
            for inner_row in row.similarity_items:
                np.testing.assert_almost_equal(decimal=expected[int(key1), int(inner_row.key)],)

        self._delete_dirs(result_uri)
        self.assertFalse(os.path.isdir(result_uri))

    def test_build_marreco_with_threshold(self):
        klass = self._get_target_class()()
        result_uri = 'tests/system/data/neighbor/result/similarity'
        inter_uri = 'tests/system/data/neighbor/inter/{}'
        users_matrix_uri = 'tests/system/data/neighbor/result/users'
        self._delete_dirs(result_uri, inter_uri.format(1), inter_uri.format(2), users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
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
        args = Args(2, 1, 0.5, 6, 'no', 'tests/system/data/neighbor/train/{}/train.json', inter_uri, 'tests/sytem/data/neighbor/result', 0.0, 0.0)
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
        a = np.array([[0.5, 1.0, 0.5, 2.0],
         [
          1.0, 2.0, 1.0, 0.5],
         [
          6.0, 1.0, 0.5, 0.5],
         [
          1.0, 1.0, 6.0, 6.0]])
        n = np.linalg.norm(axis=a,).reshape(1, a.shape[1])
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
                self.assertTrue((actual - inner_row.score) / actual < 0.2)

        self._delete_dirs(result_uri, inter_uri.format(1), inter_uri.format(2), users_matrix_uri)
        self.assertFalse(os.path.isdir(result_uri))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
        self.assertFalse(os.path.isdir(users_matrix_uri))
