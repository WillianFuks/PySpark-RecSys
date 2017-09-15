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

import unittest
import sys
import mock
import json
import datetime
import math
from collections import namedtuple
from pyspark.sql import types as stypes
sys.path.append('./spark_jobs')


class Test_neighbor(unittest.TestCase):

    @staticmethod
    def _get_target_class():
        from neighbor import MarrecoNeighborJob


        return MarrecoNeighborJob


    def test_users_matrix_schema(self):
        klass = self._get_target_class()()
        expected = stypes.StructType(fields=[
         stypes.StructField('fullvisitor_id', stypes.StringType()),
         stypes.StructField('interacted_items', stypes.ArrayType(
          stypes.StructType(fields=[
           stypes.StructField('key', stypes.StringType()),
            stypes.StructField('score', stypes.FloatType())],)))],)
        self.assertEqual(expected, klass._load_users_matrix_schema())


    def test_neighbor_schema(self):
        klass = self._get_target_class()()
        expected = stypes.StructType(fields=[
                    stypes.StructField('item_key', stypes.StringType()),
                     stypes.StructField('similarity_items', stypes.ArrayType(
                      stypes.StructType(fields=[
                       stypes.StructField('key', stypes.StringType()),
                        stypes.StructField('score', stypes.FloatType())])))])
        self.assertEqual(expected, klass._load_neighbor_schema())


    @mock.patch('neighbor.random')
    def test_run_dimsum(self, random_mock):
        klass = self._get_target_class()()
        random_mock.random.return_value = 0.5

        class BroadDict(object):

            def __init__(self, dict_):
                self.value = dict_

        pq_b = BroadDict({'0':[0.6, 2.0],
                          '1':[0.6, 2.0],
                          '2':[0.3, 2.0],
                          '3':[0.6, 4.0]
         
                         })

        row = [('0', 2.0), ('1', 4.0), ('2', 6.0), ('3', 8)]
        expected = [(('0', '1'), 2), (('0', '3'), 2.0), (('1', '3'), 4.0)]
        result = list(klass._run_DIMSUM(row, pq_b))
        self.assertEqual(expected, result)


    def test_process_scores(self):
        klass = self._get_target_class()()
        row = ['0', [('0', 1.0), ('1', 2.0), ('2', 3.0)]]
        expected = [('0', 1.0), ('1', 4.0), ('2', 9)]
        result = list(klass._process_scores(row))
        self.assertEqual(expected, result)


    def test_render_inter_uri(self):
        klass = self._get_target_class()()
        expected = 'test_uri/part-*'
        result = klass._render_inter_uri('test_uri')
        self.assertEqual(expected, result)


    @mock.patch('neighbor.datetime')
    def test_process_json_product_view(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime.utcfromtimestamp(1502685428.091)
        datetime_mock.datetime.utcfromtimestamp.return_value = datetime.datetime(*[2017, 8, 13])
        data = open('tests/data/productview_mock.json').read()
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 1.5)
        klass = self._get_target_class()()
        result = list(klass._process_json(data, args))
        expected = [['25e35a54c8cace51', ('MA042APM76IPJ', math.exp(-1.5) * args.w_browse)]]
        self.assertEqual(expected, result)


    @mock.patch('neighbor.datetime')
    def test_process_json_orderconfirmation(self, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime.utcfromtimestamp(1502685428.091)
        datetime_mock.datetime.utcfromtimestamp.return_value = datetime.datetime(*[2017, 8, 13])
        data = open('tests/data/orderconfirmation_mock.json').read()
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 1.5)
        klass = self._get_target_class()()
        result = list(klass._process_json(data, args))
        expected = [
         ['610574c802ba3b33',
          (
           'DA923SHF35RHK', math.exp(-1.5) * args.w_purchase)],
         ['610574c802ba3b33',
          (
           'VI618SHF69UQC', math.exp(-1.5) * args.w_purchase)]]
        self.assertEqual(expected, result)


    def test_process_json_search(self):
        data = open('tests/data/search_mock.json').read()
        Args = namedtuple('args', ['w_browse', 'w_purchase', 'decay'])
        args = Args(0.5, 2.0, 1.5)
        klass = self._get_target_class()()
        result = list(klass._process_json(data, args))
        expected = [[]]
        self.assertEqual(expected, result)


    def test_aggregate_skus(self):
        row = ['0', [('1', 0.5), ('2', 1.0), ('1', 1.0)]]
        expected = [('0', [('1', 1.5), ('2', 1.0)])]
        klass = self._get_target_class()()
        result = list(klass._aggregate_skus(row))
        self.assertEqual(expected, result)


    def test_process_sysargs(self):
        args = ['--days_init=3',
                '--days_end=2',
                '--source_uri=source_uri',
                '--inter_uri=inter_uri',
                '--threshold=0.5',
                '--force=yes',
                '--users_matrix_uri=users_uri',
                '--neighbor_uri=neighbor_uri',
                '--w_browse=0.6',
                '--w_purchase=1.5']
        klass = self._get_target_class()()
        args = klass.process_sysargs(args)
        self.assertEqual(args.days_init, 3)
        self.assertEqual(args.days_end, 2)
        self.assertEqual(args.source_uri, 'source_uri')
        self.assertEqual(args.inter_uri, 'inter_uri')
        self.assertEqual(args.threshold, 0.5)
        self.assertEqual(args.force, 'yes')
        self.assertEqual(args.users_matrix_uri, 'users_uri')
        self.assertEqual(args.neighbor_uri, 'neighbor_uri')
        self.assertEqual(args.w_browse, 0.6)
        self.assertEqual(args.w_purchase, 1.5)

