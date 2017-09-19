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

py_files = ['./spark_jobs/top_seller.py',
            './spark_jobs/base.py',
            './spark_jobs/factory.py']


class Test_top_seller(unittest.TestCase):

    _sc = pyspark.SparkContext(pyFiles=py_files)
    _session = pyspark.sql.SparkSession(_sc)


    @staticmethod
    def _get_target_class():
        from top_seller import MarrecoTopSellerJob


        return MarrecoTopSellerJob


    @staticmethod
    def _delete_dirs(*args):
        for arg in args:
            if os.path.isdir(arg):
                shutil.rmtree(arg)


    def test_process_datajet_day_no_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/dj'
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))

        klass._process_datajet_day(self._sc,
            'tests/system/data/top_seller/datajet_test.json',
            inter_uri,
            mode=None)

        result = self._session.read.json(inter_uri).toJSON().collect()
        expected = ['{"item_key":"DA923SHF35RHK","value":1}', 
                    '{"item_key":"VI618SHF69UQC","value":1}']

        self.assertEqual(result, expected)
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))


    def test_process_datajet_day_yes_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/dj'
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))

        klass._process_datajet_day(self._sc,
            'tests/system/data/top_seller/datajet_test.json',
            inter_uri,
            mode=None)

        klass._process_datajet_day(self._sc,
            'tests/system/data/top_seller/datajet_test.json',
            inter_uri,
            mode='overwrite')

        result = self._session.read.json(inter_uri).toJSON().collect()
        expected = ['{"item_key":"DA923SHF35RHK","value":1}',
                    '{"item_key":"VI618SHF69UQC","value":1}']

        self.assertEqual(result, expected)
        self._delete_dirs(inter_uri)
        self.assertFalse(os.path.isdir(inter_uri))
 

    def test_transform_data_no_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/inter/{}'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'force',
                                   'source_uri',
                                   'inter_uri'])
        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))

        args = Args(2, 1, 'no',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri)
        klass.transform_data(self._sc, args)

        data1_uri = ['{"item_key":"2","value":1}',
                     '{"item_key":"3","value":1}',
                     '{"item_key":"0","value":2}']

        data2_uri = ['{"item_key":"0","value":1}',
                     '{"item_key":"1","value":1}', 
                     '{"item_key":"2","value":2}']
 
        expected = {2: data2_uri,
                    1: data1_uri}

        for day in range(args.days_init, args.days_end - 1, -1):
            result = self._session.read.json(inter_uri.format(day),
                schema=klass._load_top_seller_schema()).toJSON().collect()
            self.assertEqual(result, expected[day])

        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
 

    def test_transform_data_yes_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/inter/{}'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'force',
                                   'source_uri',
                                   'inter_uri'])
        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))

        args = Args(2, 1, 'no',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri)
        klass.transform_data(self._sc, args)

        args = Args(2, 1, 'yes',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri)
        klass.transform_data(self._sc, args)

        data1_uri = ['{"item_key":"2","value":1}',
                     '{"item_key":"3","value":1}',
                     '{"item_key":"0","value":2}']

        data2_uri = ['{"item_key":"0","value":1}',
                     '{"item_key":"1","value":1}',
                     '{"item_key":"2","value":2}']

        expected = {2: data2_uri,
                    1: data1_uri}

        for day in range(args.days_init, args.days_end - 1, -1):
            result = self._session.read.json(inter_uri.format(day),
                schema=klass._load_top_seller_schema()).toJSON().collect()
            self.assertEqual(result, expected[day])

        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))


    def test_build_marreco_yes_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/inter/{}'
        result_uri = 'tests/system/data/top_seller/result'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'force',
                                   'source_uri',
                                   'inter_uri',
                                   'top_seller_uri'])
        self._delete_dirs(inter_uri.format(1),
                          inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))

        args = Args(2, 1, 'no',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri,
                    'tests/system/data/top_seller/result')
        klass.transform_data(self._sc, args)
        klass.build_marreco(self._sc, args)

        args = Args(2, 1, 'yes',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri,
                    'tests/system/data/top_seller/result')
        klass.transform_data(self._sc, args)
        klass.build_marreco(self._sc, args)

        expected = ['{"item_key":"0","value":3}',
                    '{"item_key":"1","value":1}',
                    '{"item_key":"2","value":3}',
                    '{"item_key":"3","value":1}']

        result = sorted(self._session.read.json(result_uri,
                schema=klass._load_top_seller_schema()).toJSON().collect())

        self.assertEqual(result, expected)

        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))


    def test_build_marreco_no_force(self):
        klass = self._get_target_class()()
        inter_uri = 'tests/system/data/top_seller/inter/{}'
        result_uri = 'tests/system/data/top_seller/result'
        Args = namedtuple('args', ['days_init',
                                   'days_end',
                                   'force',
                                   'source_uri',
                                   'inter_uri',
                                   'top_seller_uri'])
        self._delete_dirs(inter_uri.format(1),
                          inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))

        args = Args(2, 1, 'no',
                    'tests/system/data/top_seller/train/{}/train.json',
                    inter_uri,
                    'tests/system/data/top_seller/result')
        klass.transform_data(self._sc, args)
        klass.build_marreco(self._sc, args)

        expected = ['{"item_key":"0","value":3}',
                    '{"item_key":"1","value":1}',
                    '{"item_key":"2","value":3}',
                    '{"item_key":"3","value":1}']

        result = sorted(self._session.read.json(result_uri,
                schema=klass._load_top_seller_schema()).toJSON().collect())

        print(result)

        self.assertEqual(result, expected)

        self._delete_dirs(inter_uri.format(1), inter_uri.format(2))
        self.assertFalse(os.path.isdir(inter_uri.format(1)))
        self.assertFalse(os.path.isdir(inter_uri.format(2)))
 
