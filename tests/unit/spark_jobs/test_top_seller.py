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


class Test_TopSeller(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from top_seller import MarrecoTopSellerJob


        return MarrecoTopSellerJob


    def test_load_top_seller_schema(self):
        klass = self._get_target_class()()
        expected =  stypes.StructType(fields=[
                     stypes.StructField("item_key", stypes.StringType()),
                      stypes.StructField("value", stypes.IntegerType())])

        result = klass._load_top_seller_schema()

        self.assertEqual(expected, result)

 
    def test_render_inter_uri(self):
        klass = self._get_target_class()()
        expected = 'folder/part-*'
        result = klass._render_inter_uri('folder')
        self.assertEqual(expected, result)


    def test_process_json_product_view(self):
        klass = self._get_target_class()()
        data = open('tests/data/productview_mock.json').read()

        result = list(klass._process_json(data))
        self.assertEqual(result, [])


    def test_process_json_search(self):
        klass = self._get_target_class()()

        data = open('tests/data/search_mock.json').read()
        result = list(klass._process_json(data))
        self.assertEqual(result, [])


    def test_process_json_orderconfirmation(self):
        klass = self._get_target_class()()

        data = open('tests/data/orderconfirmation_mock.json').read()
        result = list(klass._process_json(data))
        expected = [('DA923SHF35RHK', 1), ('VI618SHF69UQC', 1)]

        self.assertEqual(expected, result)

    
    def test_process_sysargs(self):
        input = ['--days_init=2',
                 '--days_end=3',
                 '--source_uri=source_uri',
                 '--inter_uri=inter_uri',
                 '--top_seller_uri=top_seller_uri',
                 '--force=no']           

        klass = self._get_target_class()()
        args = klass.process_sysargs(input)
        self.assertEqual(args.days_init, 2) 
        self.assertEqual(args.days_end, 3)
        self.assertEqual(args.source_uri, 'source_uri')
        self.assertEqual(args.inter_uri, 'inter_uri')
        self.assertEqual(args.top_seller_uri, 'top_seller_uri')
        self.assertEqual(args.force, 'no')






