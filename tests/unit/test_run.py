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
import os
import mock

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class Test_run(unittest.TestCase):

    def test_run(self):
        from run import get_sysargs


        result = get_sysargs(['--location=test',
                              '--training_days=5',
                              '--validation_days=4',
                              '--testing_days=3'])
        self.assertEqual(result.location, 'test')
        self.assertEqual(result.training_days, 5)
        self.assertEqual(result.validation_days, 4)
        self.assertEqual(result.testing_days, 3)

    @mock.patch('run.sys')        
    @mock.patch('run.PySparkRecSys')
    def test_main_no_dataset_building(self, spark_mock, sys_mock):
        from run import main

        
        spark_ = mock.Mock()
        dataset_mock = mock.Mock()
        spark_mock.return_value = spark_
        spark_.build_datasets_from_BQ.return_value = dataset_mock
        sys_mock.argv = [0, '--build_datasets=no']
        
        main()
        dataset_mock.assert_not_called() 

    @mock.patch('run.sys')
    @mock.patch('run.PySparkRecSys')
    def test_main_dataset_builds(self, spark_mock, sys_mock):
        from run import main


        spark_ = mock.Mock()
        dataset_mock = mock.Mock()
        spark_mock.return_value = spark_
        spark_.build_datasets_from_BQ.return_value = mock.Mock()
        sys_mock.argv = [0, '--build_datasets=yes']

        main()
        spark_.build_datasets_from_BQ.assert_called()

