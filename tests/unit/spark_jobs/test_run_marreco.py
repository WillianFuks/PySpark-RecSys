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
from collections import namedtuple
sys.path.append('./spark_jobs')


class Test_run_marreco(unittest.TestCase):


    def test_get_alg(self):
        from run_marreco import get_alg
        expected = 'test'
        args = get_alg(['--algorithm=test'])
        self.assertEqual(expected, args.algorithm)


    @mock.patch('run_marreco.get_alg')
    @mock.patch('run_marreco.pyspark')
    @mock.patch('run_marreco.MarrecoFactory')
    def test_main_runs(self, factory_mock, spark_mock, get_alg_mock):
        from run_marreco import main


        Args = namedtuple('args', 'algorithm')
        args = Args('test')
        get_alg_mock.return_value = args
        job_mock = mock.Mock()
        factory_mock._factor_alg.return_value.return_value = job_mock
        job_mock.process_sysargs.return_value = 'test'
        context_mock = mock.Mock()
        spark_mock.SparkContext.return_value = context_mock
        context_enter_mock = mock.Mock()
        context_mock.__enter__ = context_enter_mock
        context_mock.__exit__ = mock.Mock()
        main()
        job_mock.transform_data.assert_called_once_with(context_enter_mock(), 'test')
        job_mock.build_marreco.assert_called_once_with(context_enter_mock(), 'test')


    @mock.patch('run_marreco.get_alg')
    @mock.patch('run_marreco.pyspark')
    @mock.patch('run_marreco.MarrecoFactory')
    def test_main_does_not_run(self, factory_mock, spark_mock, get_alg_mock):
        from run_marreco import main
        Args = namedtuple('args', 'algorithm')
        args = Args(None)
        get_alg_mock.return_value = args
        job_mock = mock.Mock()
        factory_mock._factor_alg.return_value.return_value = job_mock
        job_mock.process_sysargs.return_value = 'test'
        context_mock = mock.Mock()
        spark_mock.SparkContext.return_value = context_mock
        context_enter_mock = mock.Mock()
        context_mock.__enter__ = context_enter_mock
        context_mock.__exit__ = mock.Mock()
        main()
        job_mock.transform_data.assert_not_called()
        job_mock.build_marreco.assert_not_called()
