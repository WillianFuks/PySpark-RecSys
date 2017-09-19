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

sys.path.append('./data')

class Test_Exporter(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from exporter import Exporter
        

        return Exporter

    @mock.patch('exporter.uuid')
    def test_run_bq_query(self, uuid_mock):
        class JobSpec(object):
            def __init__(self, destination):
                self._destination = destination
                self._errors = None
                self._maximum_bytes_billed = None

            @property
            def destination(self):
                return self._desgination

            @destination.setter
            def destination(self, value):
                self._destination = value            

            def run(self):
                pass

            @property
            def errors(self):
                return self._errors

            @errors.setter
            def errors(self, value):
                self._errors = value

            @property
            def maximum_bytes_billed(self):
                return self._maximum_bytes_billed

            @maximum_bytes_billed.setter
            def maximum_bytes_billed(self, value):
                self._maximum_bytes_billed = value

            def begin(self):
                pass

            def result(self):
                pass

        uuid_mock.uuid4.return_value = 'test_id'
        klass = self._get_target_class()()
        job_mock = mock.Mock(spec=JobSpec)
        job_mock.errors = None

        client_mock = mock.Mock()
        client_mock.run_async_query.return_value = job_mock

        klass.run_bq_query(client_mock,
                            'query_test',
                            {'threshold': 2,
                             'destination': 'test',
                             'maximum_bytes_billed': 100})

        self.assertEqual(job_mock.destination, 'test')
        self.assertEqual(job_mock.maximum_bytes_billed, 100)
        client_mock.run_async_query.assert_called_once_with(*['test_id', 'query_test'])

        with self.assertRaises(Exception):
            job_mock.errors = 'error'
            klass.run_bq_query(client_mock, 'test', {})
