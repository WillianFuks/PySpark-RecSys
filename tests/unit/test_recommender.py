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


class Test_PySparkRecSys(unittest.TestCase):
    @staticmethod
    def _get_target_class():
        from recommender import PySparkRecSys
        

        return PySparkRecSys


    def test_ctor(self):
        klass = self._get_target_class()('.')
        self.assertEqual(klass.env.loader.searchpath, ['.'])


    def test_get_jinja_env(self):
        klass = self._get_target_class()('.')
        env = klass.get_jinja_env('tests/data/')
        self.assertEqual(env.loader.searchpath, ['tests/data/'])


    def test_get_rendered_template(self):
        klass = self._get_target_class()('tests/data/')
        text = klass.get_rendered_template('test_template.html', name='test')
        self.assertEqual(text, """<h1> test </h1>""")

        #test with macros
        expected_text = """\n\n<p> value of v1: a\n<p> value of v2: b\n"""
        self.assertEqual(expected_text, klass.get_rendered_template('test_macro_template.html', v1='a', v2='b'))
    
    @mock.patch('recommender.download_gcs_data')
    @mock.patch('recommender.export_tables_to_gcs')
    @mock.patch('recommender.run_queries')
    @mock.patch('recommender.build_queries')
    @mock.patch('recommender.load_default_neighbor_query_input')
    def test_build_datasets_from_BQ(self,
                                    load_mock,
                                    build_mock,
                                    run_mock,
                                    export_mock,
                                    download_mock):
        from recommender import PySparkRecSys


        ps = PySparkRecSys('.')
        ps.get_template = lambda x: x

        load_mock.return_value = 'query_args'
        queries = {'queries': 'queries'}
        build_mock.return_value = queries
        ps.build_datasets_from_BQ('t_path',
                               'local',
                               4,
                               3,
                               2,
                               'dataset_name',
                               'bucket_name')
        build_mock.assert_called_once_with(*['t_path',
                                               'query_args',
                                               4,
                                               3,
                                               2])
        
        run_mock.assert_called_once_with(*[queries, 'dataset_name'])
        export_mock.assert_called_once_with(*['dataset_name',
                                              queries.keys(),
                                              'gs://bucket_name/pyspark/%s',
                                              {'compress': True}])
        download_mock.assert_called_once_with(*['bucket_name', '/home/jovyan/'])
 
        download_mock = mock.Mock() # prepares for testing GCP location       
        ps.build_datasets_from_BQ('t_path',
                                  'GCP',
                                  4,
                                  3,
                                  2,
                                  'dataset_name',
                                  'bucket_name')
        download_mock.assert_not_called()

