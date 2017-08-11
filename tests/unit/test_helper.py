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

    def test_load_default_neighbor_query_input(self):
        from helper import load_default_neighbor_query_input


        default_values = load_default_neighbor_query_input()
        expected = {'item_navigated_score': 0.5,
                    'item_carted_score': 2.,
                    'item_purchased_score': 5.,
                    'item_campaign_score': 0,
                    'item_search_score': 0,
                    'item_wishlist_added_score': 0,
                    'item_reco_score': 0,
                    'decay_weight': 0,
                    'days_interval':  0,
                    'days_interval_end': 0,
                    'country': 'Brazil',
                    'domain': 'dafiti\.com\.br',
                    'campaign_custom_index': 14,
                    'customer_id_array_index': 8,
                    'ga_client_id_array_index': 11,
                    'search_url_pattern': '\?q=',
                    'location': 'gfg comercio digital ltda',
                    'wishlist_added_eventcategory': 'addCat',
                    'wishlist_added_eventaction': 'addAction',
                    'reco_eventcategory': 'recommendation',
                    'reco_eventaction': 'eventAct',
                    'only_users_items': True,
                    'data_set': 40663402,
                    'project_id': 'dafiti-analytics',
                    'from_clause': None}

        self.assertEqual(default_values, expected)

    def test_build_queries(self):
        from helper import build_queries
        from recommender import PySparkRecSys


        klass = PySparkRecSys('.')             
        env = klass.env
        input_args = {'days_interval': 0, 'days_interval_end': 0}
        queries = build_queries(env.get_template('tests/data/build_query_test'),
                                input_args,
                                10,
                                5,
                                5)

        self.assertEqual(queries['train_query'], "SELECT\n  data\nFROM table\nWHERE init_days = 20 and 11")
        self.assertEqual(queries['validation_query'], "SELECT\n  data\nFROM table\nWHERE init_days = 10 and 6")

    def test_build_gcs_template(self):
        from helper import build_gcs_template


        expected = 'gs://lbanor/pyspark/name'
        self.assertEqual(expected,
                         build_gcs_template(**{'gcs_bucket': 'lbanor',
                                               'file_name': 'name'}))

    @mock.patch('helper.uuid')
    def test_run_bq_query(self, uuid_mock):
        from helper import run_bq_query


        uuid_mock.uuid4.return_value = 'rand_str'

        client_mock = mock.Mock()
        job_mock = mock.Mock()
        dataset_mock = mock.Mock()

        dataset_mock.table.return_value = 'tb_test'
        dataset_mock.return_value = 'ds_test'
        client_mock.run_async_query.return_value = job_mock
        client_mock.dataset.return_value = dataset_mock
        job_mock.begin.return_value = mock.Mock()
        job_mock.result.return_value = mock.Mock() 
 
        query = "SELECT 1 FROM `table`"
        config = {'table_name': 'tb_test', 'dataset_name': 'ds_test'}

        run_bq_query(client_mock, query, config)
        client_mock.run_async_query.assert_called_once_with(*['rand_str', query])
        self.assertEqual(job_mock.use_legacy_sql, False)
        self.assertEqual(job_mock.destination, 'tb_test')
        self.assertEqual(job_mock.create_disposition, 'CREATE_IF_NEEDED')
        self.assertEqual(job_mock.write_disposition, 'WRITE_TRUNCATE')
        job_mock.begin.assert_called_once()
        job_mock.result.assert_called_once()


    @mock.patch('helper.bq_Client')
    @mock.patch('helper.run_bq_query')
    def test_run_queries(self, func_mock, client_mock):
        from helper import run_queries
        

        client_mock.return_value = 'rand_str'
        select_template = "SELECT %s"
        queries = {'test%s' %i: select_template %i for i in range(1)}
        run_queries(queries, 'ds_test')
        client_mock.assert_called()
        func_mock.assert_called_with(*['rand_str',
                                        select_template % 0,
                                        {'table_name': 'test0',
                                         'dataset_name': 'ds_test'}])

        queries = {'test%s' %i: select_template %i for i in range(3)}
        run_queries(queries, 'ds_test')
        self.assertEqual(len(func_mock.call_args_list), 4)

    @mock.patch('helper.uuid')
    @mock.patch('helper.bq_Client')
    def test_export_tables_to_gcs(self, client_mock, uuid_mock):
        from helper import export_tables_to_gcs


        client_ = mock.Mock()
        dataset_mock = mock.Mock()
        job_mock = mock.Mock()
        result_mock = mock.Mock()

        uuid_mock.uuid4.return_value = 'rand_str'
        client_mock.return_value = client_
        client_.dataset.return_value = dataset_mock
        client_.extract_table_to_storage.return_value = job_mock
        dataset_mock.table.return_value = 'table_name'
        job_mock.result.return_value = result_mock
        result_mock.errors = None

        export_tables_to_gcs('ds_name',
                             ['table1', 'table2'],
                             'gs://bucket/%s',
                             {'compress': True})
        
        client_.extract_table_to_storage.assert_called_with(*['rand_str',
            'table_name',
            'gs://bucket/table2.gz'])

        self.assertEqual(job_mock.compression, 'GZIP')
        result_mock.errors = True
        with self.assertRaises(Exception):
            export_tables_to_gcs('ds_name', 
                                 ['table1', 'table2'], 
                                 'gs://bucket/%s', 
                                 {'compress': True})

    @mock.patch('helper.os')
    @mock.patch('helper.s_Client')
    def test_download_gcs_data(self, client_mock, os_mock):
        from helper import download_gcs_data


        class Blob(mock.Mock):
            _name = None

            @property
            def name(self):
                return self._name

            @name.setter
            def name(self, value):
                self._name = value

            @classmethod
            def build_blob(cls, value):
                b = cls()
                b.name = value
                b.download_to_filename.return_value = mock.Mock()
                return b

        os_mock.path.isdir.return_value = True
        _client = mock.Mock()
        bucket_mock = mock.Mock()
        client_mock.return_value = _client
        _client.bucket.return_value = bucket_mock
        blob_list =  list(map(lambda x: Blob.build_blob(x),
                         ['pyspark/name1', 'pyspark/name2', 'name3']))
        bucket_mock.list_blobs.return_value = blob_list
        
        download_gcs_data('bucket_name', '/home/folder/')
        
        for blob in blob_list:
            if blob.name.find('pyspark/') >= 0:
                blob.download_to_filename.assert_called_once_with(*['/home/folder/' + blob.name])
            else:
                blob.download_to_filename.assert_not_called()  

        with self.assertRaises(FileNotFoundError):
            os_mock.path.isdir.return_value = False
            download_gcs_data('bucket_name', '/home/folder/') 

