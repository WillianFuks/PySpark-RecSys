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


"""Helper functions with general scopes"""

import os
import uuid
from jinja2 import Environment, FileSystemLoader

from google.cloud.bigquery import Client as bq_Client
from google.cloud.storage import Client as s_Client


class Jinjafy(object):
    """Handles main operations related to Jinja such as creating
    environments, rendering templates and related operations.
    :type env: str
    :param env: folder of where to build jinja environment
    """

    def __init__(self, loader_path):
        self.env = Environment(loader=FileSystemLoader(loader_path))
    
    def render_template(self, file_path, **kwargs):
        """Gets Jinja template and return the file rendered based on kwargs input.
        :type file_path: str
        :param file_path: path to file containing jinja template
        :param kwargs: key values to render jinja template.
        """
        return self.env.get_template(file_path).render(**kwargs)


#def load_default_neighbor_query_input():
#    return {'item_navigated_score': 0.5,
#            'item_carted_score': 2.,
#            'item_purchased_score': 5.,
#            'item_campaign_score': 0,
#            'item_search_score': 0,
#            'item_wishlist_added_score': 0,
#            'item_reco_score': 0,
#            'decay_weight': 0,
#            'days_interval':  0,
#            'days_interval_end': 0,
#            'country': 'Brazil',
#            'domain': 'dafiti\.com\.br',
#            'campaign_custom_index': 14,
#            'customer_id_array_index': 8,
#            'ga_client_id_array_index': 11,
#            'search_url_pattern': '\?q=',
#            'location': 'gfg comercio digital ltda',
#            'wishlist_added_eventcategory': 'addCat',
#            'wishlist_added_eventaction': 'addAction',
#            'reco_eventcategory': 'recommendation',
#            'reco_eventaction': 'eventAct',
#            'only_users_items': True,
#            'data_set': 40663402,
#            'project_id': 'dafiti-analytics',
#            'from_clause': None}
#
#def build_queries(template,
#                  input_args,
#                  training_days,
#                  validation_days,
#                  testing_days):
#
#    """Build training, validation and testing queries from jinja template.
#
#    
#    :type template: `jinja2.Template`
#    :param template: template that builds neighborhood query for BQ.
#
#    :input_args: dict
#    :param input_args: parameters that renders the jinja template.
#
#    :type training_days: int
#    :param training_days: total days that should be used in training range
#    
#    :rtype queries: dict
#    :returns: dict whose keys are the name of the dataset and value is
#             correspondent queries
#    """ 
#
#    queries = {}
#    
#    init_train_day = sum((training_days, validation_days, testing_days))
#    last_train_day = sum((validation_days, testing_days))
#
#    init_validation_day = sum((validation_days, testing_days))
#    last_validation_day = testing_days
#
#    init_test_day = testing_days
#    last_test_day = 0
#
#    input_args.update({'days_interval': init_train_day,
#                       'days_interval_end': last_train_day})
#    queries['train_query'] = template.render(**input_args)
#
#    input_args.update({'days_interval': init_validation_day,
#                       'days_interval_end': last_validation_day})
#    queries['validation_query'] = template.render(**input_args)
#
#
#
#    input_args.update({'days_interval': init_test_day,
#                       'days_interval_end': last_test_day})
#    queries['test_query'] = template.render(**input_args)
# 
#    return queries 
# 
#
#def run_bq_query(client, query, config):
#    """Uses BQ Client to run a query using config for metadata.
#
#    :type client: `google.cloud.core.client.Client`
#    :param client: client for interacting with BigQuery.
#
#    :type query: str
#    :param query: query to run
#
#    :type config: dict
#    :param config: metadata to be used for query config. Contains values such 
#                   as ``dataset_name``, ``table_name``, ``create_disposition``, 
#                   ``write_disposition``.
#    """
#    job = client.run_async_query(str(uuid.uuid4()), query)
#    job.use_legacy_sql = False # This is always False
#
#    dataset = client.dataset(config['dataset_name'])
#    table = dataset.table(config['table_name'])
#
#    job.destination = table
#    job.create_disposition = ('CREATE_IF_NEEDED' if 'create_disposition' not
#                               in config else config['create_disposition'])
#
#    job.write_disposition = ('WRITE_TRUNCATE' if 'write_disposition' not in
#                              config else config['write_disposition'])
#
#    #job.begin()
#    job.result()
#
#def run_queries(queries, dataset_name):
#    """Runs each query inside ``queries`` against BigQuery.
#
#    :type queries: dict
#    :param queries: dict whose `key` is a str, such as 'train_query' and
#                    value is the query string itself.
#
#    :type dataset_name: str
#    :param dataset_name: dataset name of where to create the tables in BQ.
#    """
#    bq_client = bq_Client()
#
#    for key, query in queries.items():
#        print('working in key: %s' %(key))
#        run_bq_query(bq_client, query, {'table_name': key,
#                                        'dataset_name': dataset_name})
#
#def build_gcs_template(**kwargs):
#    """Builds url to be used in GCS.
#
#    :param kwargs: contains fields to be replaced in template. Example:
#                   {'gcs_bucket': 'name', 'file_name': 'the_name'} 
#
#    :rtype: str
#    :returns: formated URL to interact with GCS
#    """
#    return "gs://{gcs_bucket}/pyspark/{file_name}".format(**kwargs)
#    
#
#def export_tables_to_gcs(dataset_name, tables, gcs_uri, config):
#    """Exports tables in BQ to GCS
#
#    :type dataset_name: str
#    :param dataset_name: Name of dataset where tables are located.
#
#    :type tables: list
#    :parma tables: Name of each table to export, such as 'train_table'.
#
#    :type gcs_uri: str
#    :param gcs_uri: Template of gcs uri to export the tables. Follows a 
#                    pattern like 'gs://bucket/folder/%s*'
#
#    :type config: dict
#    :param config: Contains parameters to be used in job extraction.
#                   Examples: ``compress`` (``True`` or ``False``)
#    """
#
#    bq_client = bq_Client() # initializes client for bigquery
#    dataset = bq_client.dataset(dataset_name)
#
#    gcs_uri = gcs_uri + '.gz' if config['compress'] else gcs_uri
#
#    for table_name in tables:
#        table_name += '*'
#        print('Exporting table: %s' % table_name)
#        job = bq_client.extract_table_to_storage(str(uuid.uuid4()),
#                                                 dataset.table(table_name),
#                                                 gcs_uri % (table_name))
#        job.compression = 'GZIP' if config['compress'] else 'NONE'
#        job.begin()
#        result = job.result()
#        if result.errors:
#            raise Exception(result.errors)
#
#
#def download_gcs_data(bucket, destination):
#    """Download all files present in bucket and saves to specified
#    ``destination`` the ones with the string 'pyspark/' in the name.
#
#    :type location: str
#    :param location: if 'local', then downloads the file_names from GCS.
#
#    :type bucket: str
#    :param bucket: bucket where files are located.
#
#    :type destination: str
#    :param destination: where to save the downloaded files.
#
#    :raises: ``FileNotFoundError`` on destination does not exist.
#    """
#
#    if not os.path.isdir(destination):
#        raise FileNotFoundError()
#
#    sc = s_Client()
#    url = build_gcs_template(**{'gcs_bucket': bucket,
#                                'file_name': '%s'})
#
#    b = sc.bucket(bucket)
#    l = list(b.list_blobs())
#    blobs = [blob for blob in list(b.list_blobs()) if blob.name.find('pyspark/') >= 0]
#    for blob in blobs:
#        blob.download_to_filename(destination + blob.name)

