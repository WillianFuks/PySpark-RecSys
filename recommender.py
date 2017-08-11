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
Main module for preparing PySpark jobs in Google Dataproc.
"""


from jinja2 import Environment, FileSystemLoader
from google.cloud.bigquery import Client

from helper import (load_default_neighbor_query_input,
                    build_queries,
                    run_queries,
                    build_gcs_template,
                    export_tables_to_gcs,
                    download_gcs_data)


class PySparkRecSys(object):

    def __init__(self, loader_path):
        self.env = self.get_jinja_env(loader_path)

    def get_jinja_env(self, loader_path):
        return Environment(loader=FileSystemLoader(loader_path))

    def get_rendered_template(self, template_path, **kwargs):
        return self.get_template(template_path).render(**kwargs)

    def get_template(self, template_path):
        return self.env.get_template(template_path)

    def build_datasets_from_BQ(self,
                               query_template_path,
                               location,
                               training_days,
                               validation_days,
                               testing_days,
                               dataset_name,
                               gcs_bucket):
        """Runs a query against BigQuery and exports the results to GCS.

        :type query_template_path: str
        :param query_template_path: jinja template that builds the query
                                    to run against BQ, returning visitor,
                                    item and score (user, sku, score).

        :type location: str
        :param location: it's either ``local`` meaning run jobs in local spark
                         or ``gcp`` which runs in google dataproc.

        :type training_days: int
        :param training_days: total amount of days that should be present in 
                              training dataset.

        :type dataset_name: str
        :param dataset_name: name of the dataset to save the tables.

        :type gcs_bucket: str
        :param gcs_bucket: bucket in where to save the datasets extracted
                           results.
        """
        
        query_args = load_default_neighbor_query_input()
        queries = build_queries(self.get_template(query_template_path),
                                query_args,
                                training_days,
                                validation_days,
                                testing_days)
        run_queries(queries, dataset_name)
        export_tables_to_gcs(dataset_name,
                             queries.keys(),
                             build_gcs_template(**{'gcs_bucket': gcs_bucket,
                                                    'file_name': '%s'}),
                                                {'compress': True})
        if location == 'local':
            download_gcs_data(gcs_bucket, '/home/jovyan/')
