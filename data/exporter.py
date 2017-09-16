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
Exports data from BigQuery to GCS for future spark jobs.
"""

import os
import uuid

from jinja2 import Environment, FileSystemLoader
from google.cloud.bigquery import Client


class Exporter(object):
    def bq_to_gcs(self,
                  client,
                  query,
                  bq_config,
                  gcs_config):
        """Runs ``query`` against BigQuery and exports the results to GCS.
        :type config: dict
        :param config: parameters to set the job constructor to run in BQ, 
                       such as destination table, dataset, expiration time.
        :type gcs_bucket: str
        :param gcs_bucket: bucket in where to save the query results.
        """
        self.run_bq_query(client, query, bq_config)
        self.export_to_gcs(client, gcs_config)


    def run_bq_query(self, client, query, config):
        """Runs ``query`` against BQ
        :type client: data.clients.bq.uClient
        :param client: bq client for job operations.
        :type config: dict
        :param config: general information for job execution.
        :raises Exception: on ``job.errors`` is not None.
        """
        job = client.run_async_query(str(uuid.uuid4()), query)
        job = self._update_job_attrs(job, config) 

        job.begin()
        job.result()
        if job.errors:
            raise Exception(str(job.errors))


    def export_to_gcs(self, client, config):
        """Runs job to export table from BigQuery to GCS.
        :type client: `google.cloud.bigquery.Client`
        :param client: bigquery client to run the job.
        :type config: dict
        :param config: key values to setup the job execution.
        :raises Exception: on ``job.errors`` is not None.
        """
        job = client.extract_table_to_storage(str(uuid.uuid4()),
                                              config['table'],
                                              config['uri'])
        job = self._update_job_attrs(job, config)
        job.begin()
        result = job.result()
        if result.errors:
            raise Exception(str(result.errors))       

 
    def _update_job_attrs(self, job, config):
        """Updates job attributes before running ``begin`` or ``run``.
        :type job: `google.cloud.bigquery.job.Job`
        :param job: job to be executed.
        :type config: dict
        :param config: values with attributes to update how ``job`` should be
                       executed.
        :rtype job: Job
        :returns: job with updated attributes.
        """
        for key, value in config.items():
            if key in set(dir(job)):
                job.__setattr__(key, value)
        return job
