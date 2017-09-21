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
Set of tools to run Marreco's Top Seller algorithm in spark.
"""

import os
import sys
import json
import operator
import math
import random
import argparse
from collections import defaultdict

sys.path.append('..')

from base import MarrecoBase
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as stypes


class MarrecoTopSellerJob(MarrecoBase):
    """This Class has all methods necessary to build Marreco Neighborhood
    against Spark.

    :type context: `pyspark.SparkContext`
    :param context: context in which Jobs are ran against.
    """
    def transform_data(self, sc, args):
        """This method gets datajet files as input and prepare them on a daily
        intermediary basis for Marreco's Top Seller algorithm.

        :type sc: spark context
        :param sc: spark context for running jobs.

        :param kwargs:
          
          :type days_init: int
          :param days: how many days to scan through the files to be used
                       in the transformation phase.

          :type days_end: int
          :param days_end: 

          :type inter_uri: str
          :param inter_uri: uri for where to save intermediate results.

          :type force: str
          :param force: either ``yes``, in which case forces recreation of
                        files, or ``no``, which in case if files already
                        exist then does nothing.

          :type source_uri: str
          :param source_uri: URI from where to read files.
        """
        spark = SparkSession(sc)
        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = self.get_formatted_date(day)

            source_uri = args.source_uri.format(formatted_day)
            inter_uri = args.inter_uri.format(formatted_day)
            try:
                inter_data = spark.read.json(inter_uri,
                    schema = self._load_top_seller_schema()).first()
                
                if args.force == 'yes' or not inter_data:
                    self._process_datajet_day(sc,
                                              source_uri,
                                              inter_uri,
                                              'overwrite')
            except Exception:
                self._process_datajet_day(sc, source_uri, inter_uri)
            finally:
                print('processed data for {} day'.format(day))


    def _process_datajet_day(self, sc, uri, inter_uri, mode=None):
        """Gets datajet json like files and transforms them into data like
        [(sku, items_sold),...] saving it in the end.

        :type sc: spark context
        :param sc: context to run spark jobs.
        
        :type uri: str
        :param uri: where the files are located.
        
        :type inter_uri: str
        :param inter_uri: where intermediate results should be saved.
        
        :type mode: str
        :param mode: indicates how data should be saved. If ``None`` then
        	     throws error if file already exist. If ``overwrite`` then
        	     deletes previous file and saves new one.
        """
        sc.textFile(uri) \
          .flatMap(lambda x: self._process_json(x)) \
          .filter(lambda x: x) \
          .reduceByKey(operator.add) \
          .toDF(schema=self._load_top_seller_schema()) \
          .write.json(inter_uri, compression='gzip', mode=mode)


    def _load_top_seller_schema(self):
        """Loads schema for top seller intermediate data saved like
        [sku, items_sold]

        :rtype: `pyspark.sql.StructType`
        :returns: schema for top selling data
        """
        return stypes.StructType(fields=[
                stypes.StructField("item_key", stypes.StringType()),
                 stypes.StructField("value", stypes.IntegerType())])


    def build_marreco(self, sc, args):
        """Main method for building Marreco's algorithms and saving results
        for later usage.
        
        :type sc: `pyspark.SparkContext`
        :param sc: spark context for running jobs.
        
        :type args: Namespace
        :param args:
          :type days_init: int
          :param days_init: which date time that will be used for reading data
        		    with intermediary daily results.
          
          :type days_end: int
          :param days_end: until what file to read input data.
        
          :type inter_uri: str
          :param inter_uri: URI where intermediary results should be read from

          :type source_uri: str
          :param source_uri: source from where to read input data

          :type force: str
          :param force: either ``yes`` in which case replace intermediate files
                        or ``no`` where nothing is done if file already exists.

          :type top_seller_uri: str
          :param top_seller_uri: URI for where to save results
        """
        spark = SparkSession(sc)
        data = sc.emptyRDD()

        for day in range(args.days_init, args.days_end - 1, -1):
            formatted_day = get_formatted_date(day)
            inter_uri = self._render_inter_uri(args.inter_uri.format(
                                               formatted_day))

            data = data.union(spark.read.json(inter_uri,
        		       schema=self._load_top_seller_schema()).rdd)
        
        data = data.reduceByKey(operator.add) \
                   .sortBy(lambda x: x[1], False)
        self._save_top_seller_matrix(args.top_seller_uri, data)


    def _save_top_seller_matrix(self, top_seller_uri, data):
        """Loads top seller schema and saves final results as
        [(item_key, items_sold), (item_key, items_sold)...]}

        :type top_seller_uri: str
        :param top_seller_uri: uri for where to save the matrix.
        
        :type data: RDD
        :param data: RDD with data like [item_key, items_sold]
        """
        data.toDF(schema=self._load_top_seller_schema()) \
            .write.json(top_seller_uri, compression='gzip', mode='overwrite')


    def _render_inter_uri(self, inter_uri, name_pattern='part-*'):
        """Helper function to process inter_uri's for later usage.

        :type inter_uri: str
        :param inter_uri: URI used for saving intermediate data transformation
                          results.

        :type name_pattern: str
        :param name_pattern: pattern used by spark to save multiple files.

        :rtype: str
        :returns: URI rendered template for retrieving data back to code.
        """
        return os.path.join(inter_uri, name_pattern)


    @staticmethod
    def _process_json(row):
        """Mapper function to extract from each line from datajet file
        and return interactions between customers and sold skus.

        :type row: str
        :param row: json string with datajet data.

        :rtype: list
        :returns: `yield` on [sku, items_sold]
        """
        try:
            r = json.loads(row)
            if (r['event']['source']['tracker'] == 'fish' and
                'local_timestamp' in r['event'] and
                r['event']['identifiers']['djUCID']['value'] and
                r['event']['type'] == "orderconfirmation"):
        
                for e in list(zip([e['group_id'] for e in
                    r['event']['details']['products']],
                     ([int(e) for e in
                      r['event']['details']['quantities']]))):
                    yield e

        except Exception as err:
            yield []


    @staticmethod
    def process_sysargs(args):
        parser = argparse.ArgumentParser()
    
        parser.add_argument('--days_init',
                            dest='days_init',
                            type=int,
                            help=("Total amount of days to come back in time "
                                  "from today's date."))
    
        parser.add_argument('--days_end',
                            dest='days_end',
                            type=int,
                            help=("Total amount of days to come back in time "
                                  "from today's date."))
    
        parser.add_argument('--source_uri',
                            dest='source_uri',
                            type=str,
                            help=("URI template from where to read source "
                                  "files from."))
    
        parser.add_argument('--inter_uri',
                            dest='inter_uri',
                            type=str,
                            help=('URI for saving intermediary results.'))
    
        parser.add_argument('--top_seller_uri',
                            dest='top_seller_uri',
                            type=str,
                            help=('URI for saving top_seller results.'))
    
        parser.add_argument('--force',
                            dest='force',
                            type=str,
                            help=('If ``yes`` then replace all files with new ones. '
                                  ' If ``no``, then no replacing happens.'))
    
        args = parser.parse_args(args)
        return args
