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
Set of tools to run Marreco's neighborhood algorithm in spark.
"""

import os
import sys
import json
import operator
import math
import random
import time
import argparse
import datetime
from collections import defaultdict

sys.path.append('..')

from base import MarrecoBase
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql import types as stypes
from pyspark.sql.utils import AnalysisException

class MarrecoNeighborJob(MarrecoBase):
    """This Class has all methods necessary to build Marreco Neighborhood
    against Spark.

    :type context: `pyspark.SparkContext`
    :param context: context in which Jobs are ran against.
    """
    def transform_data(self, sc, args):
        """This method gets datajet files as input and prepare them on a daily
        intermediary basis for Marreco's main algorithm DIMSUM.

        :type sc: spark context
        :param sc: spark context for running jobs.

        :param args:
          
          :type days_init: int
          :param days: how many days to scan through the files to bse used
                       in the transformation phase.

          :type days_end: int
          :param days_end: 

          :type w_browse: float
          :param w_browse: weight associated to browsing events on skus.

          :type w_purchase: float
          :param w_purchase: weight associated to purchasing events on skus.

          :type force: str
          :param force: either ``yes``, in which case forces recreation of
                        files, or ``no``, which in case if files already
                        exist then does nothing.

          :type source_uri: str
          :param source_uri: URI from where to read input data from.

          :type inter_uri: str
          :param inter_uri: URI to save intermediate results.

          :type neighbor_uri: str
          :param neighbor_uri: URI for where to save similarity matrix result.

          :type threshold: str
          :param threshold: This should be converted to float. It asserts how
                            much quality we should sacrifice in order to gain
                            performance.

          :type decay: float
          :param decay: how much less of an influence a score has given how
                       long ago it happened.
        """
        spark = SparkSession(sc)
        for day in range(args.days_init, args.days_end - 1, -1):
            print(day)
            source_uri = args.source_uri.format(day)
            inter_uri = args.inter_uri.format(day)
            try:
                inter_data = spark.read.json(inter_uri,
                    schema = self._load_users_matrix_schema()).first()

                if args.force == 'yes':
                    self._process_datajet_day(sc,
                                              source_uri,
                                              inter_uri,
                                              args,
                                              mode='overwrite')
            except (Py4JJavaError, AnalysisException):
                self._process_datajet_day(sc, source_uri, inter_uri, args)
            finally:
                print('processed data for {} day'.format(day))


    def _process_datajet_day(self,
                             sc,
                             uri,
                             inter_uri,
                             args,
                             mode=None,
                             compression='gzip'):
        """Gets datajet json like files and transforms them into data like
        [fv, user_bin, ga_client_id, customer_id, [(sku, score),...]] saving
        it in the end.
        :type sc: spark context
        :param sc: context to run spark jobs.
        
        :type uri: str
        :param uri: where the files are located.
        
        :type inter_uri: str
        :param inter_uri: where intermediate results should be saved.
        
        :type args: namedtuple
          :type args.w_browse: float
          :param args.w_browse: weight associated to users browsing history.

          :type args.w_purchase: float
          :param args.w_purchase: weight associated to purchases.

          :type args.decay: float
          :param args.decay: decay factor for account events that happened
                             long ago.
        :type mode: str
        :param mode: indicates how data should be saved. If ``None`` then
        	     throws error if file already exist. If ``overwrite`` then
        	     deletes previous file and saves new one.
        """
        sc.textFile(uri) \
          .flatMap(lambda x: self._process_json(x, args)) \
          .filter(lambda x: x) \
          .groupByKey() \
          .mapValues(list) \
          .flatMap(lambda x: self._aggregate_skus(x)) \
          .toDF(schema=self._load_users_matrix_schema()) \
          .write.json(inter_uri, compression=compression, mode=mode)


    def _load_users_matrix_schema(self):
        """Loads schema with data type [user, [(sku, score), (sku, score)]]

        :rtype: `pyspark.sql.type.StructType`
        :returns: schema speficiation for user -> (sku, score) data.
        """
        return stypes.StructType(fields=[
        	stypes.StructField("fullvisitor_id", stypes.StringType()),
        	 stypes.StructField('interacted_items', stypes.ArrayType(
        	  stypes.StructType(fields=[stypes.StructField('key', 
        	   stypes.StringType()), stypes.StructField('score', 
        	    stypes.FloatType())])))])
        

    def build_marreco(self, sc, args):
        """Main method for building Marreco's algorithms and saving results
        for later usage.
        
        :type sc: `pyspark.SparkContext`
        :param sc: spark context for running jobs.
        
        :param args:
          :type days_init: int
          :param days_init: which date time that will be used for reading data
        		    with intermediary daily results.
          
          :type days_end: int
          :param days_end: until what file to read input data.
        
          :type inter_uri: str
          :param inter_uri: URI where intermediary results should be read from
        
          :type neighbor_uri: str
          :param neighbor_uri: where to save final marreco matrix (similarity
        		      and user_sku_score matrix).
          :type inter_uri: str
          :param inter_uri: URI for where to save intermediary results.
          :type users_matrix_uri: str
          :param users_matrix_uri: URI for where to save matrix of users
                                   and their interacted skus.
          :type threshold: str
          :param threshold: this should be converted to str. Sets how much
                            we'll sacrifice in terms of quality in exchange
                            of processing time.
        """
        spark = SparkSession(sc)
        data = sc.emptyRDD()
        for day in range(args.days_init, args.days_end - 1, -1):
            print(day)
            inter_uri = self._render_inter_uri(args.inter_uri.format(day))
            data = data.union(spark.read.json(inter_uri,
                schema=self._load_users_matrix_schema()).rdd)
 
        data = data.reduceByKey(operator.add) \
        	   .flatMap(lambda x: self._aggregate_skus(x)) \
        	   .filter(lambda x: len(x[1]) > 1 and len(x[1]) <= 20)
        
        if args.users_matrix_uri:
            self._save_users_matrix(args.users_matrix_uri, data)
        
        pq_b = self._broadcast_pq(sc, data, float(args.threshold))
        data = data.flatMap(lambda x: self._run_DIMSUM(x[1], pq_b)) \
                   .reduceByKey(operator.add)
        print(data.collect())

        self._save_neighbor_matrix(args.neighbor_uri, data)


    def _save_neighbor_matrix(self, neighbor_uri, data):
        """Turns similarities into the final neighborhood matrix. The schema
        for saving the matrix is like {sku0: [(sku1, similarity1)...]}
        :type neighbor_uri: str
        :param neighbor_uri: uri for where to save the matrix.
        
        :type data: RDD
        :param data: RDD with data like [sku0, sku1, similarity]
        """
        def duplicate_keys(row):
            """this methods builds the similarities between both the diagonals
            of the similarity matrix. In the DIMSUM algorithm, we just compute
            one of the diagonals. Here we will add the transpose of the matrix
            so Marreco can see all similarities between all skus.
        
            :type row: list
            :param row: data of type [(sku0, sku1), similarity]
        
            :rtype: list:
            :returns: skus and their transposed similarities, such as
                      [sku0, [sku1, s]], [sku1, [sku0, s]]
            """
            yield (row[0][0], [(row[0][1], row[1])])
            yield (row[0][1], [(row[0][0], row[1])])
        
        data.flatMap(lambda x: duplicate_keys(x)) \
            .reduceByKey(operator.add) \
            .toDF(schema=self._load_neighbor_schema()) \
            .write.json(neighbor_uri, compression='gzip', mode='overwrite')


    def _load_neighbor_schema(self):
        """loads neighborhood schema for similarity matrix
        :rtype: `pyspark.sql.types.StructField`
        :returns: schema of type ["key", [("key", "value")]]
        """
        return stypes.StructType(fields=[
                stypes.StructField("item_key", stypes.StringType()),
                 stypes.StructField("similarity_items", stypes.ArrayType(
                  stypes.StructType(fields=[
                   stypes.StructField("key", stypes.StringType()),
                    stypes.StructField("score", stypes.FloatType())])))])


    def _save_users_matrix(self, user_matrix_uri, data):
        """Saves user -> sku matrix so Marreco can use it later for greater
        optimization. In this case, the matrix is saved as:
        [fullvisitor_id, [{"key": sku, "score": score}] interacted_items] 
        :type sc: 
        :param sc:
        :type session: `pyspark.sql.SparkSession`
        :param session: session used so to be able to save DataFrames.
        :type data: RDD
        :param data: RDD with values [user, [(sku, score), (sku, score)]]
        """
        def transform_users_data(row):
            """Transform row from [user, [(sku, score)]] to desired output.
            :type data: RDD
            :param data: observed users interaction
            """
            yield [{"fullvisitor_id": row[0],
                    "interacted_items": list(map(
                     lambda x: {"key": x[0], "score": x[1]}, row[1]))}]
        data.toDF(schema=self._load_users_matrix_schema()) \
            .write.json(user_matrix_uri, compression='gzip', mode='overwrite')


    def _run_DIMSUM(self, row, pq_b):
        """Implements DIMSUM as describe here:
        
        http://arxiv.org/abs/1304.1467

        :type row: list
        :param row: list with values (user, [(sku, score)...])

        :rtype: list
        :returns: similarities between skus in the form [(sku0, sku1, similarity)]
        """
        for i in range(len(row)):
            if random.random() < pq_b.value[row[i][0]][0]:
                for j in range(i + 1, len(row)):
                    if random.random() < pq_b.value[row[j][0]][0]:
                        value_i = row[i][1] / pq_b.value[row[i][0]][1]
                        value_j = row[j][1] / pq_b.value[row[j][0]][1]
                        key = ((row[i][0], row[j][0]) if row[i][0] < row[j][0] 
                               else (row[j][0], row[i][0]))
                        yield (key, value_i * value_j)


    def _broadcast_pq(self, sc, data, threshold):
        """Builds and broadcast probability ``p`` value and factor ``q`` for
        each sku.

        :type data: `spark.RDD`
        :param data: RDD with values (user, (sku, score)).

        :type threshold: float
        :param threshold: all similarities above this value will be guaranteed
                          to converge to real value with relative error ``e``
                          such that ``e`` < 20%.

        :rtype: broadcasted dict
        :returns: dict sku -> (p, q) where p is defined as ``gamma / ||c||``
                  and ``q = min(gamma, ||c||)``.
        """
        norms = {sku: score for sku, score in
                  data.flatMap(lambda x: self._process_scores(x)) \
                      .reduceByKey(operator.add) \
                      .map(lambda x: (x[0], math.sqrt(x[1]))) \
                      .collect()}

        gamma = (math.sqrt(10 * math.log(len(norms)) / threshold) if threshold
                  > 1e-6 else math.inf)

        pq_b = sc.broadcast({sku: (gamma / value, min(gamma, value))
                             for sku, value in norms.items()})        
        return pq_b


    def _process_scores(self, row):
        """After all user -> score aggregation is done, this method loops
        through each sku for a given user and yields its squared score so
        that we can compute the norm ``||c||`` for each sku column.

        :type row: list
        :param row: list of type [(user, (sku, score))]

        :rtype: tuple
        :returns: tuple of type (sku, (score ** 2))
        """
        for inner_row in row[1]:
            yield (inner_row[0], inner_row[1] ** 2)
 

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
    def _process_json(row, args):
        """Mapper function to extract from each line from datajet file
        and return interactions between customers and skus.

        :type row: str
        :param row: json string with datajet data.

        :type args: namedtuple
        :param args: contains values to specify how the json transformantion
                     should happen.

          :type w_browse: float
          :param w_browse: weight associated to the browsing patterns of
                           customers.

          :type w_purchase: float
          :param w_purchase: weight associated to purchasing patterns of
                             customers.

          :type decay: float
          :param decay: determines how much past interactions should be less
                        meaningful as time passes by.

        :rtype: list
        :returns: `yield` on [customerID, (sku, score)]
        """
        try:
            r = json.loads(row)
            if (r['event']['source']['tracker'] == 'fish' and
                'local_timestamp' in r['event'] and
                r['event']['identifiers']['djUCID']['value'] and
                r['event']['type'] in {"productview", "orderconfirmation"}):

                decay_factor = math.exp(-args.decay * (datetime.datetime.now() -
                                datetime.datetime.utcfromtimestamp(
                                 r['event']['local_timestamp'] / 1000)).days)

                type_ = r['event']['type']
                score = (args.w_browse if type_ == 'productview'
                          else args.w_purchase) * decay_factor

                if type_ == 'productview':
                    yield [r['event']['identifiers']['djUCID']['value'],
                           (r['event']['details']['product']['group_id'], score)]
                elif type_ == 'orderconfirmation':
                    for product in r['event']['details']['products']:
                        yield [r['event']['identifiers']['djUCID']['value'],
                               (product['group_id'], score)]
                    
        except:
            yield []


    @staticmethod
    def _aggregate_skus(row):
        """Aggregates skus from customers and their respective scores

        :type row: list
        :param row: list having values [user, (sku, score)]

        :rtype: list
        :returns: `yield` on [user, (sku, sum(score))]
        """
        d = defaultdict(float)
        for inner_row in row[1]:
            d[inner_row[0]] += inner_row[1]
        yield (row[0], list(d.items()))


    def process_sysargs(self, args):
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
    
        parser.add_argument('--threshold',
                            dest='threshold',
                            type=float,
                            help=('Threshold for acceptable similarity relative'
                                  ' error.'))
    
        parser.add_argument('--force',
                            dest='force',
                            type=str,
                            help=('If ``yes`` then replace all files with new ones. '
                                  'If ``no``, then no replacing happens.'))
    
        parser.add_argument('--users_matrix_uri',
                            dest='users_matrix_uri',
                            type=str,
                            default=None,
                            help=('where to save matrix of users. If ``None`` '
                                  'then the matrix is not built.'))
    
        parser.add_argument('--neighbor_uri',
                            dest='neighbor_uri',
                            type=str,
                            help=('where to save matrix of skus similarities'))
   
        parser.add_argument('--w_browse',
                            dest='w_browse',
                            type=float,
                            help=('weight associated to browsing action score'))

        parser.add_argument('--w_purchase',
                            dest='w_purchase',
                            type=float,
                            help=('weight associated to purchasing action score'))

        args = parser.parse_args(args)
        return args

