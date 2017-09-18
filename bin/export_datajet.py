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
Export data from BigQuery to GCS for PySpark Neighborhood
"""

import argparse
import sys

sys.path.append('..')

from data.exporter import Exporter
from data.help import Jinjafy
from google.cloud.bigquery import Client


def get_sysargs(args):
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

    parser.add_argument('--table',
                        dest='table',
                        type=str,
                        help=("Table name for where to save results in BQ."))

    parser.add_argument('--dataset',
                        dest='dataset',
                        type=str,
                        help=('Name of dataset to export BQ tables to.'))

    parser.add_argument('--uri',
                        dest='uri',
                        type=str,
                        help=('URI name to save the contents in GCS'))

    args = parser.parse_args(args)
    return args

def build_query(jinjafy, query, input):
    """builds neighborhood query.
    :type jinjafy: `data.help.Jinjafy`
    :param jinjafy: handler for jinja operations.
    :type input: dict
    :param input: values to be used in jinja rendering.
  
    :rtype query: str
    :param query: query after jinja rendered runs.
    """
    return jinjafy.render_template(query, **input)

def main():
    args = get_sysargs(sys.argv[1:])
    exporter = Exporter()
    jinjafy = Jinjafy('../data/queries/marreco/datajet/')

    client = Client()
    dataset = client.dataset(args.dataset)
    table = dataset.table(args.table)

    for day in range(args.days_init, args.days_end - 1, -1):
        print('processing day: ', day)
        for idx, file_ in enumerate(['productview.sql',
                                     'search.sql',
                                     'purchase.sql']):
                    
             query = build_query(jinjafy,
                                 file_,
                                 {'dataset': '40663402', 
                                 'days_interval': day,
                                 'days_interval_end': day})

             exporter.bq_to_gcs(client,
                                query,
                                {'destination': table,
                                 'maximum_bytes_billed': 1000000000000,
                                 'write_disposition': 'WRITE_TRUNCATE'},
                                {'uri': args.uri.format(day=day, idx=idx),
                                 'table': table,
                                 'compression': 'GZIP',
                                 'destination_format': 'NEWLINE_DELIMITED_JSON'})


if __name__ == '__main__':
    sys.exit(main())

