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
Runner of PySpark Jobs
"""

import argparse
import sys

from recommender import PySparkRecSys


def get_sysargs(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--location',
                        dest='location',
                        type=str,
                        help=('Chooses where to run the pyspark job. '
                              'Can be either ``local``, i.e, when '
                              'running in local machine, or ``gcp``, '
                              'i.e, when running on google dataproc.'))

    parser.add_argument('--training_days',
                        dest='training_days',
                        type=int,
                        help=('Total amount of days that training data '
                              'should have.'))

    parser.add_argument('--validation_days',
                        dest='validation_days',
                        type=int,
                        help=('Total amount of days that validation data '
                              'should have.'))


    parser.add_argument('--testing_days',
                        dest='testing_days',
                        type=int,
                        help=('Total amount of days that testing data '
                              'should have.'))



    args = parser.parse_args(args)
    return args

def main():
    args = get_sysargs(sys.argv[1:])
    spark_rec = PySparkRecSys('.')
    queries = spark_rec.build_datasets_from_BQ('users.sql', args.location, args.training_days, args.validation_days, args.testing_days) 




if __name__ == '__main__':
    sys.exit(main())

    
