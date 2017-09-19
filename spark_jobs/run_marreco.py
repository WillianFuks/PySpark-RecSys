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
Builds Marreco to run Jobs in Spark.
"""


import sys
import argparse

import pyspark
from factory import MarrecoFactory

def get_alg(args):
    parser = argparse.ArgumentParser()

    args = [e for e in args if 'algorithm' in e or '-h' in e]
    if len(args) == 2:
        args.remove('-h')
    parser.add_argument('--algorithm',
                        dest='algorithm',
                        type=str,
                        help=('Which algorithm to run. Currently options are '
                              '"neighbor" or "top seller"'))
    
    args = parser.parse_args(args)
    return args

def main():
    alg = get_alg(sys.argv[1:]).algorithm
    if alg:
        job = MarrecoFactory._factor_alg(alg)()
        args = job.process_sysargs(
            [e for e in sys.argv[1:] if 'algorithm' not in e])

        with pyspark.SparkContext() as sc: 
            job.transform_data(sc, args)
            job.build_marreco(sc, args)
    

if __name__ == '__main__':
    sys.exit(main())

