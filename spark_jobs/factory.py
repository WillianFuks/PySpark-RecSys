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
Main Class to manage Spark Jobs.
"""


class MarrecoFactory(object):
    """Factory to get appropriate algorithm strategy.
    :type algorithm: str
    :param algorithm: states which algorithm should be prepared.
    :rtype: `base.MarrecoBase`
    :returns: algorithm strategy ready to run jobs and analysis.
    """
    @classmethod
    def _factor_alg(cls, alg):
        if alg == 'top_seller':
            from top_seller import MarrecoTopSellerJob
            return MarrecoTopSellerJob
        elif alg == 'neighbor':
            from neighbor import MarrecoNeighborJob
            return MarrecoNeighborJob
        else:
            raise ValueError("Algorithm '{}' is not available. Please choose "
                             "between 'neighbor' or 'top_seller'".format(alg))
