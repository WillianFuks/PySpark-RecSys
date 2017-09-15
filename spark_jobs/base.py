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
Base Class for Algorithms in Spark.
"""

import abc

class MarrecoBase(object):
    """Base Class to run Jobs against Spark
    
    :type tasks: list
    :param tasks: list with values [(task, {key:value}] pairs to be used later
                  when invoking command ``self.run()``
    """


    def __init__(self, tasks = None):
        self.tasks = tasks


    def run_tasks(self, sc):
        """For each task saved in ``self.task``, uses the context ``sc`` to
        execute the jobs.
        
        :type sc: `pyspark.SparkContext`
        :param sc: spark context used to run the jobs.
        """
        if not self.tasks:
            raise ValueError('``self.tasks`` list is empty. Please specify which jobs you want to run')
        for method, kwargs in self.tasks:
            method(sc, **kwargs)


    @abc.abstractmethod
    def process_sysargs(self, args):
        """Process input arguments sent in sys args. Each algorithm have its
        own implementation for making the parsing.
        
        :type args: list
        :param args: list of arguments like ['--days_init=2', '--days_end=1']
        """
        pass


    @abc.abstractmethod
    def transform_data(self, sc, args):
        """Gets data from datajet and transforms so that Marreco can read
        and use it properly. Each algorithm shall implement its own strategy
        """
        pass


    @abc.abstractmethod
    def build_marreco(self, sc, args):
        """Main method for each algorithm where results are calculated, such 
        as computing matrix similarities or top selling items.
        """
        pass
