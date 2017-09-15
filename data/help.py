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
        self.env = Environment(loader=FileSystemLoader(loader_path),)


    def render_template(self, file_path, **kwargs):
        """Gets Jinja template and return the file rendered based on kwargs input.
        
        :type file_path: str
        :param file_path: path to file containing jinja template
        
        :param kwargs: key values to render jinja template.
        """
        return self.env.get_template(file_path).render(**kwargs)

