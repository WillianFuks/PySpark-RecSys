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


import unittest
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class Test_run(unittest.TestCase):

    def test_load_default_neighbor_query_input(self):
        from helper import load_default_neighbor_query_input


        default_values = load_default_neighbor_query_input()
        expected = {'item_navigated_score': 0.5,
                    'item_carted_score': 2.,
                    'item_purchased_score': 5.,
                    'item_campaign_score': 0,
                    'item_search_score': 0,
                    'item_wishlist_added_score': 0,
                    'item_reco_score': 0,
                    'decay_weight': 0,
                    'days_interval':  0,
                    'days_interval_end': 0,
                    'country': 'Brazil',
                    'domain': 'dafiti\.com\.br',
                    'campaign_custom_index': 14,
                    'customer_id_array_index': 8,
                    'ga_client_id_array_index': 11,
                    'search_url_pattern': '\?q=',
                    'location': 'gfg comercio digital ltda',
                    'wishlist_added_eventcategory': 'addCat',
                    'wishlist_added_eventaction': 'addAction',
                    'reco_eventcategory': 'recommendation',
                    'reco_eventaction': 'eventAct',
                    'only_users_items': True,
                    'data_set': 40663402,
                    'project_id': 'dafiti-analytics',
                    'from_clause': None}

        self.assertEqual(default_values, expected)


    def test_build_queries(self):
        from helper import build_queries
        from recommender import PySparkRecSys


        klass = PySparkRecSys('.')             
        env = klass.env
        input_args = {'days_interval': 0, 'days_interval_end': 0}
        queries = build_queries(env.get_template('tests/data/build_query_test'),
                                input_args,
                                10,
                                5,
                                5)

        self.assertEqual(queries['train_query'], "SELECT\n  data\nFROM table\nWHERE init_days = 20 and 11")
        self.assertEqual(queries['validation_query'], "SELECT\n  data\nFROM table\nWHERE init_days = 10 and 6") 
