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


def load_default_neighbor_query_input():
    return {'item_navigated_score': 0.5,
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

def build_queries(template,
                  input_args,
                  training_days,
                  validation_days,
                  testing_days):

    """Build training, validation and testing queries from jinja template.

    
    :type template: `jinja2.Template`
    :param template: template that builds neighborhood query for BQ.

    :input_args: dict
    :param input_args: parameters that renders the jinja template.

    :type training_days: int
    :param training_days: total days that should be used in training range
    
    :rtype queries: dict
    :returns: dict whose keys are the name of the dataset and value is
             correspondent queries
    """ 

    queries = {}
    
    init_train_day = sum((training_days, validation_days, testing_days))
    last_train_day = sum((init_train_day, -validation_days, -testing_days, 1))

    init_validation_day = sum((validation_days, testing_days))
    last_validation_day = sum((init_validation_day, -testing_days, 1))

    init_test_day = testing_days
    last_test_day = 0

    input_args.update({'days_interval': init_train_day,
                       'days_interval_end': last_train_day})
    queries['train_query'] = template.render(**input_args)

    input_args.update({'days_interval': init_validation_day,
                       'days_interval_end': last_validation_day})
    queries['validation_query'] = template.render(**input_args)

    input_args.update({'days_interval': init_test_day,
                       'days_interval_end': last_test_day})
    queries['test_query'] = template.render(**input_args)
 
    return queries 
 

def run_bq_query(client, query, config):
    """Uses BQ Client to run a query using config for metadata.

    :type client: `google.cloud.core.client.Client`
    :param client: client for interacting with BigQuery.

    :type query: str
    :param query: query to run

    :type config: dict
    :param config: metadata to be used for query config
    """

    job = client.run_async_query(str(uuid4()), query)
    job.use_legacy_sql = False # This is mandatory

    dataset = client.dataset(config['dataset'])
    table = dataset.table(config['table'])

    job.destination = table
    job.create_disposition = ('CREATE_IF_NEEDED' if 'create_disposition' not
                               in config else config['create_disposition'])

    job.write_disposition = ('WRITE_TRUNCATE' if 'write_disposition' not in
                              config else config['write_disposition'])

    job.begin()
    job.result()

     
