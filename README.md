# PySpark Marreco 
Implements the algorithm [DIMSUM](http://arxiv.org/abs/1304.1467) using a PySpark implementation. 

## Getting Started
This repository is built to implement the algorithm DIMSUM on a set of data containing customers interactions on products for a given web commerce.

The folder `data` was implemented so to manipulate data that is used as input for the algorithm. It follows already a pre-defined schema that transforms data from Google BigQuery GA data to the specified schema (and saves results to a user input specified URI, as will further be discussed in soon.

The main folder of this repository is `spark_jobs` where you'll find the main algorithm implemented, specifically, the file `spark_jobs/neighbor.py`.

To run a neighbor job against spark using [Google Dataproc](https://cloud.google.com/dataproc/), you can run the following command in the folder `spark_jobs`:

```sh
gcloud dataproc jobs submit pyspark \
 --cluster=test3 \
--properties=spark.hadoop.fs.s3n.awsAccessKeyId=<key>,spark.hadoop.fs.s3n.awsSecretAccessKey=<secret> \
--py-files=base.py,factory.py,neighbor.py \
--bucket=lbanor \
run_marreco.py -- \
--days_init=7 \
--days_end=3 \
--source_uri=gs://lbanor/pyspark/datajet/dt={}/*.gz \
--inter_uri=gs://lbanor/pyspark/marreco/neighbor/intermediate/{} \
--threshold=0.1 \
--force=no \
--decay=0.03 \
--w_browse=0.5 \
--w_purchase=6.0 \
--neighbor_uri=s3n://gfg-reco/similarities_matrix/ \
--algorithm=neighbor
```

In this example, notice the `source_uri` is a template for where to get datajet data from. The `{}` is
later used for string formatting in python (where the date is set). 

Next we have `inter_uri` and this is where intermediary results are saved. By intermediary results, this means
the result of the pre-processing that each algorithm applies on datajet data to get its input schema setup for
later usage.

Finally we have the `neighbor_uri` and that's where we save the final results. The example shown above contains values
that we used in our own production environment. Please change them accordingly to your infrastructure.

For the `top_seller` algorithm, here follows an example:

```sh
gcloud dataproc jobs submit pyspark --cluster=test3 \
--properties=spark.hadoop.fs.s3n.awsAccessKeyId=<key>,spark.hadoop.fs.s3n.awsSecretAccessKey=<secret> \
--py-files=base.py,factory.py,top_seller.py \
--bucket=lbanor \
run_marreco.py -- \
--days_init=7 \
--days_end=3 \
--source_uri=gs://lbanor/pyspark/datajet/dt={}/*.gz \
--inter_uri=gs://lbanor/pyspark/marreco/top_seller/intermediate/{} \
--force=no \
--top_seller_uri=s3n://gfg-reco/top_seller_array/ \
--algorithm=top_seller
```

To get access for the *help* menu, you can run:

```sh
python run_marreco.py -h
```

And for information about each algorithm, you can run (replace "neighbor" with any other available *algorithm* you desire):

```sh
python run_marreco.py --algorithm=neighbor -h
```

Examples of running each algorithm can be found in the folder `bin` such as the file `bin/dataproc_neighbor.sh`.

### Neighbor Algorithm

For the neighborhood algorithm, you can send the parameter `threshold` which sets from which number the similarities should converge to real values with given probability. For instance, if you choose `threshold=0.1`, then everything above this value will be guaranteed to converge to real value with given probability and with a given relative error. The trade-off is that less computing resources is required to run the job.

## Pre-Requisites

Main dependecies are:
* *pyspark* with spark installed and ready to receive jobs.
* Jinja2
* Numpy (for unit test)
* *pytest*, *pytest-cov* and *mock*

## Running Unit Tests

There are two types of tests in this project, *unit* and *system*. To run the latter, it's required to have a local spark cluster running in order to receive the jobs.

To run *unit testing*, go to main folder and run:

```sh
py.test tests/unit/ --quiet --cov=.
```

For *integration testing*, it's required to run each test separately so to not have spark conflicts:

```sh
py.test tests/system/spark_jobs/test_neighbor.py --quiet --cov=. --cov-fail-under=100
```

Or for top seller:

```sh
py.test tests/system/spark_jobs/test_top_seller.py --quiet --cov=. --cov-fail-under=100
```

Notice the integration tests will take much longer as it initializes a spark context for the tests.
