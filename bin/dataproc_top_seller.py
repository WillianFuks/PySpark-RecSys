gcloud dataproc jobs submit pyspark --cluster=test3 --py-files=base.py,factory.py,top_seller.py --bucket=lbanor run_marreco.py -- --days_init=4 --days_end=2 --source_uri=gs://lbanor/pyspark/train_{}_*.gz --inter_uri=gs://lbanor/pyspark/marreco/top_seller/intermediate/{} --force=no --top_seller_uri=gs://lbanor/pyspark/marreco/top_seller/results --algorithm=top_seller