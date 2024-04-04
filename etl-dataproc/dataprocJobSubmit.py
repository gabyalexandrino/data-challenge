import re, os

from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials.json'

cluster_name = 'dc-cluster'
region = 'us-central1'
project_id = 'ac-data-challenge'

def submit_job(project_id, region, cluster_name):
    # Create the job client.
    #job_client = dataproc.Client()
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    # job = {
    #     "placement": {"cluster_name": cluster_name},
    #     "spark_job": {
    #         #"main_class": "main",
    #         "jar_file_uris": ["gs://dc-solution-bucket/main.py"],
    #         "args": ['gs://dc-solution-bucket/testeup.csv', 'gs://dc-solution-bucket/testeok/', 'csv','dc_table'],
    #     },
    # }

    # Prepare your pyspark job details
    job = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': 'gs://dc-solution-bucket/main.py',
            'args': ['gs://dc-solution-bucket/testeup.csv', 'gs://dc-solution-bucket/testeok/', 'csv','dc_bq_dataset.dc_table'],
        }
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()
    print('_________________________________________________')
    print(response)
    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}")

submit_job(project_id, region, cluster_name)