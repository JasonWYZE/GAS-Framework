# hw3_run.py

import sys
import time
import driver
import boto3
from botocore.exceptions import ClientError
import os

"""A rudimentary timer for coarse-grained profiling"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")

def upload_file_to_s3(file_path, bucket, object_name="yanze41/"):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified, file_name is used
    """

    object_name += file_path.split('/')[-1]

    
    s3_client = boto3.client('s3', region_name = 'us-east-1')
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def cleanup_local_file(file_name):
    """Delete a local file"""
    os.remove(file_name)

if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        input_file_name = sys.argv[1]
        with Timer():
            driver.run(input_file_name, 'vcf')
        
        # Define S3 bucket name for results
        bucket_name = 'mpcs-cc-gas-results'
        
        # Upload the results file and log file to S3 results bucket
        home_dir = os.path.expanduser('~/mpcs-cc')
        file_name_prefix = input_file_name.split('.')[0]
        results_file =  os.path.join(home_dir,  file_name_prefix+'.annot.vcf')
        print(results_file)
        log_file = os.path.join(home_dir, input_file_name+ '.count.log')
        
        upload_file_to_s3(results_file, bucket_name)
        upload_file_to_s3(log_file, bucket_name)
        
        # Cleanup local files
        cleanup_local_file(results_file)
        cleanup_local_file(log_file)
        cleanup_local_file(input_file_name)
        
        print(f"Results and log files for {input_file_name} have been uploaded to S3 and local copies deleted.")
    else:
        print("A valid .vcf file must be provided as input to this program.")