import boto3
import os
import pyarrow as pa
from pyarrow import parquet as pq
from queue import Empty

class s3_writer(object):

    def __init__(self, q_to_write):
        '''
        Takes items from `q_to_write` from a multiprocess.JoinableQueue
        and writes them to S3.

        :param multiprocessing.JoinableQueue q_to_write: Each item from the queue
            should be a dictionary containing the following information:
            - s3_bucket: str
            - key: str
            - body: pyarrow table or string
        '''
        self.q_to_write = q_to_write
        self.s3_client = None
        self.initialize()
        # Default params for pyarrow
        self.pq_params = {
            'version': '2.0',
            'use_dictionary': True,
            'flavor': 'spark',
            'compression': 'gzip',
            'use_deprecated_int96_timestamps': True,
            'allow_truncated_timestamps': True
            }

    def is_aws(self):
        '''
        Use AWS enviornmental variables to check if running on ec2 instance.
        A bit hacky but it works
        '''
        return os.environ.get('AWS_DEFAULT_REGION') is not None

    def initialize(self):
        if self.is_aws():
            self.session = boto3.Session()
        else:
            self.session = boto3.Session(profile_name = 'aws_credentials')
        self.s3_client = self.session.client('s3')

    def listen_and_write(self, q_to_write = None):
        '''
        Fetches from a queue and writes the contents to s3.
        Items from the queue should be dictionaries containing at least:
            - s3_bucket: str
            - key: str
            - body: pyarrow table or string
        '''
        q_to_write = q_to_write or self.q_to_write

        while True:
            try:
                # Get job from queue
                job = q_to_write.get(block = False)
            except Empty:
                return
            else:
                s3_bucket = job['writer_params']['s3_bucket']
                s3_key = job['writer_params']['s3_key']
                body = job['body']
                self.write(s3_bucket, s3_key, body)
                q_to_write.task_done()
                return

    def write(self, s3_bucket, s3_key, body):
        '''
        Writes content to s3.
        '''
        if self.s3_client is None:
            self.initialize()
        
        if isinstance(body, pa.Table):
            writer = pa.BufferOutputStream()
            pq.write_table(body,
                        writer,
                        **self.pq_params
                        )
            body = bytes(writer.getvalue())
        elif isinstance(body, str):
            pass
        print(f'Writing to {s3_bucket}/{s3_key}')
        self.s3_client.put_object(Body = body, Bucket = s3_bucket, Key = s3_key)
        return   

def write_to_s3(q_to_write):
    writer = s3_writer(q_to_write)
    writer.listen_and_write()
    return