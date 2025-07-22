'''
    AWS Functions
'''
import gzip
import json
from io import BytesIO

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from boto3.s3.transfer import S3Transfer, TransferConfig

import settings
from utils import logger_setup

logger = logger_setup.get_logger(__name__)


class AWSHelper(object):
    """description of class"""

    def __init__(self):
        self.s3 = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.logger = logger
        self.s3_contabo = boto3.client('s3',
                                       endpoint_url=settings.CONTABO_URL,
                                       aws_access_key_id=settings.CONTABO_KEY,
                                       aws_secret_access_key=settings.CONTABO_SECRET)
        self.s3_contabo_any_bucket = boto3.client('s3',
                                                  endpoint_url=settings.CONTABO_BASE_URL,
                                                  aws_access_key_id=settings.CONTABO_KEY,
                                                  aws_secret_access_key=settings.CONTABO_SECRET)
        self.s3_idrivee2 = boto3.client('s3',
                                        endpoint_url=settings.IDRIVEE2_BASE_URL,
                                        aws_access_key_id=settings.IDRIVEE2_KEY,
                                        aws_secret_access_key=settings.IDRIVEE2_SECRET)

    def get_json_from_s3(self, bucket_name, s3_file_key, data_gzipped=True) -> dict:
        '''
            Returns S3 Object as JSON data
        '''
        try:
            logger.info(
                "Getting data from s3://%s/%s", bucket_name, s3_file_key)
            # Get the JSON object from S3
            response = self.s3_contabo_any_bucket.get_object(
                Bucket=bucket_name, Key=s3_file_key)
            json_content = {}
            if data_gzipped:
                compressed_body = response['Body'].read()
                # Decompress the gzipped content
                with gzip.GzipFile(fileobj=BytesIO(compressed_body)) as gz:
                    content = gz.read().decode('utf-8')
                    json_content = json.loads(content)
            else:
                content = response['Body'].read().decode('utf-8')
                json_content = json.loads(content)
            return json_content
        except NoCredentialsError:
            logger.exception("Credentials not available")
            raise
        except PartialCredentialsError:
            logger.exception("Incomplete credentials provided")
            raise
        except Exception as e:
            logger.exception("Error occurred: %s", e)
            raise

    def upload_file_to_contabo(self, bucket_name, key_name, file_path):
        '''
            upload_if_file_not_found('sravz-historical-rolling-stats', 'gold_2017-01-17')
        '''
        transfer = None
        # Multipart for files over 10MB
        config = TransferConfig(multipart_threshold=10 * 1024 * 1024)
        transfer = S3Transfer(self.s3_contabo_any_bucket, config)
        if transfer:
            transfer.upload_file(file_path, bucket_name, key_name)

    def get_signed_url(self, bucket_name, key_name):
        '''
            from src.services import aws
            ae = aws.engine()
            ae.get_signed_url('sravz-scatter-plot-pca', 'get_scatter_plot_data_pca_by_timeframe_ca5a409ff93a9271dbfd02eaae62d7c5_10')
        '''
        # if not self.key_exists(bucket_name, key_name):
        #    raise ValueError("Bucket (%s) or Key (%s) does not exists. Cannot create signed URL"%(bucket_name, key_name))
        return self.s3_contabo_any_bucket.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket_name,
                'Key': key_name,
            },
            # Expire in 2 days
            ExpiresIn=172800
        )


if __name__ == '__main__':
    aws_helper = AWSHelper()
    print(aws_helper.get_json_from_s3(settings.CONTABO_DATA_BUCKET,
          f'{settings.MUTUAL_FUNDS_FUNDAMENTAL_DATA_PREFIX}commonstock_us_arkvx.json'))
