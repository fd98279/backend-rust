import additional_imports  # pylint: disable=C0411
import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Import the AWSHelper class from your module
from aws import AWSHelper  # Replace 'your_module' with the actual module name


@pytest.fixture
def aws_helper():
    return AWSHelper()


@pytest.fixture
def sample_json_content():
    return {"key": "value"}


@pytest.fixture
def sample_json_str(sample_json_content):
    return json.dumps(sample_json_content)


@pytest.fixture
def bucket_name():
    return 'test-bucket'


@pytest.fixture
def s3_file_key():
    return 'test.json'


def test_get_json_from_s3_success(aws_helper, sample_json_str, bucket_name, s3_file_key):
    with patch.object(aws_helper, 's3_contabo_any_bucket') as mock_s3:
        mock_response = {
            'Body': MagicMock(read=MagicMock(return_value=sample_json_str.encode('utf-8')))
        }
        mock_s3.get_object.return_value = mock_response

        result = aws_helper.get_json_from_s3(
            bucket_name, s3_file_key, gzip=False)

        assert result == json.loads(sample_json_str)
        mock_s3.get_object.assert_called_once_with(
            Bucket=bucket_name, Key=s3_file_key)


def test_get_json_from_s3_no_credentials(aws_helper, bucket_name, s3_file_key):
    with patch.object(aws_helper, 's3_contabo_any_bucket') as mock_s3:
        mock_s3.get_object.side_effect = NoCredentialsError

        with pytest.raises(NoCredentialsError):
            aws_helper.get_json_from_s3(bucket_name, s3_file_key,  gzip=False)

        mock_s3.get_object.assert_called_once_with(
            Bucket=bucket_name, Key=s3_file_key)

# def test_get_json_from_s3_partial_credentials(aws_helper, bucket_name, s3_file_key):
#     with patch.object(aws_helper, 's3_contabo_any_bucket') as mock_s3:
#         mock_s3.get_object.side_effect = PartialCredentialsError

#         with pytest.raises(PartialCredentialsError):
#             aws_helper.get_json_from_s3(bucket_name, s3_file_key)

#         mock_s3.get_object.assert_called_once_with(Bucket=bucket_name, Key=s3_file_key)


def test_get_json_from_s3_general_exception(aws_helper, bucket_name, s3_file_key):
    with patch.object(aws_helper, 's3_contabo_any_bucket') as mock_s3:
        mock_s3.get_object.side_effect = Exception('An error occurred')

        with pytest.raises(Exception, match='An error occurred'):
            aws_helper.get_json_from_s3(bucket_name, s3_file_key, gzip=False)

        mock_s3.get_object.assert_called_once_with(
            Bucket=bucket_name, Key=s3_file_key)
