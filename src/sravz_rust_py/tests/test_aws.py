import additional_imports  # pylint: disable=C0411
import json
from unittest.mock import MagicMock, patch
import unittest
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Import the AWSHelper class from your module
from aws import AWSHelper  # Replace 'your_module' with the actual module name


class TestAWSHelper(unittest.TestCase):
    def setUp(self):
        self.aws_helper = AWSHelper()
        self.sample_json_content = {"key": "value"}
        self.sample_json_str = json.dumps(self.sample_json_content)
        self.bucket_name = 'test-bucket'
        self.s3_file_key = 'test.json'

    def test_get_json_from_s3_success(self):
        with patch.object(self.aws_helper, 's3_contabo_any_bucket') as mock_s3:
            mock_response = {
                'Body': MagicMock(read=MagicMock(return_value=self.sample_json_str.encode('utf-8')))
            }
            mock_s3.get_object.return_value = mock_response

            result = self.aws_helper.get_json_from_s3(
                self.bucket_name, self.s3_file_key, gzip=False)

            self.assertEqual(result, json.loads(self.sample_json_str))
            mock_s3.get_object.assert_called_once_with(
                Bucket=self.bucket_name, Key=self.s3_file_key)

    def test_get_json_from_s3_no_credentials(self):
        with patch.object(self.aws_helper, 's3_contabo_any_bucket') as mock_s3:
            mock_s3.get_object.side_effect = NoCredentialsError

            with self.assertRaises(NoCredentialsError):
                self.aws_helper.get_json_from_s3(
                    self.bucket_name, self.s3_file_key,  gzip=False)

            mock_s3.get_object.assert_called_once_with(
                Bucket=self.bucket_name, Key=self.s3_file_key)

    # def test_get_json_from_s3_partial_credentials(self):
    #     with patch.object(self.aws_helper, 's3_contabo_any_bucket') as mock_s3:
    #         mock_s3.get_object.side_effect = PartialCredentialsError
    #
    #         with self.assertRaises(PartialCredentialsError):
    #             self.aws_helper.get_json_from_s3(self.bucket_name, self.s3_file_key)
    #
    #         mock_s3.get_object.assert_called_once_with(Bucket=self.bucket_name, Key=self.s3_file_key)

    def test_get_json_from_s3_general_exception(self):
        with patch.object(self.aws_helper, 's3_contabo_any_bucket') as mock_s3:
            mock_s3.get_object.side_effect = Exception('An error occurred')

            with self.assertRaises(Exception) as cm:
                self.aws_helper.get_json_from_s3(
                    self.bucket_name, self.s3_file_key, gzip=False)

            self.assertEqual(str(cm.exception), 'An error occurred')
            mock_s3.get_object.assert_called_once_with(
                Bucket=self.bucket_name, Key=self.s3_file_key)


if __name__ == "__main__":
    unittest.main()
