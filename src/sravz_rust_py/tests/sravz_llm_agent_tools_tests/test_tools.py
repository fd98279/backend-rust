import additional_imports
import unittest
import os
from sravz_rust_py.sravz_llm_agent_tools import tools


class TestTools(unittest.TestCase):
    def test_add(self):
        self.assertEqual(tools.add(2, 3), 5)
        self.assertEqual(tools.add(-1, 1), 0)

    def test_multiply(self):
        self.assertEqual(tools.multiply(2, 3), 6)
        self.assertEqual(tools.multiply(-1, 5), -5)

    def test_get_value_a_key_simple(self):
        data = {"a": 1, "b": 2}
        self.assertEqual(tools.get_value_a_key("a", data), 1)
        self.assertEqual(tools.get_value_a_key("b", data), 2)
        self.assertIsNone(tools.get_value_a_key("c", data))

    def test_get_value_a_key_nested(self):
        data = {"a": {"b": {"c": 42}}}
        self.assertEqual(tools.get_value_a_key("c", data), 42)

    def test_get_value_a_key_list(self):
        data = {"a": [{"b": 5}, {"c": 10}]}
        self.assertEqual(tools.get_value_a_key("c", data), 10)

    def test_save_to_file(self):
        file_path = tools.save_to_file("test data")
        self.assertTrue(os.path.isfile(file_path))
        with open(file_path, "r", encoding="utf-8") as f:
            self.assertEqual(f.read(), "test data")
        os.remove(file_path)

    def test_upload_file_to_contabo(self):
        class DummyAWSHelper:
            def upload_file_to_contabo(self, bucket, key, path): return None
            def get_signed_url(
                self, bucket, key): return "https://dummy-url.com"
        tools.AWSHelper = DummyAWSHelper
        test_file = "/tmp/test.txt"
        with open(test_file, "w") as f:
            f.write("dummy")
        url = tools.upload_file_to_contabo(test_file)
        self.assertEqual(url, "https://dummy-url.com")
        os.remove(test_file)

    def test_login_guest(self):
        class DummyResponse:
            text = "guest_token"
        tools.requests.get = lambda *a, **kw: DummyResponse()
        self.assertEqual(tools.get_bearer_token(), "guest_token")

    def test_web_search(self):
        self.assertEqual(tools.web_search("query"), "Hello world!!!")
