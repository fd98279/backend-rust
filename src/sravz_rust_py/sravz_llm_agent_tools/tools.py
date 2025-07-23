
import datetime

import json
from typing import Any, Optional
from aws import AWSHelper
import os
import requests
import datetime


def get_bearer_token():
    """ Get a guest login token from the server. """
    url = "https://portfolio.sravz.com/api/auth/login"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": "https://sravz.com",
        "priority": "u=1, i",
        "referer": "https://sravz.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    }
    payload = {
        "email": "guest123@guest.com",
        "password": "password",
        "rememberMe": "true"
    }
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    return response.json().get("token").get("access_token")


def get_market_news() -> str:
    '''
        Get market news from the API.
    '''
    today = datetime.datetime.now()
    fifteen_days_ago = today - datetime.timedelta(days=15)
    start_date = fifteen_days_ago.strftime("%a %b %d %Y")
    end_date = today.strftime("%a %b %d %Y")
    url = f"https://analytics.sravz.com/api/feeds/betweendatess3url/{start_date}/{end_date}"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "authorization": f"Bearer {get_bearer_token()}",
        "origin": "https://sravz.com",
        "priority": "u=1, i",
        "referer": "https://sravz.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    }
    response = requests.get(url, headers=headers, timeout=30)
    s3_url = response.json().get("URL")
    response = requests.get(s3_url, timeout=30)
    news_summary = [x.get('summary')
                    for x in response.json() if x.get('summary')]
    unique_news_summary = list(set(news_summary))
    # 1 token ≈ 4 characters, so 15,000 tokens ≈ 60,000 characters
    max_chars = 3000 * 4
    joined = " ".join(unique_news_summary)
    return joined[:max_chars]


def add(a: float, b: float) -> float:
    """Add two numbers."""
    return a + b


def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b


def get_value_a_key(target_key: str, json_input_data_dict=None) -> Optional[Any]:
    """
    Recursively searches for the target key in a nested dictionary.

    Args:
        data (Dict[str, Any]): The dictionary to search.
        target_key (str): The key to search for.

    Returns:
        Optional[Any]: The value associated with the key if found, otherwise None.
    """
    if not json_input_data_dict:
        raise ValueError("json_input_data_dict must be provided")

    if target_key in json_input_data_dict:
        return json_input_data_dict[target_key]

    for _, value in json_input_data_dict.items():
        if isinstance(value, dict):
            result = get_value_a_key(target_key)
            if result is not None:
                return result
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    result = get_value_a_key(target_key)
                    if result is not None:
                        return result

    return None


def save_to_file(data: str) -> str:
    """
    Saves the given data to a file and returns the file name.
    """
    output_file_path = f'/tmp/sravz_llm_output_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    with open(output_file_path, "w", encoding="utf-8") as f:
        f.write(data)
    return output_file_path


def upload_file_to_contabo(local_file_path: str) -> str:
    """
    Saves the given local file to contabo and returns presigned url
    """
    aws_helper = AWSHelper()
    aws_helper.upload_file_to_contabo(
        'sravz', f'llm-output/{os.path.basename(local_file_path)}', local_file_path)
    return aws_helper.get_signed_url('sravz', f'llm-output/{os.path.basename(local_file_path)}')


def web_search(_: str) -> str:
    """Search the web for information."""
    return "Hello world!!!"
