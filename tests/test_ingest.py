import pytest
from unittest.mock import patch
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../jobs'))

def test_fetch_json_success():
    from raw_ingest_paris import fetch_json
    with patch('requests.get') as mock:
        mock.return_value.status_code = 200
        mock.return_value.text = '{"status":"ok"}'
        result = fetch_json("http://fake.url")
        assert result["ok"] == True
        assert result["status_code"] == 200

def test_fetch_json_http_error():
    from raw_ingest_paris import fetch_json
    with patch('requests.get') as mock:
        mock.return_value.status_code = 500
        mock.return_value.text = ''
        result = fetch_json("http://fake.url")
        assert result["ok"] == False
        assert result["error"] == "HTTP_500"

def test_fetch_json_network_error():
    from raw_ingest_paris import fetch_json
    with patch('requests.get', side_effect=Exception("timeout")):
        result = fetch_json("http://fake.url")
        assert result["ok"] == False
        assert "timeout" in result["error"]
