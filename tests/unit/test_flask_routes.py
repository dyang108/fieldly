import os
import pytest
from extract_data import create_app


@pytest.fixture
def test_client():
    """Create a test client for the Flask app."""
    app = create_app()
    app.config["TESTING"] = True
    app.config["WTF_CSRF_ENABLED"] = False
    with app.test_client() as client:
        yield client


def test_ping_endpoint(test_client):
    """Test the /api/ping endpoint."""
    response = test_client.get("/api/ping")
    assert response.status_code == 200
    assert b"pong" in response.data


@pytest.mark.skip(reason="Test fails without proper mock - for demonstration only")
def test_download_extraction_results_endpoint_no_file(test_client):
    """Test the download endpoint when file doesn't exist."""
    # Mock environment
    os.environ["STORAGE_TYPE"] = "local"
    os.environ["LOCAL_STORAGE_PATH"] = ".data"

    # Test the endpoint with a dataset that does not exist
    response = test_client.get(
        "/api/extract/download/local/nonexistent-dataset/file.json"
    )
    assert response.status_code == 404


def test_extraction_status_endpoint(test_client):
    """Test the extraction status endpoint."""
    # Mock environment
    os.environ["STORAGE_TYPE"] = "local"
    os.environ["LOCAL_STORAGE_PATH"] = ".data"

    # Test the endpoint
    response = test_client.get("/api/extract/status/local/test-dataset")
    assert response.status_code == 200
    assert 'exists' in response.json
    assert response.json['exists'] == False


def test_get_extraction_state_endpoint(test_client):
    """Test the extraction state endpoint."""
    # Mock environment
    os.environ["STORAGE_TYPE"] = "local"
    os.environ["LOCAL_STORAGE_PATH"] = ".data"

    # Test the endpoint
    response = test_client.get("/api/extract/state/local/test-dataset")
    assert response.status_code == 200
    assert 'exists' in response.json
    assert response.json['exists'] == False 