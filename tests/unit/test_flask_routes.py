"""Unit tests for Flask routes."""
import json
import unittest
from unittest.mock import patch, MagicMock

from flask import Flask
from pytest_flask.fixtures import client
import pytest

from app import app


@pytest.fixture
def test_client():
    """Create a test client for the Flask app."""
    app.config['TESTING'] = True
    app.config['WTF_CSRF_ENABLED'] = False
    
    with app.test_client() as client:
        yield client


def test_ping_endpoint(test_client):
    """Test the /api/ping endpoint."""
    response = test_client.get('/api/ping')
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert 'status' in data
    assert data['status'] == 'ok'
    assert 'timestamp' in data


@patch('app.extraction_progress.is_extraction_active')
def test_extraction_status_endpoint(mock_is_active, test_client):
    """Test the /api/extraction/status endpoint."""
    # Mock the response from is_extraction_active
    mock_is_active.return_value = True
    
    # Test with valid parameters
    response = test_client.get('/api/extraction/status?source=local&dataset_name=test')
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert data['source'] == 'local'
    assert data['dataset_name'] == 'test'
    assert data['is_active'] is True
    
    # Test with missing parameters
    response = test_client.get('/api/extraction/status')
    assert response.status_code == 400
    
    data = json.loads(response.data)
    assert 'error' in data


@patch('app.extraction_progress.get_extraction_state')
@patch('app.extraction_progress.is_extraction_active')
def test_get_extraction_state_endpoint(mock_is_active, mock_get_state, test_client):
    """Test the /api/extraction/state endpoint."""
    # Mock the responses
    mock_state = {
        'status': 'in_progress',
        'total_files': 5,
        'processed_files': 3,
    }
    mock_get_state.return_value = mock_state
    mock_is_active.return_value = True
    
    # Test with valid parameters
    response = test_client.get('/api/extraction/state?source=local&dataset_name=test')
    assert response.status_code == 200
    
    data = json.loads(response.data)
    assert data['source'] == 'local'
    assert data['dataset_name'] == 'test'
    assert data['state'] == mock_state
    assert data['is_active'] is True
    
    # Test with missing parameters
    response = test_client.get('/api/extraction/state')
    assert response.status_code == 400
    
    # Test with non-existent extraction
    mock_get_state.return_value = None
    response = test_client.get('/api/extraction/state?source=local&dataset_name=nonexistent')
    assert response.status_code == 404 