"""Shared pytest fixtures for tests."""
import os
import tempfile
import pytest
from unittest.mock import patch

from app import app as flask_app
from db import init_db


@pytest.fixture
def app():
    """Create a Flask app for testing."""
    flask_app.config['TESTING'] = True
    flask_app.config['WTF_CSRF_ENABLED'] = False
    return flask_app


@pytest.fixture
def client(app):
    """Create a test client for the Flask app."""
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_storage():
    """Create a mock storage instance."""
    with patch('storage.create_storage') as mock:
        storage = mock.return_value
        storage.list_files.return_value = []
        storage.dataset_exists.return_value = True
        storage.create_dataset.return_value = True
        storage.download_file.return_value = True
        yield storage


@pytest.fixture
def mock_db_session():
    """Create a mock DB session."""
    with patch('db.db.get_session') as mock:
        session = mock.return_value
        yield session


@pytest.fixture
def temp_directory():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        prev_dir = os.getcwd()
        os.chdir(tmpdir)
        yield tmpdir
        os.chdir(prev_dir) 