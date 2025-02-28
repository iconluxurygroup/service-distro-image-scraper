import pytest
import os
import json
import pandas as pd
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from app import app, fetch_pending_images, send_email, upload_file_to_space, insert_file_db, load_payload_db, get_records_to_search, process_search_row, generate_download_file, process_image_batch
import asyncio
import tempfile
import shutil

# Setup FastAPI TestClient
client = TestClient(app)

# Mock Ray initialization and remote execution
@pytest.fixture
def mock_ray():
    with patch('ray.init') as mock_init, patch('ray.remote') as mock_remote, patch('ray.get') as mock_get:
        mock_get.return_value = [{"entry_id": 1, "status": "success"}]
        mock_remote.return_value = Mock(remote=AsyncMock(return_value=True))
        yield mock_init, mock_remote, mock_get

# Mock pyodbc database connection
@pytest.fixture
def mock_db():
    conn = Mock()
    cursor = Mock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []
    cursor.rowcount = 1
    with patch('pyodbc.connect', return_value=conn):
        yield conn, cursor

# Mock SendGrid API
@pytest.fixture
def mock_sendgrid():
    with patch('sendgrid.SendGridAPIClient') as mock_sg:
        mock_sg.return_value.send.return_value.status_code = 202
        yield mock_sg

# Mock boto3 for S3
@pytest.fixture
def mock_s3():
    with patch('boto3.client') as mock_client:
        mock_client.return_value.upload_file.return_value = None
        yield mock_client

# Temporary directory fixture
@pytest.fixture
def temp_dir():
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)

# Mock external image search (GP from icon_image_lib.google_parser)
@pytest.fixture
def mock_gp():
    with patch('app.GP') as mock_search:
        mock_search.return_value = [{"url": "http://example.com/image.jpg", "desc": "Test Image", "source": "Google", "thumbnail": "http://example.com/thumb.jpg"}]
        yield mock_search

# Test fetch_pending_images
def test_fetch_pending_images(mock_db):
    conn, cursor = mock_db
    cursor.fetchall.return_value = [(1, 1, "http://example.com/image.jpg", "BrandX", "Shoes", "Blue")]
    df = fetch_pending_images(limit=1)
    assert len(df) == 1
    assert list(df.columns) == ["ResultID", "EntryID", "ImageURL", "ProductBrand", "ProductCategory", "ProductColor"]
    assert df.iloc[0]["ImageURL"] == "http://example.com/image.jpg"

# Test send_email
def test_send_email(mock_sendgrid):
    send_email("test@example.com", "Test Subject", "http://example.com/download", "123")
    assert mock_sendgrid.called
    call_args = mock_sendgrid.return_value.send.call_args[0][0]
    assert call_args.from_email == "nik@iconluxurygroup.com"
    assert "Download File" in call_args.html_content

# Test upload_file_to_space
def test_upload_file_to_space(mock_s3, temp_dir):
    file_path = os.path.join(temp_dir, "test.txt")
    with open(file_path, "w") as f:
        f.write("test content")
    url = upload_file_to_space(file_path, "test.txt", True)
    assert mock_s3.return_value.upload_file.called
    assert url == "https://iconluxurygroup-s3.s3.us-east-2.amazonaws.com/test.txt"

# Test insert_file_db
def test_insert_file_db(mock_db):
    conn, cursor = mock_db
    cursor.fetchval.return_value = 1
    file_id = insert_file_db("test.xlsx", "http://example.com/test.xlsx", "test@example.com")
    assert file_id == 1
    assert cursor.execute.called
    assert "INSERT INTO utb_ImageScraperFiles" in cursor.execute.call_args[0][0]

# Test load_payload_db
def test_load_payload_db(mock_db):
    conn, cursor = mock_db
    rows = [{"absoluteRowIndex": 1, "searchValue": "test", "brandValue": "BrandX", "colorValue": "Red", "CategoryValue": "Shoes"}]
    df = load_payload_db(rows, 1)
    assert len(df) == 1
    assert "FileID" in df.columns
    assert df.iloc[0]["FileID"] == 1
    assert cursor.execute.called

# Test process_search_row
def test_process_search_row(mock_db, mock_gp):
    conn, cursor = mock_db
    result = process_search_row("test query", "http://example.com", 1)
    assert result is True
    assert mock_gp.called
    assert "INSERT INTO utb_ImageScraperResult" in cursor.execute.call_args[0][0]

# Test API: /process-image-batch/
@pytest.mark.asyncio
async def test_process_image_batch(mock_db, mock_sendgrid, mock_s3, mock_ray, mock_gp, temp_dir):
    payload = {
        "rowData": [{"absoluteRowIndex": 1, "searchValue": "test", "brandValue": "BrandX", "colorValue": "Red", "CategoryValue": "Shoes"}],
        "filePath": "http://example.com/test.xlsx",
        "sendToEmail": "test@example.com"
    }
    response = client.post("/process-image-batch/", json=payload)
    assert response.status_code == 200
    assert response.json() == {"message": "Processing started successfully. You will be notified upon completion."}
    
    # Simulate background task
    with patch('asyncio.run', side_effect=lambda x: x):
        await process_image_batch(payload)
    assert mock_ray[2].called  # ray.get
    assert mock_sendgrid.called
    assert mock_s3.called

# Test API: /generate-download-file/
@pytest.mark.asyncio
async def test_generate_download_file(mock_db, mock_sendgrid, mock_s3, temp_dir):
    conn, cursor = mock_db
    cursor.fetchall.side_effect = [
        [("http://example.com/test.xlsx",)],  # get_file_location
        [(1, "http://example.com/image.jpg", "http://example.com/thumb.jpg")]  # get_images_excel_db
    ]
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"test_content"
        response = client.post("/generate-download-file/", json={"file_id": 1})
        assert response.status_code == 200
        assert response.json() == {"message": "Processing started successfully. You will be notified upon completion."}
        
        # Simulate background task
        result = await generate_download_file("1")
        assert result["message"] == "Processing completed successfully."
        assert "public_url" in result
        assert mock_sendgrid.called
        assert mock_s3.called

# Ensure cleanup of asyncio loop
@pytest.fixture(autouse=True)
def cleanup_loop():
    yield
    loop = asyncio.get_event_loop()
    if not loop.is_closed():
        loop.close()

if __name__ == "__main__":
    pytest.main(["-v"])