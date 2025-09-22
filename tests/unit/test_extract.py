import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from docker.dags.etl.extract import MinioExtractor

class TestMinioExtractor:
    @pytest.fixture
    def extractor(self):
        with patch.dict('os.environ', {
            'MINIO_ENDPOINT': 'test-endpoint:9000',
            'MINIO_ACCESS_KEY': 'test-key',
            'MINIO_SECRET_KEY': 'test-secret',
            'MINIO_BUCKET': 'test-bucket'
        }):
            return MinioExtractor()
    
    @patch('etl.extract.Minio')
    def test_list_files_success(self, mock_minio, extractor):
        # Setup mock
        mock_client = Mock()
        mock_minio.return_value = mock_client
        mock_objects = [
            Mock(object_name='raw-data/2025/09/22/crypto_data_20250922_10.csv'),
            Mock(object_name='raw-data/2025/09/22/crypto_data_20250922_11.csv')
        ]
        mock_client.list_objects.return_value = mock_objects
        
        # Execute
        files = extractor.list_files(prefix="raw-data/")
        
        # Verify
        assert len(files) == 2
        assert 'raw-data/2025/09/22/crypto_data_20250922_10.csv' in files
        mock_client.list_objects.assert_called_once()
    
    @patch('etl.extract.Minio')
    def test_read_csv_success(self, mock_minio, extractor):
        # Setup mock
        mock_client = Mock()
        mock_minio.return_value = mock_client
        
        csv_content = """timestamp,symbol,open,high,low,close,volume
2025-09-22T10:00:00,BTC,30000,30500,29900,30400,1000
2025-09-22T10:00:00,ETH,2000,2050,1990,2040,500"""
        
        mock_response = Mock()
        mock_response.read.return_value = csv_content.encode()
        mock_client.get_object.return_value = mock_response
        
        # Execute
        df = extractor.read_csv('test-file.csv')
        
        # Verify
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert 'BTC' in df['symbol'].values
        assert 'ETH' in df['symbol'].values
        mock_client.get_object.assert_called_once()
    
    @patch('etl.extract.Minio')
    def test_read_csv_file_not_found(self, mock_minio, extractor):
        # Setup mock to raise exception
        mock_client = Mock()
        mock_minio.return_value = mock_client
        mock_client.get_object.side_effect = Exception("NoSuchKey")
        
        # Execute and verify exception
        with pytest.raises(Exception):
            extractor.read_csv('nonexistent-file.csv')
