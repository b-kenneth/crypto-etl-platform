import pytest
import pandas as pd
from docker.dags.etl.data_quality import DataQualityChecker

class TestDataQualityChecker:
    @pytest.fixture
    def checker(self):
        return DataQualityChecker()
    
    @pytest.fixture
    def valid_data(self):
        return pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00', '2025-09-22T11:00:00'],
            'symbol': ['BTC', 'ETH'],
            'open': [30000, 2000],
            'high': [30500, 2050],
            'low': [29900, 1990],
            'close': [30400, 2040],
            'volume': [1000, 500]
        })
    
    def test_validate_file_structure_success(self, checker, valid_data):
        """Test successful file structure validation"""
        is_valid, errors = checker.validate_file_structure(valid_data)
        
        assert is_valid == True
        assert len(errors) == 0
    
    def test_validate_file_structure_missing_columns(self, checker):
        """Test file structure validation with missing columns"""
        incomplete_data = pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00'],
            'symbol': ['BTC']
            # Missing: open, high, low, close, volume
        })
        
        is_valid, errors = checker.validate_file_structure(incomplete_data)
        
        assert is_valid == False
        assert len(errors) > 0
        assert 'Missing required columns' in errors[0]
    
    def test_validate_data_quality_success(self, checker, valid_data):
        """Test successful data quality validation"""
        passed, issues, metrics = checker.validate_data_quality(valid_data)
        
        assert passed == True
        assert len(issues) == 0
        assert metrics['total_records'] == 2
        assert metrics['unique_symbols'] == 2
    
    def test_validate_data_quality_negative_prices(self, checker):
        """Test data quality validation with negative prices"""
        invalid_data = pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00'],
            'symbol': ['BTC'],
            'open': [-100],  # Invalid negative price
            'high': [30500],
            'low': [29900],
            'close': [30400],
            'volume': [1000]
        })
        
        passed, issues, metrics = checker.validate_data_quality(invalid_data)
        
        assert passed == False
        assert len(issues) > 0
        assert any('Invalid open prices' in issue for issue in issues)
    
    def test_validate_data_quality_unsupported_symbols(self, checker):
        """Test data quality validation with unsupported cryptocurrency symbols"""
        invalid_data = pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00'],
            'symbol': ['UNKNOWN_COIN'],  # Unsupported symbol
            'open': [100],
            'high': [105],
            'low': [99],
            'close': [104],
            'volume': [1000]
        })
        
        passed, issues, metrics = checker.validate_data_quality(invalid_data)
        
        # Should pass but generate warnings for unsupported symbols
        assert 'Unsupported symbols found' in str(issues)
