import pytest
import pandas as pd
import numpy as np
from docker.dags.etl.transform import transform_data, validate_data

class TestTransform:
    @pytest.fixture
    def sample_data(self):
        return pd.DataFrame({
            'timestamp': [
                '2025-09-22T10:00:00',
                '2025-09-22T11:00:00',
                '2025-09-22T12:00:00'
            ],
            'symbol': ['BTC', 'BTC', 'BTC'],
            'open': [30000, 30400, 30800],
            'high': [30500, 30900, 31200],
            'low': [29900, 30300, 30700],
            'close': [30400, 30800, 31000],
            'volume': [1000, 1200, 1100],
            'market_cap': [600000000, 616000000, 620000000],
            'volatility': [0.02, 0.025, 0.018]
        })
    
    def test_validate_data_success(self, sample_data):
        """Test successful data validation"""
        result = validate_data(sample_data)
        assert result == True
    
    def test_validate_data_missing_columns(self):
        """Test validation failure with missing required columns"""
        incomplete_df = pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00'],
            'symbol': ['BTC']
            # Missing required columns: open, high, low, close, volume
        })
        result = validate_data(incomplete_df)
        assert result == False
    
    def test_validate_data_empty_dataframe(self):
        """Test validation failure with empty DataFrame"""
        empty_df = pd.DataFrame()
        result = validate_data(empty_df)
        assert result == False
    
    def test_transform_data_adds_derived_metrics(self, sample_data):
        """Test that transform_data adds required derived metrics"""
        result = transform_data(sample_data)
        
        # Check that new columns are added
        assert 'price_change_pct' in result.columns
        assert 'rolling_volatility' in result.columns
        assert 'moving_avg' in result.columns
        
        # Verify calculations
        expected_pct_change_1 = (30800 - 30400) / 30400 * 100  # ~1.32%
        assert abs(result.iloc[1]['price_change_pct'] - expected_pct_change_1) < 0.01
    
    def test_transform_data_handles_single_row(self):
        """Test transform handles edge case of single row"""
        single_row = pd.DataFrame({
            'timestamp': ['2025-09-22T10:00:00'],
            'symbol': ['BTC'],
            'open': [30000], 'high': [30500], 'low': [29900], 
            'close': [30400], 'volume': [1000],
            'market_cap': [600000000], 'volatility': [0.02]
        })
        
        result = transform_data(single_row)
        
        # First row should have 0 for price_change_pct
        assert result.iloc[0]['price_change_pct'] == 0.0
        assert pd.isna(result.iloc[0]['rolling_volatility'])  # Not enough data for rolling calc
