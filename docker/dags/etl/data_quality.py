import pandas as pd
from typing import Dict, List, Tuple
from etl.logger_config import logger

class DataQualityChecker:
    
    def __init__(self):
        self.quality_rules = {
            'price_range': {'min': 0.0001, 'max': 1000000},
            'volume_range': {'min': 0, 'max': 1000000000},
            'required_columns': ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'],
            'supported_symbols': ['BTC', 'ETH', 'ADA', 'DOT', 'BNB', 'SOL']
        }
    
    def validate_file_structure(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate basic file structure"""
        errors = []
        
        # Check required columns
        missing_cols = set(self.quality_rules['required_columns']) - set(df.columns)
        if missing_cols:
            errors.append(f"Missing required columns: {missing_cols}")
        
        # Check if file is empty
        if df.empty:
            errors.append("File contains no data")
        
        return len(errors) == 0, errors
    
    def validate_data_quality(self, df: pd.DataFrame) -> Tuple[bool, List[str], Dict]:
        """Comprehensive data quality validation"""
        errors = []
        warnings = []
        metrics = {}
        
        # Price range validation
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                invalid_prices = df[(df[col] < self.quality_rules['price_range']['min']) | 
                                 (df[col] > self.quality_rules['price_range']['max'])]
                if not invalid_prices.empty:
                    errors.append(f"Invalid {col} prices found: {len(invalid_prices)} records")
        
        # Volume validation
        if 'volume' in df.columns:
            invalid_volume = df[(df['volume'] < 0) | (df['volume'].isna())]
            if not invalid_volume.empty:
                errors.append(f"Invalid volume data: {len(invalid_volume)} records")
        
        # Symbol validation
        if 'symbol' in df.columns:
            invalid_symbols = df[~df['symbol'].isin(self.quality_rules['supported_symbols'])]
            if not invalid_symbols.empty:
                warnings.append(f"Unsupported symbols found: {invalid_symbols['symbol'].unique()}")
        
        # Calculate metrics
        metrics = {
            'total_records': len(df),
            'unique_symbols': df['symbol'].nunique() if 'symbol' in df.columns else 0,
            'date_range': {
                'start': df['timestamp'].min() if 'timestamp' in df.columns else None,
                'end': df['timestamp'].max() if 'timestamp' in df.columns else None
            },
            'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        }
        
        passed = len(errors) == 0
        return passed, errors + warnings, metrics
