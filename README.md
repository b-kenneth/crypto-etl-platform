# **CRYPTO DATA PLATFORM**
***
# Table of Contents
- [Section 1: Project Overview & Architecture](#section-1-project-overview--architecture)
    - [Project Overview](#project-overview)
    - [Core Features](#core-features)
    - [Architecture Overview](#architecture-overview)
    - [Tech Stack](#tech-stack)

- [Section 2: Getting Started](#section-2-getting-started)
    - [Prerequisites](#prerequisites)
    - [Environment Setup](#environment-setup)
    - [Installation & Configuration](#installation--configuration)
    - [Quick Start Guide](#quick-start-guide)
    - [Development Setup](#development-setup)

- [Section 3: Data Pipeline & ETL](#section-3-data-pipeline--etl)
    - [Data Flow Description](#data-flow-description)
    - [Pipeline Architecture](#pipeline-architecture)
    - [Data Quality & Validation Framework](#data-quality--validation-framework)
    - [Pipeline Monitoring & Observability](#pipeline-monitoring--observability)
    - [Performance Optimization](#performance-optimization)

- [Section 4: System Operations](#section-4-system-operations)
    - [Usage Guide](#usage-guide)
    - [Dashboard & Visualization](#dashboard--visualization)
    - [Monitoring & Logging](#monitoring--logging)
    - [Troubleshooting Guide](#troubleshooting-guide)

- [Section 5: Development & Quality](#section-5-development--quality)
    - [Complete Project Structure](#complete-project-structure)
    - [Modular Codebase Design](#modular-codebase-design)
    - [Design Principles Implementation](#design-principles-implementation)
    - [Testing Strategy](#testing-strategy)
    - [Contributing Guidelines](#contributing-guidelines)
    - [Development Workflow](#development-workflow)
    - [Security & Best Practices](#security--best-practices)
****



# **Section 1: Project Overview & Architecture**

## **Project Overview**

The Crypto Data Platform is a comprehensive end-to-end data engineering solution designed to solve critical challenges in cryptocurrency market analysis. This platform addresses the need for reliable, crypto market intelligence by implementing a robust ETL pipeline that ensures data quality, consistency, and availability for business intelligence applications.

### **What This Platform Does:**
- **Automated Data Generation**: Simulates realistic cryptocurrency market data (OHLCV) for 6 major coins.
- **Intelligent Data Processing**: Extracts, transforms, and loads crypto data with comprehensive quality validation  
- **Analytics**: Provides dashboards and visualizations for market analysis
- **Enterprise Orchestration**: Uses Apache Airflow for workflow management with sophisticated error handling
- **Scalable Storage**: Implements object storage (MinIO) and analytical database (PostgreSQL) architecture

### **Problems It Solves:**
1. **Data Reliability**: Eliminates manual data collection with automated, scheduled pipelines
2. **Data Quality**: Implements comprehensive validation and error handling to ensure clean datasets
3. **Operational Visibility**: Provides monitoring, logging, and alerting for data pipeline health  
4. **Scalability**: Designed for horizontal scaling and high-volume data processing
5. **Business Intelligence**: Enables data-driven decision making through interactive dashboards

***

## **Core Features**

### **Data Pipeline Features**
- **Idempotent Processing** - Prevents duplicate data processing with state tracking
- **Comprehensive Data Quality Validation** - Multi-layer validation (structure, business rules, consistency)
- **Graceful Error Handling** - Partial batch success with detailed failure reporting
- **Automatic Retry Logic** - Exponential backoff for transient failures
- **File Processing State Management** - Tracks processed files to prevent reprocessing

### **Orchestration & Monitoring**
- **Apache Airflow Integration** - Workflow orchestration with branching logic
- **Structured Logging** - Centralized logs with configurable levels and trace IDs
- **Real-time Monitoring** - Pipeline health metrics and execution summaries
- **Configurable Scheduling** - Hourly data generation, 15-minute ETL processing
- **Resource Management** - Container-based deployment with defined resource limits

### **Storage & Analytics**
- **Object Storage (MinIO S3-compatible)** - Organized data lake with hierarchical structure
- **Analytical Database (PostgreSQL)** - Optimized for OLAP queries and aggregations
- **Interactive Dashboards (Metabase)** - Self-service analytics and visualization
- **Data Lineage Tracking** - Maintains processing history and audit trails

***

## **Architecture Overview**
![archiecture_diagram](assets/min.drawio.png)

### **Data Flow Architecture**
1. **Generation Layer**: Automated crypto data simulation with realistic OHLCV patterns
2. **Ingestion Layer**: MinIO object storage with organized folder structure (`raw-data/YYYY/MM/DD/HH/`)
3. **Processing Layer**: Airflow-orchestrated ETL with quality validation and error handling
4. **Storage Layer**: PostgreSQL with optimized schema for analytical queries
5. **Presentation Layer**: Metabase dashboards for self-service analytics

***

## **Tech Stack**

### **Backend & Data Processing**
- **Python 3.8+** - Core programming language
- **Apache Airflow 2.7+** - Workflow orchestration and scheduling
- **Pandas** - Data manipulation and analysis
- **psycopg2** - PostgreSQL adapter for Python
- **MinIO Python SDK** - Object storage client

### **Infrastructure & Storage**
- **Docker & Docker Compose** - Containerization and service orchestration
- **MinIO** - S3-compatible object storage (data lake)
- **PostgreSQL 13+** - Analytical database (OLAP)
- **Redis** - Caching and message broker for Airflow

### **Analytics & Visualization**
- **Metabase** - Self-service business intelligence platform
- **SQL** - Query language for data analysis

### **DevOps & Monitoring**
- **Python Logging** - Structured application logging
- **Environment Variables** - Configuration management
- **Health Checks** - Container and service monitoring


# **Section 2: Getting Started**

## **Prerequisites**

### **Software Dependencies**
- **Docker** (version 20.10+) and **Docker Compose** (version 2.0+)
- **Git** for version control
- **Python 3.8+** (for development and utility scripts)
- **psql** client (optional, for direct database access)


## **Environment Setup**

### **1. Repository Clone & Navigation**
```bash
# Clone the repository
git https://github.com/b-kenneth/crypto-etl-platform/tree/main
cd crypto-data-platform
```

### **2. Environment Configuration**

```bash
# Production MinIO Configuration
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}  # Set via secrets management
MINIO_SECRET_KEY=${MINIO_SECRET_KEY}  # Set via secrets management
MINIO_ENDPOINT=minio:9000
MINIO_BUCKET=crypto-data-prod

# Production PostgreSQL Configuration  
POSTGRES_USER=******
POSTGRES_PASSWORD=${DB_PASSWORD}  # Set via secrets management
POSTGRES_DB=crypto_analytics
POSTGRES_CONN=postgresql://crypto_analytics_user:${DB_PASSWORD}@postgres-analytics:5432/crypto_analytics

# Production Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_PROJ_DIR=/opt/airflow

# Production Application Settings
PYTHONPATH=/opt/airflow
LOG_LEVEL=WARNING
ENVIRONMENT=production
```

***

## **Installation & Configuration**

### **1. Docker Services Initialization**
```bash
# Build and start all services
docker-compose up -d

# Verify all services are healthy
docker-compose ps

# Check service logs for any startup issues
docker-compose logs -f
```

### **2. Database Schema Initialization**
```bash
# Wait for PostgreSQL to be fully ready
while ! docker-compose exec postgres-analytics pg_isready; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

# Initialize database schema
docker-compose exec postgres-analytics psql -U postgres -d crypto_analytics_dev -f /docker-entrypoint-initdb.d/init_schema.sql

# Create processed files tracking table
docker-compose exec postgres-analytics psql -U postgres -d crypto_analytics_dev -c "
CREATE TABLE IF NOT EXISTS processed_files (
    file_path VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_size BIGINT,
    record_count INTEGER,
    status VARCHAR(20) DEFAULT 'processing',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files(status);
CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at ON processed_files(processed_at);


CREATE TABLE IF NOT EXISTS processed_prices (
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open NUMERIC(18, 8),
    high NUMERIC(18, 8),
    low NUMERIC(18, 8),
    close NUMERIC(18, 8),
    volume NUMERIC,
    market_cap NUMERIC,
    volatility NUMERIC,
    price_change_pct NUMERIC,
    rolling_volatility NUMERIC,
    moving_avg NUMERIC,
    PRIMARY KEY (timestamp, symbol)
);

CREATE INDEX IF NOT EXISTS idx_processed_prices_symbol ON processed_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_processed_prices_timestamp ON processed_prices(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_files_status ON processed_files(status);
"
```

***

## **Quick Start Guide**

### **1. Service Health Verification**
```bash
# Check all service status
docker-compose ps

# Verify service endpoints
curl -f http://localhost:8080/health    # Airflow
curl -f http://localhost:3000/api/health # Metabase
curl -f http://localhost:9000/minio/health/live # MinIO

# Test database connectivity
docker-compose exec postgres-analytics pg_isready -U postgres
```

### **2. Access Service Interfaces**

#### **Airflow Web UI**
- **URL**: http://localhost:8080
- **Username**: ******
- **Password**: ******
- **Purpose**: Monitor and manage ETL pipelines

#### **Metabase Analytics Dashboard**
- **URL**: http://localhost:3000
- **Initial Setup**: Create admin account on first visit
- **Purpose**: Self-service analytics and visualization

#### **MinIO Object Storage Console**
- **URL**: http://localhost:9001
- **Username**: ******
- **Password**: ******
- **Purpose**: Monitor object storage and data lake


### **3. Verify End-to-End Data Flow**
```bash
# Check processed data in PostgreSQL
docker-compose exec postgres-analytics psql -U postgres -d crypto_analytics_dev -c "
SELECT 
    symbol, 
    COUNT(*) as record_count,
    MIN(timestamp) as earliest_data,
    MAX(timestamp) as latest_data
FROM processed_prices 
GROUP BY symbol 
ORDER BY symbol;
"

# Verify file processing status
docker-compose exec postgres-analytics psql -U postgres -d crypto_analytics_dev -c "
SELECT 
    status,
    COUNT(*) as file_count
FROM processed_files 
GROUP BY status;
"
```

***

## **Development Setup**

### **Local Development Environment**
```bash
# Create Python virtual environment for development
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -r requirements.txt

# Run tests to verify setup
python -m pytest tests/ -v
```


# **Section 3: Data Pipeline & ETL**

## **Data Flow Description**

The Crypto Data Platform implements a sophisticated event-driven architecture that ensures reliable, scalable, and maintainable cryptocurrency data processing. The data flows through multiple layers with comprehensive validation, error handling, and monitoring at each stage.

### **End-to-End Data Journey**
```
Data Generation → Object Storage → Quality Validation → Transformation → Analytics Storage → Visualization
      ↓               ↓                ↓                    ↓              ↓              ↓
  Hourly Crypto    MinIO S3-     Multi-layer Data      Business Logic   PostgreSQL    Metabase
  Data Simulation  Compatible    Quality Checks        Transformations   OLAP Store    Dashboards
                   Data Lake     & Validation          & Feature Eng.
```

***

## **Pipeline Architecture**

### **1. Data Generation Layer**
**Purpose**: Automated cryptocurrency market data simulation  
**Technology**: Python data generators with realistic market patterns  
**Schedule**: Hourly execution via Airflow DAG  
**Output**: OHLCV data for 6 major cryptocurrencies (BTC, ETH, ADA, DOT, BNB, SOL)


**Design Rationale**: 
- **Realistic Market Simulation**: Uses statistical models to generate data with authentic volatility patterns
- **Organized Storage Structure**: Files stored as `raw-data/YYYY/MM/DD/HH/crypto_data_YYYYMMDD_HH.csv`
- **Scalable Generation**: Can easily extend to support additional cryptocurrencies or higher frequencies

### **2. Ingestion & Storage Layer**
**Technology**: MinIO (S3-compatible object storage)  
**Pattern**: Event-driven file-based ingestion  
**Organization**: Hierarchical partitioning by date and hour  
**Scalability**: Supports petabyte-scale data with horizontal scaling

```bash
# Storage Organization
raw-data/
├── 2025/
│   ├── 09/
│   │   ├── 21/
│   │   │   ├── 10/
│   │   │   │   └── crypto_data_20250921_10.csv
│   │   │   ├── 11/
│   │   │   └── 12/
```

**Design Benefits**:
- **Time-based Partitioning**: Enables efficient data retrieval and archival
- **Cloud Compatibility**: S3-compatible API allows seamless migration to AWS
- **Cost Optimization**: Object storage provides economical long-term data retention

### **3. ETL Processing Layer**

![airflow_dag1](assets/Screenshot%202025-09-20%20220521.png)
![airflow_dag1](assets/Screenshot%202025-09-21%20160200.png)
**Orchestration**: Apache Airflow with sophisticated DAG workflows  
**Processing Model**: Batch processing with micro-batch capabilities  
**Error Handling**: Comprehensive retry logic and graceful failure recovery  
**Monitoring**: Real-time pipeline health monitoring with detailed logging

#### **Extract Phase**

**Key Features**:
- **Idempotent Processing**: Tracks processed files to prevent duplicate processing
- **Incremental Loading**: Only processes new files since last execution
- **State Recovery**: Maintains processing state across pipeline restarts

#### **Transform Phase**

**Business Logic Applied**:
- **Price Change Percentage**: Calculates hourly price movements for trend analysis
- **Rolling Volatility**: 3-period volatility calculation for risk assessment
- **Moving Averages**: Technical indicators for trend identification
- **Data Enrichment**: Additional calculated fields for analytical insights

#### **Load Phase**

**Load Strategy Benefits**:
- **Upsert Logic**: Handles duplicate data gracefully with ON CONFLICT resolution
- **Atomic Transactions**: Ensures data consistency with transaction boundaries
- **Performance Optimization**: Bulk insert operations for high throughput

---

## **Data Quality & Validation Framework**

### **Multi-Layer Validation Strategy**

#### **Layer 1: Structural Validation**
```python
def validate_file_structure(df):
    required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
    missing_cols = set(required_columns) - set(df.columns)
    
    if missing_cols:
        return False, f"Missing columns: {missing_cols}"
    if df.empty:
        return False, "Empty dataset"
    
    return True, "Valid structure"
```

#### **Layer 2: Business Rule Validation**
```python
def validate_data_quality(df):
    errors = []
    
    # Price range validation
    invalid_prices = df[(df['close'] < 0.0001) | (df['close'] > 1000000)]
    if not invalid_prices.empty:
        errors.append(f"Invalid price range: {len(invalid_prices)} records")
    
    # Volume validation
    invalid_volume = df[df['volume'] < 0]
    if not invalid_volume.empty:
        errors.append(f"Negative volumes: {len(invalid_volume)} records")
    
    # Temporal consistency
    future_dates = df[df['timestamp'] > datetime.utcnow()]
    if not future_dates.empty:
        errors.append(f"Future timestamps: {len(future_dates)} records")
    
    return len(errors) == 0, errors
```

#### **Layer 3: Cross-Reference Validation**
- **Symbol Validation**: Ensures only supported cryptocurrencies are processed
- **Market Hours**: Validates data timing against expected market operations
- **Volume-Price Correlation**: Detects anomalous trading patterns

### **Error Handling & Recovery**

#### **Graceful Failure Management**
```python
# Partial Batch Success Strategy
for file_info in processed_data:
    try:
        # Process individual file
        transformed_df = transform_data(df)
        upsert_prices(transformed_df)
        processor.mark_file_completed(file_path, len(transformed_df))
        successful_files.append(file_info)
    except Exception as e:
        # Log error but continue processing other files
        processor.mark_file_failed(file_path, str(e))
        failed_files.append(file_info)
```

#### **Retry Strategy**
- **Exponential Backoff**: Progressive delay for transient failures
- **Circuit Breaker**: Stops processing if failure rate exceeds threshold
- **Dead Letter Queue**: Quarantines persistently failing files for manual review

***

## **Pipeline Monitoring & Observability**

### **Processing Metrics Tracked**
```sql
-- File Processing Status
SELECT 
    status,
    COUNT(*) as file_count,
    AVG(record_count) as avg_records_per_file
FROM processed_files 
GROUP BY status;

-- Data Quality Metrics
SELECT 
    symbol,
    COUNT(*) as total_records,
    COUNT(CASE WHEN price_change_pct IS NULL THEN 1 END) as missing_price_changes,
    AVG(volatility) as avg_volatility
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY symbol;
```

### **Operational Dashboards**
- **Pipeline Health**: Success/failure rates, processing times, throughput metrics
- **Data Quality**: Completeness, accuracy, and consistency indicators
- **Resource Utilization**: CPU, memory, and storage consumption patterns
- **Business Metrics**: Market coverage, data freshness, and analytical readiness

### **Alerting Strategy**
- **Critical Alerts**: Pipeline failures, data quality breaches, system outages
- **Warning Alerts**: Performance degradation, approaching resource limits
- **Informational**: Successful batch completions, data volume summaries

***

## **Performance Optimization**

### **Processing Efficiency**
- **Parallel File Processing**: Multiple files processed concurrently where possible
- **Chunked Loading**: Large datasets processed in manageable chunks
- **Connection Pooling**: Efficient database connection reuse
- **Memory Management**: Streaming processing for large files

### **Storage Optimization**
- **Partitioned Tables**: Date-based partitioning for query performance
- **Indexed Columns**: Strategic indexing on frequently queried fields
- **Compression**: Efficient storage formats for historical data
- **Archival Strategy**: Automated data lifecycle management


# **Section 4: System Operations**

## **Usage Guide**

### **Daily Operations Workflow**

The Crypto Data Platform is designed for autonomous operation with minimal manual intervention. Here's how to interact with the system once it's running:

#### **1. Dashboard Access & Navigation**

**Airflow Web Interface (Pipeline Management)**
```bash
# Access URL: http://localhost:8080
# Credentials: ****** / ******

# Key Operations:
1. Monitor DAG execution status
2. Trigger manual pipeline runs
3. View detailed task logs
4. Manage pipeline schedules
5. Troubleshoot failed tasks
```

**Primary DAGs Overview:**
- **`crypto_data_generator`**: Runs hourly, generates new market data
- **`crypto_etl_main`**: Runs every 15 minutes, processes unprocessed files

**Metabase Analytics Interface (Business Intelligence)**
```bash
# Access URL: http://localhost:3000
# Initial Setup: Create admin account on first visit

# Key Features:
1. Interactive dashboard creation
2. SQL query builder
3. Data visualization tools
4. Report sharing and scheduling
5. Real-time data exploration
```

**MinIO Storage Console (Data Lake Management)**
```bash
# Access URL: http://localhost:9001
# Credentials: ****** / ******

# Key Operations:
1. Browse raw data files
2. Monitor storage utilization
3. Manage bucket policies
4. Download/upload files manually
5. View access logs
```
![minio](assets/Screenshot%202025-09-18%20220809.png)
***

## **Dashboard & Visualization**

### **Analytics Dashboards**

![dashboard](assets/Screenshot%202025-09-21%20173123.png)

#### **Executive Overview Dashboard**
**Purpose**: High-level business metrics for stakeholders  
**Key Visualizations**:

**Market Dominance**:
Shows the relative market share of each cryptocurrency based on market capitalization over the last 24 hours. This pie chart reveals which coins control the largest portions of the crypto market and helps identify market leaders versus smaller players.

```sql
-- Market Dominance
SELECT 
    symbol as "Cryptocurrency",
    AVG(market_cap) as "Market Cap (USD)"
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY symbol
ORDER BY AVG(market_cap) DESC

```

#### **Technical Analysis Dashboard**
**Purpose**: Detailed price movement and trading analysis  
**Key Visualizations**:

**24 Hr Average Volatility**
Displays the average price volatility for each cryptocurrency over the past 24 hours, ranked from most to least volatile. Higher volatility indicates greater price swings and potential trading opportunities, while lower volatility suggests more stable price movements.

```sql
--24 hr average volatility
SELECT 
    symbol,
    AVG(volatility) as avg_volatility
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY symbol
ORDER BY avg_volatility DESC
```

**% Price Change Per Day**
Tracks the hourly percentage price movements for each cryptocurrency throughout the last 24 hours. This visualization helps identify price trends, momentum shifts, and periods of significant market activity or stability.

```sql
--% Price Change Per Day
SELECT 
    symbol,
    DATE_TRUNC('hour', timestamp) as hour,
    price_change_pct
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
```

**24 Hr Trading Volume**
Shows the total trading volume for each cryptocurrency aggregated by hour over the past 24 hours. Higher volume typically indicates greater market interest and liquidity, while volume spikes often correlate with significant price movements or news events.

```sql
--24 Hr Trading volume
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    symbol,
    SUM(volume) as hourly_volume
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY hour, symbol
ORDER BY hour
```


#### **Operational Monitoring Dashboard**
**Purpose**: Pipeline health and data quality monitoring  
**Key Visualizations**:

```sql
-- Pipeline Processing Status
SELECT 
    status as "Processing Status",
    COUNT(*) as "File Count",
    AVG(record_count) as "Avg Records per File"
FROM processed_files 
GROUP BY status;

-- Data Freshness Indicator
SELECT 
    symbol as "Cryptocurrency",
    MAX(timestamp) as "Latest Data",
    NOW() - MAX(timestamp) as "Data Age"
FROM processed_prices 
GROUP BY symbol
ORDER BY MAX(timestamp) DESC;
```


## **Monitoring & Logging**

### **System Health Monitoring**

#### **Service Status Checks**
```bash
# Check all service health
docker-compose ps

# Expected output:
# postgres-analytics    Up    5432/tcp
# minio                Up    9000/tcp, 9001/tcp  
# airflow-webserver    Up    8080/tcp
# metabase            Up    3000/tcp
# redis               Up    6379/tcp

# Detailed service logs
docker-compose logs -f [service-name]
```

#### **Database Health Monitoring**
```sql
-- Connection and performance metrics
SELECT 
    datname as "Database",
    numbackends as "Active Connections",
    xact_commit as "Transactions Committed",
    xact_rollback as "Transactions Rolled Back"
FROM pg_stat_database 
WHERE datname = 'crypto_analytics_dev';

-- Table size and activity
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as "Size",
    n_tup_ins as "Inserts",
    n_tup_upd as "Updates"
FROM pg_stat_user_tables;
```

#### **ETL Pipeline Monitoring**
```sql
-- Processing performance metrics
SELECT 
    DATE_TRUNC('hour', processed_at) as "Hour",
    COUNT(*) as "Files Processed",
    AVG(record_count) as "Avg Records",
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as "Failed Files"
FROM processed_files 
WHERE processed_at >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', processed_at)
ORDER BY "Hour" DESC;
```

### **Logging Architecture**

#### **Application Logging Configuration**
```python
# etl/logger_config.py
import logging
import sys
from datetime import datetime

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('crypto_data_platform')
```

#### **Log Analysis Queries**
```bash
# View recent ETL logs
docker-compose exec airflow-webserver tail -f /opt/airflow/logs/etl_pipeline.log

# Search for specific patterns
docker-compose exec airflow-webserver grep -i "error\|failed" /opt/airflow/logs/etl_pipeline.log

# Monitor Airflow task logs
# Navigate to: http://localhost:8080/graph?dag_id=crypto_etl_main
# Click on task → View Logs
```

***

## **Troubleshooting Guide**

### **Common Issues & Solutions**

#### **1. Pipeline Failures**

**Symptom**: ETL DAG shows red (failed) status  
**Diagnosis**:
```bash
# Check Airflow task logs
1. Navigate to http://localhost:8080
2. Click on failed DAG run
3. Click on red task → View Logs
4. Look for error traceback
```

**Common Causes & Solutions**:
```bash
# Database Connection Issues
Error: "connection refused"
Solution: 
- Check if PostgreSQL container is running
- Verify connection string in .env file
- Ensure network connectivity between containers

# MinIO Access Issues  
Error: "SignatureDoesNotMatch"
Solution:
- Verify MinIO credentials in environment variables
- Check if MinIO service is accessible
- Validate bucket permissions

# Data Quality Failures
Error: "Data validation failed"
Solution:
- Check data quality logs for specific validation errors
- Review input data structure and content
- Verify business rule validation logic
```

#### **2. Dashboard Issues**

**Symptom**: Metabase shows no data or connection errors  
**Diagnosis & Solutions**:
```bash
# Database Connection Test
1. Go to Metabase Admin → Databases
2. Click "Test Connection" on crypto database
3. If failed, verify PostgreSQL accessibility

# Data Refresh Issues
1. Check if ETL pipeline is running successfully
2. Verify data exists in processed_prices table
3. Refresh Metabase data model if schema changed

# Performance Issues
1. Check for missing indexes on frequently queried columns
2. Review query performance in PostgreSQL logs
3. Consider data aggregation for large datasets
```

#### **3. Container Issues**

**Symptom**: Services not starting or crashing  
**Diagnosis & Solutions**:
```bash
# Check container status
docker-compose ps

# View container logs
docker-compose logs [service-name]


# Port conflicts
- Check if ports 8080, 3000, 9000 are available
- Change ports in docker-compose.yml if needed

# Volume permission issues
- Check volume mount permissions
- Run: docker-compose down -v && docker-compose up -d
```

### **Performance Optimization**

#### **Database Tuning**
```sql
-- Create performance indexes
CREATE INDEX IF NOT EXISTS idx_processed_prices_symbol_timestamp 
ON processed_prices(symbol, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_processed_files_processed_at 
ON processed_files(processed_at DESC);

-- Analyze query performance
EXPLAIN ANALYZE 
SELECT symbol, AVG(close) 
FROM processed_prices 
WHERE timestamp >= NOW() - INTERVAL '24 hours' 
GROUP BY symbol;
```


# **Section 5: Development & Quality**

## **Complete Project Structure**

***

```
crypto-data-platform/
├── README.md                           # Comprehensive technical documentation
├── requirements.txt                     # Production dependencies
├── requirements-dev.txt                 # Development & testing dependencies  
├── .gitignore                          # Version control exclusions
├── pytest.ini                          # Testing framework configuration
├── .pre-commit-config.yaml             # Code quality hooks
├── .github/workflows/
│   └── ci.yml                             # Automated CI/CD pipeline
└── assets/
    ├── min.drawio.png                     # System architecture diagram
    └── Screenshot_*.png                   # Dashboard & system screenshots
```

***

## **Core ETL Pipeline Components**

### **Airflow Orchestration** (`docker/dags/`)

```
docker/dags/
├── crypto_data_generator_dag.py           # Automated hourly data generation
├── crypto_etl_dag.py                     # Robust main ETL pipeline
└── etl/                                  # Core processing modules
    ├── __init__.py                       # Package initialization
    ├── extract.py                        # MinIO S3-compatible data extraction
    ├── transform.py                      # Business logic & data enrichment
    ├── load.py                          # PostgreSQL analytical database loading
    ├── data_quality.py                 # Multi-layer validation framework
    ├── file_processor.py               # Idempotent file state management
    ├── pipeline.py                     # End-to-end workflow coordination
    └── logger_config.py                # Centralized structured logging
```

### **Data Generation & Utilities** (`docker/dags/scripts/`)

```
scripts/
├── generate_data.py                      # Realistic cryptocurrency market simulation
├── upload_to_minio.py                   # Object storage upload utilities
└── data/                                # Sample dataset (development only)
    ├── crypto_data_20250917_12.csv      # Hourly OHLCV market data
    ├── crypto_data_20250917_13.csv      # Structured time-series format
    └── ... (additional hourly files)     # Historical data samples
```

***

## **Infrastructure & Service Orchestration**

### **Docker Container Architecture** (`docker/`)

```
docker/
├── docker-compose.yml                    # Multi-service orchestration
├── config/                             # Service-specific configurations
├── dags/                               # Dags and utils scripts
├── plugins/
└── logs/                               # Centralized application logging
```

***

## **Data & Analytics Infrastructure**

### **Database Schemas** (`sql/`)

```
sql/
├── create_processed_files_table.sql      # File processing state tracking
├── create_processed_prices.sql           # Analytics-ready price data schema
```


***

## **Quality Assurance & Testing**

### **Comprehensive Testing Framework** (`tests/`)

```
tests/
├── unit/                                # Component-level testing
│   ├── test_extract.py                  # Data extraction validation
│   ├── test_transform.py               # Business logic verification
│   ├── test_data_quality.py            # Quality framework testing
│ 
├── integration/                         # Cross-component testing
│   ├── test_etl_pipeline.py            # End-to-end workflow validation
```

### **CI/CD & Automation** (`.github/workflows/`)

```
.github/workflows/
├── ci.yml                               # Continuous integration pipeline
├── cd.yml                               # Continuous deployment automation
```

### **Modular Codebase Design**

The Crypto Data Platform follows enterprise-grade software engineering principles with a clear separation of concerns and modular architecture that promotes maintainability, testability, and scalability.

#### **Project Directory Structure**
### **Design Principles Implementation**

#### **1. Single Responsibility Principle**
Each module has a focused, well-defined purpose:

```python
# etl/extract.py - Focused solely on data extraction
class MinioExtractor:
    def list_files(self, prefix=""):
        """List files in MinIO bucket with optional prefix"""
        
    def read_csv(self, file_path):
        """Read CSV file from MinIO storage"""

# etl/transform.py - Dedicated to data transformation
def validate_data(df: pd.DataFrame) -> bool:
    """Validate data structure and quality"""
    
def add_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate derived business metrics"""
```

***

## **Testing Strategy**

### **Comprehensive Testing Framework**
The current CI/CD pipeline includes:
- Code quality checks (linting, type checking, security scanning)
- Docker Compose configuration validation

Unit tests have been written but are not yet integrated into the pipeline and will be implemented in subsequent iterations.

## **Contributing Guidelines**

### **Development Workflow**

#### **Branching Strategy**
```bash
# Feature development workflow
git checkout -b feature/improve-data-quality-validation
# Develop feature with tests
# Commit with conventional commits format

# Branch naming conventions:
# feature/description-of-feature
# bugfix/description-of-bug
# hotfix/critical-issue-description
# docs/documentation-updates
```

#### **Commit Message Standards**
```bash
# Conventional Commits format
type(scope): description

# Types: feat, fix, docs, style, refactor, test, chore
# Examples:
feat(etl): add comprehensive data quality validation
fix(transform): resolve price change calculation error
docs(readme): update installation instructions
test(integration): add PostgreSQL connection tests
```

#### **Code Review Process**
```markdown
## Pull Request Template

### Description
Brief description of changes and motivation

### Type of Change
- [ ] Bug fix (non-breaking change)
- [ ] New feature (non-breaking change)  
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation update

### Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed
- [ ] All tests pass locally

### Quality Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Code documentation updated
- [ ] No security vulnerabilities introduced
```

#### **Development Environment Setup**
```bash
# Setup development environment
python -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
pre-commit install

# Run quality checks before committing
flake8 etl/ dags/ scripts/
pytest tests/unit/ -v

# Integration testing
docker-compose -f docker-compose.test.yml up -d
pytest tests/integration/ -v
```

### **Security & Best Practices**

#### **Security Code Review Checklist**
```python
# Security validation in code reviews
SECURITY_CHECKLIST = [
    "No hardcoded credentials or secrets",
    "SQL injection prevention (parameterized queries)",
    "Input validation for all external data",
    "Proper error handling without information leakage", 
    "Authentication and authorization checks",
    "Secure communication protocols (TLS/SSL)",
    "Sensitive data encryption at rest and in transit",
    "Proper logging without exposing secrets"
]
```

#### **Performance Guidelines**
```python
# Performance best practices
PERFORMANCE_GUIDELINES = [
    "Database queries use appropriate indexes",
    "Bulk operations for large datasets",
    "Connection pooling for database access",
    "Efficient data structures and algorithms",
    "Memory usage optimization for large files",
    "Parallel processing where applicable",
    "Caching for frequently accessed data",
    "Resource cleanup in finally blocks"
]
```
