# tests/test_incremental_load.py
# Unit tests for v0.3.0 incremental load functionality
# Run with: pytest tests/ -v --cov=src

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from sqlalchemy import create_engine, text
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect_pipeline import (
    get_watermark,
    load_incremental_data,
    quality_gate_incremental,
    update_watermark
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def test_db():
    """
    Create in-memory SQLite database with control.watermarks table.
    Yields engine, then cleans up.
    """
    engine = create_engine('sqlite:///:memory:')
    
    # Create control schema (SQLite doesn't have schemas, so we use table naming)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE control_watermarks (
                watermark_id INTEGER PRIMARY KEY,
                table_name TEXT UNIQUE NOT NULL,
                last_processed_id INTEGER DEFAULT 0,
                last_processed_timestamp TIMESTAMP DEFAULT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active'
            )
        """))
        # Insert initial watermark for supply_chain_data
        conn.execute(text("""
            INSERT INTO control_watermarks (table_name, last_processed_id, status)
            VALUES ('supply_chain_data', 0, 'active')
        """))
    
    yield engine
    engine.dispose()


@pytest.fixture
def sample_df():
    """
    Create sample DataFrame mimicking supply_chain_data.csv structure.
    Includes 'id' column added by load_incremental_data().
    """
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'product_type': ['haircare', 'skincare', 'cosmetics', 'haircare', 'skincare'],
        'sku': ['SKU0', 'SKU1', 'SKU2', 'SKU3', 'SKU4'],
        'price': [69.80, 14.84, 11.31, 61.16, 4.80],
        'supplier_name': ['Supplier 3', 'Supplier 3', 'Supplier 1', 'Supplier 5', 'Supplier 1'],
        'location': ['Mumbai', 'Mumbai', 'Mumbai', 'Kolkata', 'Delhi']
    })


@pytest.fixture
def sample_csv_path(tmp_path):
    """
    Create temporary CSV file for testing load_incremental_data().
    """
    csv_file = tmp_path / "test_supply_chain.csv"
    df = pd.DataFrame({
        'Product type': ['haircare', 'skincare', 'cosmetics', 'haircare', 'skincare'],
        'SKU': ['SKU0', 'SKU1', 'SKU2', 'SKU3', 'SKU4'],
        'Price': [69.80, 14.84, 11.31, 61.16, 4.80],
        'Supplier name': ['Supplier 3', 'Supplier 3', 'Supplier 1', 'Supplier 5', 'Supplier 1'],
        'Location': ['Mumbai', 'Mumbai', 'Mumbai', 'Kolkata', 'Delhi']
    })
    df.to_csv(csv_file, index=False)
    return str(csv_file)


# ============================================================================
# TESTS: get_watermark()
# ============================================================================

class TestGetWatermark:
    """Tests for get_watermark task."""
    
    @patch('prefect_pipeline.create_engine')
    @patch('prefect_pipeline.get_run_logger')
    def test_get_watermark_first_run(self, mock_logger, mock_engine_factory):
        """
        Test: get_watermark() returns 0 when table doesn't exist (first run).
        """
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.scalar.return_value = None  # No watermark found
        mock_engine_factory.return_value = mock_engine
        
        mock_logger.return_value = MagicMock()
        
        # Call function
        result = get_watermark('supply_chain_data', 'postgresql://localhost/test')
        
        # Assert
        assert result == 0, "First run should return watermark=0"
        mock_engine.dispose.assert_called_once()
    
    @patch('prefect_pipeline.create_engine')
    @patch('prefect_pipeline.get_run_logger')
    def test_get_watermark_existing(self, mock_logger, mock_engine_factory):
        """
        Test: get_watermark() returns correct value if watermark exists.
        """
        # Mock engine and connection
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.scalar.return_value = 100  # Watermark found
        mock_engine_factory.return_value = mock_engine
        
        mock_logger.return_value = MagicMock()
        
        # Call function
        result = get_watermark('supply_chain_data', 'postgresql://localhost/test')
        
        # Assert
        assert result == 100, "Should return existing watermark value (100)"
        mock_engine.dispose.assert_called_once()


# ============================================================================
# TESTS: load_incremental_data()
# ============================================================================

class TestLoadIncrementalData:
    """Tests for load_incremental_data task."""
    
    @patch('prefect_pipeline.get_run_logger')
    def test_load_incremental_filters_correctly(self, mock_logger, sample_csv_path):
        """
        Test: load_incremental_data() filters rows where id > watermark.
        """
        mock_logger.return_value = MagicMock()
        
        # Call with watermark=2, expect ids 3, 4, 5
        df_new, max_id = load_incremental_data(sample_csv_path, watermark=2)
        
        # Assert
        assert len(df_new) == 3, "Should have 3 new rows (ids 3, 4, 5)"
        assert max_id == 5, "Max ID should be 5"
        assert list(df_new['id'].values) == [3, 4, 5], "IDs should be [3, 4, 5]"
    
    @patch('prefect_pipeline.get_run_logger')
    def test_load_incremental_no_new_data(self, mock_logger, sample_csv_path):
        """
        Test: load_incremental_data() returns empty DataFrame if all rows processed.
        """
        mock_logger.return_value = MagicMock()
        
        # Call with watermark=5 (all rows already processed)
        df_new, max_id = load_incremental_data(sample_csv_path, watermark=5)
        
        # Assert
        assert df_new.empty, "Should return empty DataFrame when no new data"
        assert max_id == 5, "Max ID should remain at watermark value"
    
    @patch('prefect_pipeline.get_run_logger')
    def test_load_incremental_first_run(self, mock_logger, sample_csv_path):
        """
        Test: load_incremental_data() loads all rows on first run (watermark=0).
        """
        mock_logger.return_value = MagicMock()
        
        # Call with watermark=0 (first run)
        df_new, max_id = load_incremental_data(sample_csv_path, watermark=0)
        
        # Assert
        assert len(df_new) == 5, "First run should load all 5 rows"
        assert max_id == 5, "Max ID should be 5"


# ============================================================================
# TESTS: quality_gate_incremental()
# ============================================================================

class TestQualityGateIncremental:
    """Tests for quality_gate_incremental task."""
    
    @patch('prefect_pipeline.get_run_logger')
    def test_quality_gate_valid_data(self, mock_logger, sample_df):
        """
        Test: quality_gate_incremental() passes valid data.
        """
        mock_logger.return_value = MagicMock()
        
        # Call with valid data
        result = quality_gate_incremental(sample_df)
        
        # Assert
        assert result is True, "Valid data should pass quality gate"
    
    @patch('prefect_pipeline.get_run_logger')
    def test_quality_gate_empty_dataframe(self, mock_logger):
        """
        Test: quality_gate_incremental() passes empty DataFrame (no new data).
        """
        mock_logger.return_value = MagicMock()
        df_empty = pd.DataFrame()
        
        # Call with empty DataFrame
        result = quality_gate_incremental(df_empty)
        
        # Assert
        assert result is True, "Empty DataFrame should pass quality gate"
    
    @patch('prefect_pipeline.get_run_logger')
    def test_quality_gate_missing_id_column(self, mock_logger):
        """
        Test: quality_gate_incremental() fails if 'id' column is missing.
        """
        mock_logger.return_value = MagicMock()
        df_no_id = pd.DataFrame({
            'product_type': ['haircare', 'skincare'],
            'sku': ['SKU0', 'SKU1']
        })
        
        # Call should raise ValueError
        with pytest.raises(ValueError, match="'id' missing"):
            quality_gate_incremental(df_no_id)
    
    @patch('prefect_pipeline.get_run_logger')
    def test_quality_gate_null_ids(self, mock_logger):
        """
        Test: quality_gate_incremental() fails on null values in 'id' column.
        """
        mock_logger.return_value = MagicMock()
        df_null_id = pd.DataFrame({
            'id': [1, None, 3],
            'product_type': ['haircare', 'skincare', 'cosmetics']
        })
        
        # Call should raise ValueError
        with pytest.raises(ValueError, match="Null values"):
            quality_gate_incremental(df_null_id)
    
    @patch('prefect_pipeline.get_run_logger')
    def test_quality_gate_duplicate_ids(self, mock_logger):
        """
        Test: quality_gate_incremental() fails on duplicate IDs.
        """
        mock_logger.return_value = MagicMock()
        df_dup_id = pd.DataFrame({
            'id': [1, 2, 2, 3],  # ID 2 is duplicated
            'product_type': ['haircare', 'skincare', 'skincare', 'cosmetics']
        })
        
        # Call should raise ValueError
        with pytest.raises(ValueError, match="duplicate"):
            quality_gate_incremental(df_dup_id)


# ============================================================================
# TESTS: update_watermark()
# ============================================================================

class TestUpdateWatermark:
    """Tests for update_watermark task."""
    
    @patch('prefect_pipeline.create_engine')
    @patch('prefect_pipeline.get_run_logger')
    def test_update_watermark_new_table(self, mock_logger, mock_engine_factory):
        """
        Test: update_watermark() inserts new watermark if table doesn't exist.
        """
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_engine_factory.return_value = mock_engine
        mock_logger.return_value = MagicMock()
        
        # Call function
        update_watermark('supply_chain_data', 100, 'postgresql://localhost/test')
        
        # Assert execute was called
        assert mock_conn.execute.called, "Should execute update/insert SQL"
        mock_engine.dispose.assert_called_once()
    
    @patch('prefect_pipeline.create_engine')
    @patch('prefect_pipeline.get_run_logger')
    def test_update_watermark_existing(self, mock_logger, mock_engine_factory):
        """
        Test: update_watermark() updates existing watermark value.
        """
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_engine_factory.return_value = mock_engine
        mock_logger.return_value = MagicMock()
        
        # Call function
        update_watermark('supply_chain_data', 150, 'postgresql://localhost/test')
        
        # Assert execute was called
        assert mock_conn.execute.called, "Should execute update SQL"
        mock_engine.dispose.assert_called_once()


# ============================================================================
# INTEGRATION TEST
# ============================================================================

class TestIncrementalLoadIntegration:
    """Integration test: watermark → load → quality gate → update flow."""
    
    @patch('prefect_pipeline.create_engine')
    @patch('prefect_pipeline.get_run_logger')
    def test_incremental_flow(self, mock_logger, mock_engine_factory, sample_csv_path):
        """
        Test: Complete incremental load flow (get watermark → load → validate → update).
        """
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_engine.begin.return_value.__enter__.return_value = mock_conn
        mock_conn.execute.return_value.scalar.return_value = 2  # Watermark exists
        mock_engine_factory.return_value = mock_engine
        mock_logger.return_value = MagicMock()
        
        # Step 1: Get watermark
        watermark = get_watermark('supply_chain_data', 'postgresql://localhost/test')
        assert watermark == 2, "Should retrieve watermark=2"
        
        # Step 2: Load incremental data
        df_new, max_id = load_incremental_data(sample_csv_path, watermark)
        assert len(df_new) == 3, "Should load 3 new rows (ids 3, 4, 5)"
        assert max_id == 5, "Max ID should be 5"
        
        # Step 3: Quality gate
        result = quality_gate_incremental(df_new)
        assert result is True, "Data should pass quality gate"
        
        # Step 4: Update watermark
        update_watermark('supply_chain_data', max_id, 'postgresql://localhost/test')
        assert mock_conn.execute.called, "Should have executed update"


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])