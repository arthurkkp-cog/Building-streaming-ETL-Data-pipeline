-- Pipeline Monitoring and Data Quality Tracking
-- PostgreSQL Stored Procedures for Streaming ETL Pipeline
-- Execute this script against the airflowdb database

-- ============================================================================
-- STEP 1: Create Database Schema and Tables
-- ============================================================================

-- Create schema for pipeline operations
CREATE SCHEMA IF NOT EXISTS pipeline_ops;

-- Tables for tracking pipeline metrics
CREATE TABLE pipeline_ops.run_metrics (
    run_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INTEGER,
    status VARCHAR(50),
    error_message TEXT
);

CREATE TABLE pipeline_ops.data_quality (
    check_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline_ops.run_metrics(run_id),
    check_type VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL,
    check_details JSONB
);

-- ============================================================================
-- STEP 2: Create Pipeline Monitoring Stored Procedures
-- ============================================================================

-- Record pipeline run start
CREATE OR REPLACE FUNCTION pipeline_ops.log_run_start(
    p_dag_id VARCHAR(250),
    p_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) RETURNS INTEGER AS $$
DECLARE
    v_run_id INTEGER;
BEGIN
    INSERT INTO pipeline_ops.run_metrics (dag_id, start_time, status)
    VALUES (p_dag_id, p_start_time, 'RUNNING')
    RETURNING run_id INTO v_run_id;
    
    RETURN v_run_id;
END;
$$ LANGUAGE plpgsql;

-- Record pipeline run completion
CREATE OR REPLACE FUNCTION pipeline_ops.log_run_complete(
    p_run_id INTEGER,
    p_end_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    p_records_processed INTEGER DEFAULT 0,
    p_status VARCHAR(50) DEFAULT 'SUCCESS',
    p_error_message TEXT DEFAULT NULL
) RETURNS BOOLEAN AS $$
BEGIN
    UPDATE pipeline_ops.run_metrics 
    SET end_time = p_end_time,
        records_processed = p_records_processed,
        status = p_status,
        error_message = p_error_message
    WHERE run_id = p_run_id;
    
    RETURN FOUND;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STEP 3: Create Data Quality Stored Procedures
-- ============================================================================

-- Validate data completeness
CREATE OR REPLACE FUNCTION pipeline_ops.check_data_completeness(
    p_run_id INTEGER,
    p_expected_count INTEGER,
    p_actual_count INTEGER
) RETURNS BOOLEAN AS $$
DECLARE
    v_check_result VARCHAR(20);
    v_details JSONB;
BEGIN
    IF p_actual_count >= p_expected_count THEN
        v_check_result := 'PASS';
    ELSE
        v_check_result := 'FAIL';
    END IF;
    
    v_details := jsonb_build_object(
        'expected_count', p_expected_count,
        'actual_count', p_actual_count,
        'completeness_ratio', ROUND(p_actual_count::NUMERIC / p_expected_count, 4)
    );
    
    INSERT INTO pipeline_ops.data_quality (run_id, check_type, check_result, check_details)
    VALUES (p_run_id, 'completeness_check', v_check_result, v_details);
    
    RETURN v_check_result = 'PASS';
END;
$$ LANGUAGE plpgsql;

-- Check for data anomalies
CREATE OR REPLACE FUNCTION pipeline_ops.detect_anomalies(
    p_run_id INTEGER,
    p_table_name TEXT,
    p_column_name TEXT,
    p_threshold NUMERIC
) RETURNS INTEGER AS $$
DECLARE
    v_anomaly_count INTEGER;
BEGIN
    EXECUTE format('
        SELECT COUNT(*) 
        FROM %I 
        WHERE %I > %L', 
        p_table_name, p_column_name, p_threshold
    ) INTO v_anomaly_count;
    
    INSERT INTO pipeline_ops.data_quality (run_id, check_type, check_result, check_details)
    VALUES (
        p_run_id, 
        'anomaly_detection', 
        CASE WHEN v_anomaly_count = 0 THEN 'PASS' ELSE 'FAIL' END,
        jsonb_build_object('anomaly_count', v_anomaly_count, 'threshold', p_threshold)
    );
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- STEP 4: Create Monitoring Views
-- ============================================================================

-- Pipeline health dashboard view
CREATE OR REPLACE VIEW pipeline_ops.pipeline_health AS
SELECT 
    dag_id,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_runs,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
    MAX(start_time) as last_run_time
FROM pipeline_ops.run_metrics 
GROUP BY dag_id;

-- Recent data quality checks
CREATE OR REPLACE VIEW pipeline_ops.recent_quality_checks AS
SELECT 
    rm.dag_id,
    rm.start_time,
    dq.check_type,
    dq.check_result,
    dq.check_details
FROM pipeline_ops.run_metrics rm
JOIN pipeline_ops.data_quality dq ON rm.run_id = dq.run_id
WHERE rm.start_time > CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY rm.start_time DESC;
