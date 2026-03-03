-- =============================================
-- Data Vault 2.0 Implementation Guide
-- ETL Control Framework Setup - Version 3.2
-- UPDATED: Added usp_StartStep/usp_EndStep, Fixed usp_LogStep
-- ENHANCEMENTS: Configuration Table, Retry Logic, Enhanced Monitoring
-- v3.2 FIXES:
--   - ETL_ErrorLog: Removed always-NULL T-SQL columns (ErrorNumber, ErrorState, ErrorProcedure, ErrorLine)
--   - usp_LogError: Simplified to match cleaned ETL_ErrorLog
--   - usp_StartStep: Output parameter renamed @StepLogID → @StepID to match SSIS guide
--   - usp_EndStep: Parameter names aligned with SSIS guide (@StepID, @Status, @RowCount)
-- =============================================

-- Create ETL Control Database with Dynamic Path
USE master;
GO

-- Close any existing connections and drop database if exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'ETL_Control')
BEGIN
    ALTER DATABASE ETL_Control SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ETL_Control;
END
GO

CREATE DATABASE ETL_Control;
GO

USE ETL_Control;
GO

-- =============================================
-- ENHANCEMENT: Configuration Table
-- =============================================

CREATE TABLE ETL_Configuration (
    ConfigID INT IDENTITY(1,1) PRIMARY KEY,
    ConfigKey VARCHAR(100) NOT NULL UNIQUE,
    ConfigValue VARCHAR(500) NOT NULL,
    ConfigDataType VARCHAR(20) NOT NULL DEFAULT 'VARCHAR', -- VARCHAR, INT, DECIMAL, BIT
    ConfigDescription VARCHAR(500),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- Insert default configuration values
INSERT INTO ETL_Configuration (ConfigKey, ConfigValue, ConfigDataType, ConfigDescription) VALUES
('BatchSize', '10000', 'INT', 'Records per batch for large loads'),
('RetryAttempts', '3', 'INT', 'Number of retry attempts for transient failures'),
('RetryDelaySeconds', '5', 'INT', 'Seconds to wait between retry attempts'),
('ParallelDegree', '4', 'INT', 'Degree of parallelism for loads'),
('MaxExecutionTimeMinutes', '120', 'INT', 'Maximum execution time before alerting'),
('ErrorThresholdPercent', '1.0', 'DECIMAL', 'Maximum error percentage threshold'),
('DataQualityCheckEnabled', '1', 'INT', 'Enable data quality checks (1=on, 0=off)'),
('AlertOnFailure', '1', 'INT', 'Send alerts on batch failures (1=on, 0=off)'),
('ArchiveLogDays', '90', 'INT', 'Days to retain batch logs'),
('EnableDetailedLogging', '1', 'INT', 'Enable detailed step logging (1=on, 0=off)');
GO

-- =============================================
-- Create Control Tables
-- =============================================

-- ETL Process Registry
CREATE TABLE ETL_Process (
    ProcessID INT IDENTITY(1,1) PRIMARY KEY,
    ProcessName VARCHAR(100) NOT NULL UNIQUE,
    ProcessDescription VARCHAR(500),
    SourceSystem VARCHAR(50) NOT NULL,
    TargetSystem VARCHAR(50) NOT NULL,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- Batch Execution Log
CREATE TABLE ETL_BatchLog (
    BatchID INT IDENTITY(1,1) PRIMARY KEY,
    ProcessID INT NOT NULL,
    BatchStartTime DATETIME NOT NULL,
    BatchEndTime DATETIME NULL,
    BatchStatus VARCHAR(20) NOT NULL, -- Running, Success, Failed, Cancelled
    RecordsProcessed INT DEFAULT 0,
    RecordsInserted INT DEFAULT 0,
    RecordsUpdated INT DEFAULT 0,
    RecordsDeleted INT DEFAULT 0,
    ErrorCount INT DEFAULT 0,
    ExecutionTimeSeconds AS DATEDIFF(SECOND, BatchStartTime, ISNULL(BatchEndTime, GETDATE())),
    CreatedBy VARCHAR(100) DEFAULT SYSTEM_USER,
    ServerName VARCHAR(100) DEFAULT @@SERVERNAME, -- ENHANCEMENT: Track server
    DatabaseName VARCHAR(100) DEFAULT DB_NAME(), -- ENHANCEMENT: Track database
    FOREIGN KEY (ProcessID) REFERENCES ETL_Process(ProcessID)
);

-- Step Execution Log
CREATE TABLE ETL_StepLog (
    StepLogID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL,
    StepName VARCHAR(100) NOT NULL,
    StepStartTime DATETIME NOT NULL,
    StepEndTime DATETIME NULL,
    StepStatus VARCHAR(20) NOT NULL, -- Running, Success, Failed, Skipped, Retrying
    RecordsProcessed INT DEFAULT 0,
    RecordsInserted INT DEFAULT 0,
    RecordsUpdated INT DEFAULT 0,
    RecordsDeleted INT DEFAULT 0,
    ErrorMessage VARCHAR(MAX),
    RetryAttempt INT DEFAULT 0, -- ENHANCEMENT: Track retry attempts
    ExecutionTimeSeconds AS DATEDIFF(SECOND, StepStartTime, ISNULL(StepEndTime, GETDATE())),
    FOREIGN KEY (BatchID) REFERENCES ETL_BatchLog(BatchID)
);

-- Error Log
CREATE TABLE ETL_ErrorLog (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    StepLogID INT,
    ErrorDateTime DATETIME DEFAULT GETDATE(),
    ErrorSeverity VARCHAR(20) NOT NULL, -- Critical, High, Medium, Low
    ErrorSource VARCHAR(100) NOT NULL,
    ErrorMessage VARCHAR(MAX) NOT NULL,
    ErrorDetails VARCHAR(MAX),
    IsResolved BIT DEFAULT 0,
    ResolvedBy VARCHAR(100),
    ResolvedDate DATETIME,
    ResolutionNotes VARCHAR(MAX),
    FOREIGN KEY (BatchID) REFERENCES ETL_BatchLog(BatchID),
    FOREIGN KEY (StepLogID) REFERENCES ETL_StepLog(StepLogID)
);

-- Watermark Tracking
CREATE TABLE ETL_Watermark (
    WatermarkID INT IDENTITY(1,1) PRIMARY KEY,
    ProcessID INT NOT NULL,
    TableName VARCHAR(100) NOT NULL,
    ColumnName VARCHAR(100) NOT NULL,
    LastValue VARCHAR(500),
    LastLoadDate DATETIME NOT NULL,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProcessID) REFERENCES ETL_Process(ProcessID),
    CONSTRAINT UQ_Watermark UNIQUE (ProcessID, TableName, ColumnName) -- ENHANCEMENT: Unique constraint
);

-- Data Quality Checks
CREATE TABLE ETL_DataQualityCheck (
    QualityCheckID INT IDENTITY(1,1) PRIMARY KEY,
    ProcessID INT NOT NULL,
    CheckName VARCHAR(100) NOT NULL,
    CheckDescription VARCHAR(500),
    CheckSQL VARCHAR(MAX) NOT NULL,
    ExpectedResult VARCHAR(100),
    Severity VARCHAR(20) DEFAULT 'Medium', -- ENHANCEMENT: Critical, High, Medium, Low
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ProcessID) REFERENCES ETL_Process(ProcessID)
);

-- Data Quality Results
CREATE TABLE ETL_DataQualityResult (
    QualityResultID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT NOT NULL,
    QualityCheckID INT NOT NULL,
    CheckDateTime DATETIME DEFAULT GETDATE(),
    ActualResult VARCHAR(100),
    CheckPassed BIT NOT NULL,
    ErrorMessage VARCHAR(MAX),
    FOREIGN KEY (BatchID) REFERENCES ETL_BatchLog(BatchID),
    FOREIGN KEY (QualityCheckID) REFERENCES ETL_DataQualityCheck(QualityCheckID)
);

-- ENHANCEMENT: Alert Log Table
CREATE TABLE ETL_AlertLog (
    AlertID INT IDENTITY(1,1) PRIMARY KEY,
    BatchID INT,
    AlertType VARCHAR(50) NOT NULL, -- Failure, Performance, DataQuality, Warning
    AlertMessage VARCHAR(MAX) NOT NULL,
    AlertDateTime DATETIME DEFAULT GETDATE(),
    IsSent BIT DEFAULT 0,
    SentDateTime DATETIME NULL,
    RecipientList VARCHAR(500),
    FOREIGN KEY (BatchID) REFERENCES ETL_BatchLog(BatchID)
);

-- =============================================
-- Insert Default ETL Processes
-- =============================================

INSERT INTO ETL_Process (ProcessName, ProcessDescription, SourceSystem, TargetSystem) VALUES
-- Main ETL Processes
('Load_Staging', 'Load data from source to staging layer', 'TaxSystemDB', 'DV_Staging'),
('Load_Bronze', 'Load data from staging to bronze layer (Data Vault)', 'DV_Staging', 'DV_Bronze'),
('Load_Silver', 'Load data from bronze to silver layer (Business Vault)', 'DV_Bronze', 'DV_Silver'),
('Load_Gold', 'Load data from silver to gold layer (Dimensional Model)', 'DV_Silver', 'DV_Gold'),
('Master_ETL', 'Complete ETL pipeline execution', 'TaxSystemDB', 'DV_Gold'),
-- Package-Level Processes
('Load_Staging_Full', 'Full load of all staging tables', 'TaxSystemDB', 'DV_Staging'),
('Load_All_Staging_Tables', 'Load all staging tables package', 'TaxSystemDB', 'DV_Staging'),
-- Individual Staging Table Processes
('Load_STG_Category', 'Load staging data for Category', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Structure', 'Load staging data for Structure', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Activity', 'Load staging data for Activity', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Taxpayer', 'Load staging data for Taxpayer', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Owner', 'Load staging data for Owner', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Officer', 'Load staging data for Officer', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_MonthlyDeclaration', 'Load staging data for MonthlyDeclaration', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_AnnualDeclaration', 'Load staging data for AnnualDeclaration', 'TaxSystemDB', 'DV_Staging'),
('Load_STG_Payment', 'Load staging data for Payment', 'TaxSystemDB', 'DV_Staging');
GO

-- =============================================
-- ENHANCEMENT: Configuration Helper Functions
-- =============================================

CREATE FUNCTION dbo.fn_GetConfigValue(@ConfigKey VARCHAR(100))
RETURNS VARCHAR(500)
AS
BEGIN
    DECLARE @Value VARCHAR(500);
    SELECT @Value = ConfigValue 
    FROM ETL_Configuration 
    WHERE ConfigKey = @ConfigKey AND IsActive = 1;
    RETURN @Value;
END;
GO

CREATE FUNCTION dbo.fn_GetConfigValueInt(@ConfigKey VARCHAR(100))
RETURNS INT
AS
BEGIN
    DECLARE @Value INT;
    SELECT @Value = CAST(ConfigValue AS INT)
    FROM ETL_Configuration 
    WHERE ConfigKey = @ConfigKey AND IsActive = 1 AND ConfigDataType = 'INT';
    RETURN ISNULL(@Value, 0);
END;
GO

-- =============================================
-- Core Utility Procedures
-- =============================================

-- Start Batch Execution
CREATE PROCEDURE usp_StartBatch
    @ProcessName VARCHAR(100),
    @BatchID INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessID INT;
    
    -- Get Process ID
    SELECT @ProcessID = ProcessID 
    FROM ETL_Process 
    WHERE ProcessName = @ProcessName AND IsActive = 1;
    
    IF @ProcessID IS NULL
    BEGIN
        RAISERROR('Process %s not found or inactive', 16, 1, @ProcessName);
        RETURN;
    END;
    
    -- Insert batch record
    INSERT INTO ETL_BatchLog (ProcessID, BatchStartTime, BatchStatus)
    VALUES (@ProcessID, GETDATE(), 'Running');
    
    SET @BatchID = SCOPE_IDENTITY();
    
    PRINT 'Batch ' + CAST(@BatchID AS VARCHAR(10)) + ' started for process ' + @ProcessName;
END;
GO

-- =============================================
-- Enhanced End Batch Execution with Auto-Calculation
-- =============================================
CREATE PROCEDURE usp_EndBatch
    @BatchID INT,
    @Status VARCHAR(20),
    @RecordsProcessed INT = NULL,
    @RecordsInserted INT = NULL,
    @RecordsUpdated INT = NULL,
    @RecordsDeleted INT = NULL,
    @ErrorCount INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Auto-Calculate Metrics from Step Logs if not provided
    IF @RecordsProcessed IS NULL OR @RecordsInserted IS NULL OR 
       @RecordsUpdated IS NULL OR @RecordsDeleted IS NULL OR @ErrorCount IS NULL
    BEGIN
        SELECT 
            @RecordsProcessed = ISNULL(SUM(CASE WHEN StepStatus = 'Success' THEN RecordsProcessed ELSE 0 END), 0),
            @RecordsInserted = ISNULL(SUM(CASE WHEN StepStatus = 'Success' THEN RecordsInserted ELSE 0 END), 0),
            @RecordsUpdated = ISNULL(SUM(CASE WHEN StepStatus = 'Success' THEN RecordsUpdated ELSE 0 END), 0),
            @RecordsDeleted = ISNULL(SUM(CASE WHEN StepStatus = 'Success' THEN RecordsDeleted ELSE 0 END), 0),
            @ErrorCount = ISNULL(SUM(CASE WHEN StepStatus = 'Failed' THEN 1 ELSE 0 END), 0)
        FROM ETL_StepLog
        WHERE BatchID = @BatchID;
        
        PRINT 'Auto-calculated metrics from ETL_StepLog:';
        PRINT '  Records Processed: ' + CAST(@RecordsProcessed AS VARCHAR(20));
        PRINT '  Records Inserted: ' + CAST(@RecordsInserted AS VARCHAR(20));
        PRINT '  Records Updated: ' + CAST(@RecordsUpdated AS VARCHAR(20));
        PRINT '  Records Deleted: ' + CAST(@RecordsDeleted AS VARCHAR(20));
        PRINT '  Error Count: ' + CAST(@ErrorCount AS VARCHAR(10));
    END;
    
    -- Update Batch Record
    UPDATE ETL_BatchLog 
    SET BatchEndTime = GETDATE(),
        BatchStatus = @Status,
        RecordsProcessed = @RecordsProcessed,
        RecordsInserted = @RecordsInserted,
        RecordsUpdated = @RecordsUpdated,
        RecordsDeleted = @RecordsDeleted,
        ErrorCount = @ErrorCount
    WHERE BatchID = @BatchID;
    
    DECLARE @ExecutionTime INT;
    SELECT @ExecutionTime = ExecutionTimeSeconds FROM ETL_BatchLog WHERE BatchID = @BatchID;
    
    PRINT 'Batch ' + CAST(@BatchID AS VARCHAR(10)) + ' completed with status: ' + @Status;
    PRINT 'Execution time: ' + CAST(@ExecutionTime AS VARCHAR(10)) + ' seconds';
    
    -- ENHANCEMENT: Check for alerts
    IF @Status = 'Failed' AND dbo.fn_GetConfigValueInt('AlertOnFailure') = 1
    BEGIN
        EXEC usp_CreateAlert 
            @BatchID = @BatchID,
            @AlertType = 'Failure',
            @AlertMessage = 'Batch execution failed';
    END;
    
    -- ENHANCEMENT: Check execution time threshold
    DECLARE @MaxExecTime INT = dbo.fn_GetConfigValueInt('MaxExecutionTimeMinutes') * 60;
    IF @ExecutionTime > @MaxExecTime
    BEGIN
        EXEC usp_CreateAlert 
            @BatchID = @BatchID,
            @AlertType = 'Performance',
            @AlertMessage = 'Batch execution exceeded time threshold';
    END;
END;
GO

-- =============================================
-- ENHANCEMENT: Log Step with Retry Support
-- =============================================
CREATE PROCEDURE usp_LogStep
    @BatchID INT,
    @StepName VARCHAR(100),
    @StepStatus VARCHAR(20),
    @RecordsProcessed INT = 0,
    @RecordsInserted INT = 0,
    @RecordsUpdated INT = 0,
    @RecordsDeleted INT = 0,
    @ErrorMessage VARCHAR(MAX) = NULL,
    @RetryAttempt INT = 0,
    @StepLogID INT = NULL OUTPUT  -- Made optional
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check if step is already running
    DECLARE @ExistingStepID INT;
    SELECT @ExistingStepID = StepLogID 
    FROM ETL_StepLog 
    WHERE BatchID = @BatchID 
    AND StepName = @StepName 
    AND StepEndTime IS NULL;
    
    IF @ExistingStepID IS NOT NULL AND @StepStatus IN ('Success', 'Failed', 'Skipped')
    BEGIN
        -- Update existing step
        UPDATE ETL_StepLog 
        SET StepEndTime = GETDATE(),
            StepStatus = @StepStatus,
            RecordsProcessed = @RecordsProcessed,
            RecordsInserted = @RecordsInserted,
            RecordsUpdated = @RecordsUpdated,
            RecordsDeleted = @RecordsDeleted,
            ErrorMessage = @ErrorMessage,
            RetryAttempt = @RetryAttempt
        WHERE StepLogID = @ExistingStepID;
        
        SET @StepLogID = @ExistingStepID;
    END
    ELSE
    BEGIN
        -- Insert new step log
        INSERT INTO ETL_StepLog (
            BatchID, StepName, StepStartTime, StepStatus,
            RecordsProcessed, RecordsInserted, RecordsUpdated, RecordsDeleted,
            ErrorMessage, RetryAttempt
        )
        VALUES (
            @BatchID, @StepName, GETDATE(), @StepStatus,
            @RecordsProcessed, @RecordsInserted, @RecordsUpdated, @RecordsDeleted,
            @ErrorMessage, @RetryAttempt
        );
        
        SET @StepLogID = SCOPE_IDENTITY();
    END;
    
    IF dbo.fn_GetConfigValueInt('EnableDetailedLogging') = 1
    BEGIN
        PRINT 'Step: ' + @StepName + ' - Status: ' + @StepStatus + 
              ' - Records: ' + CAST(@RecordsProcessed AS VARCHAR(10));
    END;
END;
GO

-- =============================================
-- Start Step - Simplified interface for starting steps
-- =============================================
CREATE PROCEDURE usp_StartStep
    @BatchID INT,
    @StepName VARCHAR(100),
    @StepID INT OUTPUT                  -- Matches SSIS guide: User::v_StepID
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert new step log record
    INSERT INTO ETL_StepLog (
        BatchID, 
        StepName, 
        StepStartTime, 
        StepStatus,
        RecordsProcessed,
        RecordsInserted,
        RecordsUpdated,
        RecordsDeleted,
        RetryAttempt
    )
    VALUES (
        @BatchID, 
        @StepName, 
        GETDATE(), 
        'Running',
        0,
        0,
        0,
        0,
        0
    );
    
    SET @StepID = SCOPE_IDENTITY();
    
    IF dbo.fn_GetConfigValueInt('EnableDetailedLogging') = 1
    BEGIN
        PRINT 'Step ' + CAST(@StepID AS VARCHAR(10)) + ' started: ' + @StepName;
    END;
END;
GO

-- =============================================
-- End Step - Simplified interface for ending steps
-- =============================================
CREATE PROCEDURE usp_EndStep
    @StepID INT,                        -- Matches SSIS guide: User::v_StepID
    @Status VARCHAR(20),                -- Matches SSIS guide: 'Success' or 'Failed'
    @RowCount INT = 0,                  -- Matches SSIS guide: User::v_RowCount
    @RecordsInserted INT = 0,
    @RecordsUpdated INT = 0,
    @RecordsDeleted INT = 0,
    @ErrorMessage VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- SSIS passes 0 (default INT) when variable is unset. Skip if no valid StepID.
    IF @StepID IS NULL OR @StepID = 0 RETURN;
    
    -- Update step log record
    UPDATE ETL_StepLog 
    SET StepEndTime = GETDATE(),
        StepStatus = @Status,
        RecordsProcessed = @RowCount,
        RecordsInserted = @RecordsInserted,
        RecordsUpdated = @RecordsUpdated,
        RecordsDeleted = @RecordsDeleted,
        ErrorMessage = @ErrorMessage
    WHERE StepLogID = @StepID;
    
    IF dbo.fn_GetConfigValueInt('EnableDetailedLogging') = 1
    BEGIN
        DECLARE @StepName VARCHAR(100);
        SELECT @StepName = StepName FROM ETL_StepLog WHERE StepLogID = @StepID;
        PRINT 'Step ' + CAST(@StepID AS VARCHAR(10)) + ' completed: ' + ISNULL(@StepName, '(unknown)') + 
              ' - Status: ' + @Status + ' - Records: ' + CAST(@RowCount AS VARCHAR(10));
    END;
END;
GO

-- =============================================
-- ENHANCEMENT: Enhanced Error Logging
-- =============================================
CREATE PROCEDURE usp_LogError
    @BatchID INT = NULL,
    @StepLogID INT = NULL,
    @ErrorSeverity VARCHAR(20),
    @ErrorSource VARCHAR(100),
    @ErrorMessage VARCHAR(MAX),
    @ErrorDetails VARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- SSIS passes 0 (default INT) when variable is unset. Convert to NULL for FK safety.
    SET @BatchID = NULLIF(@BatchID, 0);
    SET @StepLogID = NULLIF(@StepLogID, 0);
    
    INSERT INTO ETL_ErrorLog (
        BatchID, StepLogID, ErrorSeverity, ErrorSource, 
        ErrorMessage, ErrorDetails
    )
    VALUES (
        @BatchID, @StepLogID, @ErrorSeverity, @ErrorSource,
        @ErrorMessage, @ErrorDetails
    );
    
    PRINT 'Error logged: [' + @ErrorSeverity + '] ' + @ErrorMessage;
END;
GO

-- =============================================
-- ENHANCEMENT: Execute with Retry Logic
-- =============================================
CREATE PROCEDURE usp_ExecuteWithRetry
    @BatchID INT,
    @StepName VARCHAR(100),
    @SQLCommand NVARCHAR(MAX),
    @MaxRetries INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Get retry configuration
    IF @MaxRetries IS NULL
        SET @MaxRetries = dbo.fn_GetConfigValueInt('RetryAttempts');
    
    DECLARE @RetryDelay INT = dbo.fn_GetConfigValueInt('RetryDelaySeconds');
    DECLARE @RetryDelayStr VARCHAR(12) = '00:00:' + RIGHT('0' + CAST(@RetryDelay AS VARCHAR(2)), 2); -- FIX: WAITFOR DELAY requires 'HH:MM:SS' string
    DECLARE @RetryCount INT = 0;
    DECLARE @Success BIT = 0;
    DECLARE @ErrorMessage VARCHAR(MAX);
    DECLARE @StepLogID INT;
    
    WHILE @RetryCount <= @MaxRetries AND @Success = 0
    BEGIN
        BEGIN TRY
            -- Log step start
            EXEC usp_LogStep 
                @BatchID = @BatchID,
                @StepName = @StepName,
                @StepStatus = 'Running',
                @RetryAttempt = @RetryCount,
                @StepLogID = @StepLogID OUTPUT;
            
            -- Execute the command
            EXEC sp_executesql @SQLCommand;
            
            -- Log success
            EXEC usp_LogStep 
                @BatchID = @BatchID,
                @StepName = @StepName,
                @StepStatus = 'Success',
                @RetryAttempt = @RetryCount,
                @StepLogID = @StepLogID OUTPUT;
            
            SET @Success = 1;
            
            IF @RetryCount > 0
                PRINT 'Step succeeded after ' + CAST(@RetryCount AS VARCHAR(10)) + ' retry attempt(s)';
            
        END TRY
        BEGIN CATCH
            SET @ErrorMessage = ERROR_MESSAGE();
            
            IF @RetryCount < @MaxRetries
            BEGIN
                -- Log retry attempt
                EXEC usp_LogStep 
                    @BatchID = @BatchID,
                    @StepName = @StepName,
                    @StepStatus = 'Retrying',
                    @ErrorMessage = @ErrorMessage,
                    @RetryAttempt = @RetryCount,
                    @StepLogID = @StepLogID OUTPUT;
                
                PRINT 'Retry attempt ' + CAST(@RetryCount + 1 AS VARCHAR(10)) + 
                      ' of ' + CAST(@MaxRetries AS VARCHAR(10)) + ' for step: ' + @StepName;
                PRINT 'Error: ' + @ErrorMessage;
                PRINT 'Waiting ' + CAST(@RetryDelay AS VARCHAR(10)) + ' seconds before retry...';
                
                -- Wait before retry
                WAITFOR DELAY @RetryDelayStr;
                
                SET @RetryCount = @RetryCount + 1;
            END
            ELSE
            BEGIN
                -- Final failure
                EXEC usp_LogStep 
                    @BatchID = @BatchID,
                    @StepName = @StepName,
                    @StepStatus = 'Failed',
                    @ErrorMessage = @ErrorMessage,
                    @RetryAttempt = @RetryCount,
                    @StepLogID = @StepLogID OUTPUT;
                
                EXEC usp_LogError 
                    @BatchID = @BatchID,
                    @StepLogID = @StepLogID,
                    @ErrorSeverity = 'High',
                    @ErrorSource = @StepName,
                    @ErrorMessage = @ErrorMessage;
                
                PRINT 'Step failed after ' + CAST(@MaxRetries AS VARCHAR(10)) + ' retry attempts';
                
                -- Re-throw error
                THROW;
            END
        END CATCH
    END
END;
GO

-- =============================================
-- Watermark Management Procedures
-- =============================================

CREATE OR ALTER PROCEDURE usp_UpdateWatermark
    @ProcessName VARCHAR(100),
    @TableName VARCHAR(100),
    @ColumnName VARCHAR(100),
    @LastLoadDate DATETIME
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessID INT;
    
    SELECT @ProcessID = ProcessID 
    FROM ETL_Process 
    WHERE ProcessName = @ProcessName AND IsActive = 1;
    
    IF @ProcessID IS NULL
    BEGIN
        RAISERROR('Process %s not found or inactive', 16, 1, @ProcessName);
        RETURN;
    END;
    
    -- Use MERGE for atomic upsert
    MERGE ETL_Watermark AS target
    USING (SELECT @ProcessID AS ProcessID, @TableName AS TableName, @ColumnName AS ColumnName) AS source
    ON target.ProcessID = source.ProcessID 
       AND target.TableName = source.TableName 
       AND target.ColumnName = source.ColumnName
    WHEN MATCHED THEN
        UPDATE SET 
            LastValue = CONVERT(VARCHAR(500), @LastLoadDate, 120),
            LastLoadDate = @LastLoadDate,
            UpdatedDate = GETDATE()
    WHEN NOT MATCHED THEN
        INSERT (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
        VALUES (@ProcessID, @TableName, @ColumnName, 
                CONVERT(VARCHAR(500), @LastLoadDate, 120), @LastLoadDate);
    
    PRINT 'Watermark updated for ' + @TableName + '.' + @ColumnName + ' = ' + 
          CONVERT(VARCHAR(20), @LastLoadDate, 120);
END;
GO

CREATE OR ALTER PROCEDURE usp_GetWatermark
    @ProcessName VARCHAR(100),
    @TableName VARCHAR(100),
    @ColumnName VARCHAR(100),
    @LastLoadDate DATETIME OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessID INT;
    
    SELECT @ProcessID = ProcessID 
    FROM ETL_Process 
    WHERE ProcessName = @ProcessName AND IsActive = 1;
    
    IF @ProcessID IS NULL
    BEGIN
        RAISERROR('Process %s not found or inactive', 16, 1, @ProcessName);
        RETURN;
    END;
    
    SELECT @LastLoadDate = LastLoadDate
    FROM ETL_Watermark 
    WHERE ProcessID = @ProcessID 
      AND TableName = @TableName 
      AND ColumnName = @ColumnName 
      AND IsActive = 1;
    
    IF @LastLoadDate IS NULL
        SET @LastLoadDate = '1900-01-01';
    
    PRINT 'Watermark retrieved for ' + @TableName + '.' + @ColumnName + ' = ' + 
          CONVERT(VARCHAR(20), @LastLoadDate, 120);
    
    SELECT @LastLoadDate AS LastWatermarkValue;
END;
GO

-- =============================================
-- ENHANCEMENT: Alert Management
-- =============================================

CREATE PROCEDURE usp_CreateAlert
    @BatchID INT = NULL,
    @AlertType VARCHAR(50),
    @AlertMessage VARCHAR(MAX),
    @RecipientList VARCHAR(500) = 'ETL-Team@company.com'
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO ETL_AlertLog (
        BatchID, 
        AlertType, 
        AlertMessage, 
        AlertDateTime,
        IsSent,
        RecipientList
    )
    VALUES (
        @BatchID, 
        @AlertType, 
        @AlertMessage, 
        GETDATE(),
        0, -- Not sent yet
        @RecipientList
    );
    
    -- In production, integrate with sp_send_dbmail or monitoring system
    PRINT 'ALERT [' + @AlertType + ']: ' + @AlertMessage;
END;
GO

-- =============================================
-- Reporting and Monitoring Procedures
-- =============================================

CREATE PROCEDURE usp_GeneratePerformanceReport
    @ReportPeriodDays INT = 7
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartDate DATETIME = DATEADD(DAY, -@ReportPeriodDays, GETDATE());
    
    PRINT 'ETL Performance Report for last ' + CAST(@ReportPeriodDays AS VARCHAR(10)) + ' days';
    PRINT '================================================';
    
    -- Batch Summary
    SELECT 
        p.ProcessName,
        COUNT(*) AS BatchCount,
        SUM(CASE WHEN bl.BatchStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessCount,
        SUM(CASE WHEN bl.BatchStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedCount,
        AVG(bl.ExecutionTimeSeconds) AS AvgExecutionTimeSeconds,
        MAX(bl.ExecutionTimeSeconds) AS MaxExecutionTimeSeconds,
        SUM(bl.RecordsProcessed) AS TotalRecordsProcessed,
        SUM(bl.ErrorCount) AS TotalErrors
    FROM ETL_BatchLog bl
    INNER JOIN ETL_Process p ON bl.ProcessID = p.ProcessID
    WHERE bl.BatchStartTime >= @StartDate
    GROUP BY p.ProcessName
    ORDER BY AvgExecutionTimeSeconds DESC;
    
    -- Error Summary
    SELECT 
        p.ProcessName,
        el.ErrorSeverity,
        COUNT(*) AS ErrorCount,
        COUNT(DISTINCT el.ErrorMessage) AS UniqueErrors
    FROM ETL_ErrorLog el
    INNER JOIN ETL_BatchLog bl ON el.BatchID = bl.BatchID
    INNER JOIN ETL_Process p ON bl.ProcessID = p.ProcessID
    WHERE el.ErrorDateTime >= @StartDate
    GROUP BY p.ProcessName, el.ErrorSeverity
    ORDER BY ErrorCount DESC;
    
    -- Recent Batches
    SELECT TOP 10
        p.ProcessName,
        bl.BatchID,
        bl.BatchStartTime,
        bl.BatchStatus,
        bl.ExecutionTimeSeconds,
        bl.RecordsProcessed,
        bl.ErrorCount
    FROM ETL_BatchLog bl
    INNER JOIN ETL_Process p ON bl.ProcessID = p.ProcessID
    WHERE bl.BatchStartTime >= @StartDate
    ORDER BY bl.BatchStartTime DESC;
END;
GO

CREATE PROCEDURE usp_VerifyBatchMetrics
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT 'Verifying Batch Metrics for Batch ' + CAST(@BatchID AS VARCHAR(10));
    PRINT '================================================';
    
    SELECT 
        'Batch Totals' AS Source,
        bl.RecordsProcessed,
        bl.RecordsInserted,
        bl.RecordsUpdated,
        bl.RecordsDeleted,
        bl.ErrorCount
    FROM ETL_BatchLog bl
    WHERE bl.BatchID = @BatchID
    
    UNION ALL
    
    SELECT 
        'Step Totals (Success Only)' AS Source,
        SUM(CASE WHEN StepStatus = 'Success' THEN RecordsProcessed ELSE 0 END),
        SUM(CASE WHEN StepStatus = 'Success' THEN RecordsInserted ELSE 0 END),
        SUM(CASE WHEN StepStatus = 'Success' THEN RecordsUpdated ELSE 0 END),
        SUM(CASE WHEN StepStatus = 'Success' THEN RecordsDeleted ELSE 0 END),
        SUM(CASE WHEN StepStatus = 'Failed' THEN 1 ELSE 0 END)
    FROM ETL_StepLog
    WHERE BatchID = @BatchID;
    
    -- Show individual step details
    PRINT '';
    PRINT 'Individual Step Details:';
    SELECT 
        StepName,
        StepStatus,
        RetryAttempt,
        RecordsProcessed,
        RecordsInserted,
        RecordsUpdated,
        RecordsDeleted,
        ExecutionTimeSeconds,
        ErrorMessage
    FROM ETL_StepLog
    WHERE BatchID = @BatchID
    ORDER BY StepLogID;
END;
GO

-- =============================================
-- ENHANCEMENT: Monitoring Views
-- =============================================

CREATE VIEW vw_ETL_BatchSummary
AS
SELECT 
    p.ProcessName,
    bl.BatchID,
    bl.BatchStartTime,
    bl.BatchEndTime,
    bl.ExecutionTimeSeconds,
    bl.BatchStatus,
    bl.RecordsProcessed,
    bl.ErrorCount,
    CASE 
        WHEN bl.ErrorCount > 0 THEN 'Review Required'
        WHEN bl.ExecutionTimeSeconds > 7200 THEN 'Performance Concern'
        WHEN bl.BatchStatus = 'Running' AND DATEDIFF(MINUTE, bl.BatchStartTime, GETDATE()) > 60 THEN 'Long Running'
        ELSE 'Normal'
    END AS HealthStatus,
    bl.ServerName,
    bl.CreatedBy
FROM ETL_BatchLog bl
INNER JOIN ETL_Process p ON bl.ProcessID = p.ProcessID
WHERE bl.BatchStartTime >= DATEADD(DAY, -7, GETDATE());
GO

CREATE VIEW vw_ETL_ErrorSummary
AS
SELECT 
    p.ProcessName,
    el.ErrorSeverity,
    COUNT(*) AS ErrorCount,
    COUNT(DISTINCT el.ErrorMessage) AS UniqueErrorCount,
    MAX(el.ErrorDateTime) AS LastErrorDate,
    SUM(CASE WHEN el.IsResolved = 0 THEN 1 ELSE 0 END) AS UnresolvedCount
FROM ETL_ErrorLog el
INNER JOIN ETL_BatchLog bl ON el.BatchID = bl.BatchID
INNER JOIN ETL_Process p ON bl.ProcessID = p.ProcessID
WHERE el.ErrorDateTime >= DATEADD(DAY, -30, GETDATE())
GROUP BY p.ProcessName, el.ErrorSeverity;
GO

CREATE VIEW vw_ETL_CurrentConfiguration
AS
SELECT 
    ConfigKey,
    ConfigValue,
    ConfigDataType,
    ConfigDescription,
    IsActive,
    UpdatedDate
FROM ETL_Configuration
WHERE IsActive = 1;
GO

-- =============================================
-- Create Indexes for Performance
-- =============================================

CREATE INDEX IX_ETL_BatchLog_ProcessID ON ETL_BatchLog(ProcessID);
CREATE INDEX IX_ETL_BatchLog_BatchStartTime ON ETL_BatchLog(BatchStartTime);
CREATE INDEX IX_ETL_BatchLog_BatchStatus ON ETL_BatchLog(BatchStatus);

CREATE INDEX IX_ETL_StepLog_BatchID ON ETL_StepLog(BatchID);
CREATE INDEX IX_ETL_StepLog_StepStartTime ON ETL_StepLog(StepStartTime);
CREATE INDEX IX_ETL_StepLog_StepStatus ON ETL_StepLog(StepStatus);

CREATE INDEX IX_ETL_ErrorLog_BatchID ON ETL_ErrorLog(BatchID);
CREATE INDEX IX_ETL_ErrorLog_ErrorDateTime ON ETL_ErrorLog(ErrorDateTime);
CREATE INDEX IX_ETL_ErrorLog_IsResolved ON ETL_ErrorLog(IsResolved);
CREATE INDEX IX_ETL_ErrorLog_ErrorSeverity ON ETL_ErrorLog(ErrorSeverity); -- ENHANCEMENT

CREATE INDEX IX_ETL_Watermark_ProcessID ON ETL_Watermark(ProcessID);
CREATE INDEX IX_ETL_Watermark_TableName ON ETL_Watermark(TableName);

CREATE INDEX IX_ETL_AlertLog_IsSent ON ETL_AlertLog(IsSent) WHERE IsSent = 0; -- ENHANCEMENT

PRINT '';
PRINT '========================================';
PRINT 'ETL Control Framework V3.2 created successfully!';
PRINT 'ENHANCEMENTS:';
PRINT '  ✓ Configuration table with helper functions';
PRINT '  ✓ Retry logic with configurable attempts';
PRINT '  ✓ Enhanced error logging with SQL details';
PRINT '  ✓ Alert management framework';
PRINT '  ✓ Monitoring views for dashboards';
PRINT '  ✓ Performance optimizations';
PRINT '  ✓ Step-level retry tracking';
PRINT '  ✓ usp_StartStep and usp_EndStep procedures';
PRINT '  ✓ Compatible with all staging procedures';
PRINT '========================================';
GO

-- =============================================
-- Initialize Watermarks for All 9 Tables
-- FIXED: Use individual process names instead of generic 'Load_Staging'
-- =============================================

PRINT 'Initializing watermarks for individual staging processes...';

-- Watermarks for Lookup Tables
INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Category', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Category';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Structure', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Structure';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Activity', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Activity';

-- Watermarks for Core Tables
INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Taxpayer', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Taxpayer';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Owner', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Owner';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Officer', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Officer';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'MonthlyDeclaration', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_MonthlyDeclaration';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'AnnualDeclaration', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_AnnualDeclaration';

INSERT INTO ETL_Watermark (ProcessID, TableName, ColumnName, LastValue, LastLoadDate)
SELECT ProcessID, 'Payment', 'UpdatedDate', NULL, GETDATE()
FROM ETL_Process WHERE ProcessName = 'Load_STG_Payment';

PRINT '';
PRINT 'Watermarks initialized for all 9 tables with individual process names';
PRINT 'Total: 9 tables ready for ETL processing';
PRINT '';
PRINT 'Verification:';
SELECT 
    p.ProcessName,
    w.TableName,
    w.ColumnName,
    w.IsActive
FROM ETL_Watermark w
JOIN ETL_Process p ON w.ProcessID = p.ProcessID
WHERE p.ProcessName LIKE 'Load_STG_%'
ORDER BY p.ProcessName;
GO