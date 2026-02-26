-- =============================================
-- 05_ETL_Staging_Procedures.sql
-- Staging Layer Helper Procedures
-- Aligned with DV2_Complete_Architecture_Guide.docx
--
-- NOTE: Staging packages use Data Flow Tasks (DFTs) with
-- OLE DB Source -> DRV_AddMetadata -> OLE DB Destination.
-- No stored procedures are needed for data movement.
-- This file provides helper SPs for truncation and validation.
-- =============================================

USE DV_Staging;
GO

-- =============================================
-- usp_Truncate_STG_Table
-- Called by: SQL_TruncateStaging (Execute SQL Task) in each staging child package
-- Full Load: TRUNCATE before DFT; Incremental: skip truncation
-- =============================================

CREATE OR ALTER PROCEDURE dbo.usp_Truncate_STG_Table
    @TableName VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(200) = N'TRUNCATE TABLE dbo.' + QUOTENAME(@TableName);

    EXEC sp_executesql @SQL;

    PRINT 'Truncated: ' + @TableName;
END;
GO

-- =============================================
-- usp_GetStagingRowCount
-- Utility: verify row counts after DFT loads
-- =============================================

CREATE OR ALTER PROCEDURE dbo.usp_GetStagingRowCount
    @TableName VARCHAR(100),
    @RowCount  INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(200) = N'SELECT @cnt = COUNT(*) FROM dbo.' + QUOTENAME(@TableName);
    EXEC sp_executesql @SQL, N'@cnt INT OUTPUT', @cnt = @RowCount OUTPUT;
END;
GO

-- =============================================
-- usp_ValidateStagingData
-- Optional: run basic data quality checks post-load
-- =============================================

CREATE OR ALTER PROCEDURE dbo.usp_ValidateStagingData
    @BatchID INT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TotalRows INT = 0;

    SELECT @TotalRows = @TotalRows + cnt FROM (
        SELECT COUNT(*) AS cnt FROM STG_Category        UNION ALL
        SELECT COUNT(*) FROM STG_Structure               UNION ALL
        SELECT COUNT(*) FROM STG_Activity                UNION ALL
        SELECT COUNT(*) FROM STG_Taxpayer                UNION ALL
        SELECT COUNT(*) FROM STG_Owner                   UNION ALL
        SELECT COUNT(*) FROM STG_Officer                 UNION ALL
        SELECT COUNT(*) FROM STG_MonthlyDeclaration      UNION ALL
        SELECT COUNT(*) FROM STG_AnnualDeclaration       UNION ALL
        SELECT COUNT(*) FROM STG_Payment
    ) t;

    PRINT 'Staging validation for Batch ' + CAST(@BatchID AS VARCHAR(10))
        + ': Total rows across 9 tables = ' + CAST(@TotalRows AS VARCHAR(20));

    -- Return summary
    SELECT 'STG_Category'           AS TableName, COUNT(*) AS [RowCount] FROM STG_Category        UNION ALL
    SELECT 'STG_Structure',                        COUNT(*) FROM STG_Structure               UNION ALL
    SELECT 'STG_Activity',                         COUNT(*) FROM STG_Activity                UNION ALL
    SELECT 'STG_Taxpayer',                         COUNT(*) FROM STG_Taxpayer                UNION ALL
    SELECT 'STG_Owner',                            COUNT(*) FROM STG_Owner                   UNION ALL
    SELECT 'STG_Officer',                          COUNT(*) FROM STG_Officer                 UNION ALL
    SELECT 'STG_MonthlyDeclaration',               COUNT(*) FROM STG_MonthlyDeclaration      UNION ALL
    SELECT 'STG_AnnualDeclaration',                COUNT(*) FROM STG_AnnualDeclaration       UNION ALL
    SELECT 'STG_Payment',                          COUNT(*) FROM STG_Payment;
END;
GO

PRINT '=== DV_Staging: Helper procedures created ===';
PRINT 'Note: Staging data movement is handled by SSIS Data Flow Tasks (DFTs)';
PRINT 'SPs: usp_Truncate_STG_Table, usp_GetStagingRowCount, usp_ValidateStagingData';
GO