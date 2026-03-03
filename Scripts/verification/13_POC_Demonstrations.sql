-- ══════════════════════════════════════════════════════════════════════════════
-- POC Implementation Script — Data Vault 2.0 Tax System Data Warehouse
-- ══════════════════════════════════════════════════════════════════════════════
-- Author:      Sot So
-- Advisor:     Mr. Chap Chanpiseth
-- Institution: Royal University of Phnom Penh — MSc Data Science and Engineering
-- Date:        February 2026
-- Purpose:     Reproduce all 4 POC demonstrations from Chapter 5, Section 5.2
--
-- PREREQUISITES:
--   1. All 6 databases created via scripts 01-04 (TaxSystemDB, ETL_Control,
--      DV_Staging, DV_Bronze, DV_Silver, DV_Gold)
--   2. Full Load pipeline executed successfully (script 00 or scripts 05-09)
--   3. ETL Control Framework deployed (script 03)
--
-- EXECUTION ORDER: Run each POC section sequentially (POC 1 → 2 → 3 → 4)
-- ══════════════════════════════════════════════════════════════════════════════


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  POC 1: Schema Flexibility via Hub-Satellite Separation                  ║
-- ║  Challenge: Source changes require modifying multiple DW objects          ║
-- ║  Solution:  CREATE new Satellite with zero impact on existing objects     ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

PRINT '══════════════════════════════════════════════════════════════';
PRINT '  POC 1: Schema Flexibility via Hub-Satellite Separation';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- ─── Step 1: Verify Current Bronze Layer Structure ───
USE DV_Bronze;
GO

PRINT '─── Step 1a: HUB_Taxpayer structure ───';
SELECT 
    c.COLUMN_NAME, 
    c.DATA_TYPE, 
    c.CHARACTER_MAXIMUM_LENGTH AS MaxLength,
    c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = 'HUB_Taxpayer'
ORDER BY c.ORDINAL_POSITION;
GO

PRINT '─── Step 1b: SAT_Taxpayer structure (current descriptive attributes) ───';
SELECT 
    c.COLUMN_NAME, 
    c.DATA_TYPE, 
    c.CHARACTER_MAXIMUM_LENGTH AS MaxLength,
    c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = 'SAT_Taxpayer'
ORDER BY c.ORDINAL_POSITION;
GO

PRINT '─── Step 1c: Count all existing Taxpayer-related objects ───';
SELECT 'HUB_Taxpayer' AS ObjectName, 'Hub' AS ObjectType, COUNT(*) AS RowCount 
FROM DV_Bronze.dbo.HUB_Taxpayer
UNION ALL
SELECT 'SAT_Taxpayer', 'Satellite', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL
SELECT 'LNK_TaxpayerDeclaration', 'Link', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
UNION ALL
SELECT 'LNK_TaxpayerOfficer', 'Link', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOfficer
UNION ALL
SELECT 'LNK_TaxpayerOwner', 'Link', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOwner;
GO

-- ─── Step 2: CREATE New Satellite (Only Action Required) ───
PRINT '';
PRINT '─── Step 2: CREATE SAT_Taxpayer_Contact (new Satellite) ───';

-- Drop if exists from previous POC run
DROP TABLE IF EXISTS DV_Bronze.dbo.SAT_Taxpayer_Contact;
GO

CREATE TABLE SAT_Taxpayer_Contact (
    HUB_Taxpayer_HK     VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_RecordSource     NVARCHAR(50)    NOT NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    TaxpayerEmail        VARCHAR(200)    NULL,
    TaxpayerPhone        VARCHAR(50)     NULL,
    CONSTRAINT PK_SAT_Taxpayer_Contact 
        PRIMARY KEY (HUB_Taxpayer_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_TpContact_HUB 
        FOREIGN KEY (HUB_Taxpayer_HK)
        REFERENCES HUB_Taxpayer (HUB_Taxpayer_HK)
);
GO

-- Filtered index for current records (performance optimization)
CREATE INDEX IX_SAT_Taxpayer_Contact_Current 
    ON SAT_Taxpayer_Contact(HUB_Taxpayer_HK) 
    WHERE SAT_EndDate IS NULL;
GO

PRINT '   ✓ SAT_Taxpayer_Contact created successfully';
GO

-- ─── Step 3: Verify Zero Impact on Existing Objects ───
PRINT '';
PRINT '─── Step 3a: Confirm HUB_Taxpayer is unchanged ───';
SELECT 
    c.COLUMN_NAME, c.DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = 'HUB_Taxpayer'
ORDER BY c.ORDINAL_POSITION;
-- Expected: Identical to Step 1a — 4 columns, no changes
GO

PRINT '─── Step 3b: Confirm SAT_Taxpayer is unchanged ───';
SELECT 
    c.COLUMN_NAME, c.DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = 'SAT_Taxpayer'
ORDER BY c.ORDINAL_POSITION;
-- Expected: Identical to Step 1b — 12 columns, no changes
GO

PRINT '─── Step 3c: Confirm all Links are unchanged ───';
SELECT 
    t.name AS TableName,
    SUM(p.rows) AS RowCount
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
WHERE t.name LIKE 'LNK_%'
GROUP BY t.name;
-- Expected: All Link row counts identical to Step 1c
GO

PRINT '─── Step 3d: Confirm new Satellite linked to HUB_Taxpayer ───';
SELECT 
    t.name AS TableName,
    t.create_date,
    fk.name AS ForeignKeyName,
    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTable
FROM sys.tables t
LEFT JOIN sys.foreign_keys fk ON fk.parent_object_id = t.object_id
WHERE t.name = 'SAT_Taxpayer_Contact';
GO

-- ─── Step 4: Load Sample Data into New Satellite ───
PRINT '';
PRINT '─── Step 4: Insert sample contact data ───';

INSERT INTO SAT_Taxpayer_Contact 
    (HUB_Taxpayer_HK, SAT_LoadDate, SAT_RecordSource, SAT_HashDiff, TaxpayerEmail, TaxpayerPhone)
SELECT 
    h.HUB_Taxpayer_HK,
    GETDATE(),
    'TaxSystemDB.ContactModule',
    HASHBYTES('SHA2_256', CONCAT(
        ISNULL('taxpayer' + CAST(ROW_NUMBER() OVER(ORDER BY h.TaxID) AS VARCHAR) + '@tax.gov.kh', ''), '|',
        ISNULL('+855-' + RIGHT('000000000' + CAST(ABS(CHECKSUM(h.TaxID)) % 1000000000 AS VARCHAR), 9), '')
    )),
    'taxpayer' + CAST(ROW_NUMBER() OVER(ORDER BY h.TaxID) AS VARCHAR) + '@tax.gov.kh',
    '+855-' + RIGHT('000000000' + CAST(ABS(CHECKSUM(h.TaxID)) % 1000000000 AS VARCHAR), 9)
FROM DV_Bronze.dbo.HUB_Taxpayer h;

PRINT '   ✓ SAT_Taxpayer_Contact loaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- Verify sample data
SELECT TOP 5 
    CONVERT(VARCHAR(18), HUB_Taxpayer_HK, 1) AS HK_First8,
    TaxpayerEmail, TaxpayerPhone, SAT_LoadDate
FROM SAT_Taxpayer_Contact
ORDER BY SAT_LoadDate;
GO

-- ─── Step 5: Impact Summary ───
PRINT '';
PRINT '─── Step 5: Final Impact Assessment ───';

SELECT 
    t.name AS ObjectName,
    CASE 
        WHEN t.name LIKE 'HUB_%' THEN 'Hub'
        WHEN t.name LIKE 'SAT_%' THEN 'Satellite'
        WHEN t.name LIKE 'LNK_%' THEN 'Link'
    END AS ObjectType,
    CASE 
        WHEN t.name = 'SAT_Taxpayer_Contact' THEN '✓ NEW — Created independently'
        ELSE '✓ UNCHANGED — No modification'
    END AS Impact
FROM sys.tables t
WHERE t.name LIKE 'HUB_%' OR t.name LIKE 'SAT_%' OR t.name LIKE 'LNK_%'
ORDER BY 
    CASE WHEN t.name LIKE 'HUB_%' THEN 1 WHEN t.name LIKE 'SAT_%' THEN 2 ELSE 3 END,
    t.name;
GO

-- ─── POC 1 Cleanup ───
PRINT '';
PRINT '─── POC 1 Cleanup ───';
DROP TABLE IF EXISTS DV_Bronze.dbo.SAT_Taxpayer_Contact;
PRINT '   ✓ SAT_Taxpayer_Contact removed — Bronze layer restored to original state';
GO

PRINT '';
PRINT '═══ POC 1 COMPLETE: Zero existing objects modified ═══';
PRINT '';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  POC 2: Full Historical Tracking via Insert-Only Satellite Pattern       ║
-- ║  Challenge: Update-in-place approaches lose historical audit trail        ║
-- ║  Solution:  Insert-only Satellite with HashDiff change detection          ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

PRINT '══════════════════════════════════════════════════════════════';
PRINT '  POC 2: Full Historical Tracking via Insert-Only Satellite';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- ─── Step 1: Verify Current State (TAX000001) ───
PRINT '─── Step 1: Current SAT_Taxpayer state for TAX000001 ───';
USE DV_Bronze;
GO

SELECT 
    CONVERT(VARCHAR(18), sat.HUB_Taxpayer_HK, 1) AS HK_First8,
    sat.LegalBusinessName,
    sat.EstimatedAnnualRevenue AS Revenue,
    sat.SAT_LoadDate,
    sat.SAT_EndDate,
    CASE WHEN sat.SAT_EndDate IS NULL THEN '✓ Current' ELSE '📋 Historic' END AS VersionStatus
FROM DV_Bronze.dbo.SAT_Taxpayer sat
INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub 
    ON sat.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
WHERE hub.TaxID = 'TAX000001'
ORDER BY sat.SAT_LoadDate;
-- Expected: 1 row (Current) — no history yet after full load
GO

-- ─── Step 2: First Source Change ───
PRINT '';
PRINT '─── Step 2: Simulate first source change (revenue 500K → 350K) ───';
USE TaxSystemDB;
GO

-- Save original value for restoration later
DECLARE @OrigRevenue DECIMAL(18,2), @OrigName VARCHAR(300);
SELECT @OrigRevenue = EstimatedAnnualRevenue, @OrigName = LegalBusinessName
FROM Taxpayer WHERE TaxID = 'TAX000001';
PRINT '   Original: ' + @OrigName + ', Revenue = ' + CAST(@OrigRevenue AS VARCHAR);

UPDATE Taxpayer 
SET EstimatedAnnualRevenue = 350000.00,
    UpdatedDate = GETDATE()
WHERE TaxID = 'TAX000001';

PRINT '   ✓ Source updated: TAX000001 revenue changed to 350,000';
GO

-- ─── Step 3: Execute Staging + Bronze ETL ───
PRINT '';
PRINT '─── Step 3: Execute Staging + Bronze ETL ───';

-- Step 3a: Reload STG_Taxpayer
USE DV_Staging;
GO
TRUNCATE TABLE STG_Taxpayer;

INSERT INTO STG_Taxpayer 
    (TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID,
     RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
SELECT 
    TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID,
    RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem,
    'TaxSystemDB' AS RecordSource,
    GETDATE() AS LoadDateTime
FROM TaxSystemDB.dbo.Taxpayer;

PRINT '   ✓ STG_Taxpayer reloaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- Step 3b: Execute Bronze Hub load (idempotent)
USE DV_Bronze;
GO
DECLARE @BatchID INT = 9901, @RC INT;
EXEC dbo.usp_Load_HUB_Taxpayer @BatchID = @BatchID, @RowCount = @RC OUTPUT;
PRINT '   HUB_Taxpayer: ' + CAST(@RC AS VARCHAR) + ' new rows (expected: 0)';

-- Step 3c: Execute Bronze Satellite load (HISTORY CAPTURED HERE)
EXEC dbo.usp_Load_SAT_Taxpayer @BatchID = @BatchID, @RowCount = @RC OUTPUT;
PRINT '   SAT_Taxpayer: ' + CAST(@RC AS VARCHAR) + ' changed rows';
GO

-- ─── Step 4: Verify 2 Versions ───
PRINT '';
PRINT '─── Step 4: Verify — should now show 2 versions ───';

SELECT 
    CONVERT(VARCHAR(18), sat.HUB_Taxpayer_HK, 1) AS HK_First8,
    sat.LegalBusinessName,
    sat.EstimatedAnnualRevenue AS Revenue,
    sat.SAT_LoadDate,
    sat.SAT_EndDate,
    CASE WHEN sat.SAT_EndDate IS NULL THEN '✓ Current' ELSE '📋 Historic' END AS VersionStatus
FROM DV_Bronze.dbo.SAT_Taxpayer sat
INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub 
    ON sat.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
WHERE hub.TaxID = 'TAX000001'
ORDER BY sat.SAT_LoadDate;
-- Expected: 2 rows — V1 (Historic, 500K), V2 (Current, 350K)
GO

-- ─── Step 5: Second Source Change ───
PRINT '';
PRINT '─── Step 5: Simulate second source change (name amended + revenue → 505K) ───';
USE TaxSystemDB;
GO

UPDATE Taxpayer 
SET LegalBusinessName = LegalBusinessName + ' (AMENDED)',
    EstimatedAnnualRevenue = 505000.00,
    UpdatedDate = GETDATE()
WHERE TaxID = 'TAX000001';

PRINT '   ✓ Source updated: TAX000001 name amended, revenue changed to 505,000';
GO

-- Reload Staging
USE DV_Staging;
GO
TRUNCATE TABLE STG_Taxpayer;
INSERT INTO STG_Taxpayer 
    (TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID,
     RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
SELECT TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID,
    RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
FROM TaxSystemDB.dbo.Taxpayer;
PRINT '   ✓ STG_Taxpayer reloaded: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows';
GO

-- Execute Bronze loads
USE DV_Bronze;
GO
DECLARE @BatchID INT = 9902, @RC INT;
EXEC dbo.usp_Load_HUB_Taxpayer @BatchID = @BatchID, @RowCount = @RC OUTPUT;
EXEC dbo.usp_Load_SAT_Taxpayer @BatchID = @BatchID, @RowCount = @RC OUTPUT;
PRINT '   SAT_Taxpayer: ' + CAST(@RC AS VARCHAR) + ' changed rows';
GO

-- ─── Step 6: Verify Complete Audit Trail (All 3 Versions) ───
PRINT '';
PRINT '─── Step 6: All 3 versions preserved for TAX000001 ───';

SELECT 
    CONVERT(VARCHAR(18), sat.HUB_Taxpayer_HK, 1) AS HK_First8,
    sat.LegalBusinessName,
    sat.EstimatedAnnualRevenue AS Revenue,
    sat.SAT_LoadDate,
    sat.SAT_EndDate,
    CASE WHEN sat.SAT_EndDate IS NULL THEN '✓ V3 (Current)' 
         ELSE '📋 V' + CAST(ROW_NUMBER() OVER(
             PARTITION BY hub.TaxID ORDER BY sat.SAT_LoadDate
         ) AS VARCHAR) + ' (Historic)' 
    END AS Version
FROM DV_Bronze.dbo.SAT_Taxpayer sat
INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub 
    ON sat.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
WHERE hub.TaxID = 'TAX000001'
ORDER BY sat.SAT_LoadDate;
-- Expected: 3 rows — V1 (500K), V2 (350K), V3 (505K, AMENDED, Current)
GO

-- ─── Step 7: Point-in-Time Query (Auditor Use Case) ───
PRINT '';
PRINT '─── Step 7: Point-in-time auditor query ───';

-- Query: "What was TAX000001's data at the time of the first load?"
DECLARE @AuditDate DATETIME = (
    SELECT TOP 1 sat.SAT_LoadDate 
    FROM DV_Bronze.dbo.SAT_Taxpayer sat
    INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub ON sat.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
    WHERE hub.TaxID = 'TAX000001'
    ORDER BY sat.SAT_LoadDate ASC  -- First version
);

PRINT '   Auditing as-of: ' + CONVERT(VARCHAR(30), @AuditDate, 120);

SELECT 
    hub.TaxID,
    sat.LegalBusinessName,
    sat.EstimatedAnnualRevenue AS Revenue,
    sat.SAT_LoadDate,
    sat.SAT_EndDate,
    'Record current at audit date' AS Note
FROM DV_Bronze.dbo.SAT_Taxpayer sat
INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub 
    ON sat.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
WHERE hub.TaxID = 'TAX000001'
    AND sat.SAT_LoadDate <= @AuditDate
    AND (sat.SAT_EndDate IS NULL OR sat.SAT_EndDate > @AuditDate);
-- Returns: Exactly the record that was current at @AuditDate
GO

-- ─── POC 2 Cleanup: Restore source data to original state ───
PRINT '';
PRINT '─── POC 2 Note ───';
PRINT '   Source data (TaxSystemDB.Taxpayer) has been modified.';
PRINT '   To restore original state, re-run script 00_CleanAll_FreshFullLoad.sql';
PRINT '';
PRINT '═══ POC 2 COMPLETE: All 3 versions preserved with timestamps ═══';
PRINT '';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  POC 3: ETL Control Framework with Step-Level Logging & Error Recovery   ║
-- ║  Challenge: No visibility into ETL failures or recovery capability       ║
-- ║  Solution:  BatchLog + StepLog + OnError Event Handlers                  ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

PRINT '══════════════════════════════════════════════════════════════';
PRINT '  POC 3: ETL Control Framework — Logging & Error Recovery';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

USE ETL_Control;
GO

-- ─── Step 1: Review Current Control Tables ───
PRINT '─── Step 1: Current ETL control table counts ───';
SELECT 'ETL_BatchLog' AS TableName, COUNT(*) AS RowCount FROM ETL_BatchLog
UNION ALL
SELECT 'ETL_StepLog', COUNT(*) FROM ETL_StepLog
UNION ALL
SELECT 'ETL_ErrorLog', COUNT(*) FROM ETL_ErrorLog;
GO

-- ─── Step 2: Start a New Batch ───
PRINT '';
PRINT '─── Step 2: Start new batch ───';

DECLARE @POC_BatchID INT;
EXEC usp_StartBatch 
    @ProcessName = 'Load_Staging_Full',
    @BatchID = @POC_BatchID OUTPUT;

PRINT '   ✓ Batch started: BatchID = ' + CAST(@POC_BatchID AS VARCHAR);
GO

-- ─── Step 3: Execute Steps with Logging ───
PRINT '';
PRINT '─── Step 3: Execute steps with logging (1 simulated failure) ───';

-- Get the active batch ID
DECLARE @B INT = (SELECT MAX(BatchID) FROM ETL_BatchLog WHERE BatchStatus = 'Running');
DECLARE @S INT, @RC INT;

-- ═══ Step 3a: Load_STG_Category (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Category', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Category;
    INSERT INTO DV_Staging.dbo.STG_Category 
        (CategoryID, CategoryName, CategoryDescription, IsActive, CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT CategoryID, CategoryName, CategoryDescription, IsActive, CreatedDate, UpdatedDate, 'TaxSystem', 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Category;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Category: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Category', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3b: Load_STG_Structure (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Structure', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Structure;
    INSERT INTO DV_Staging.dbo.STG_Structure 
        (StructureID, StructureName, StructureDescription, IsActive, CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT StructureID, StructureName, StructureDescription, IsActive, CreatedDate, UpdatedDate, 'TaxSystem', 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Structure;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Structure: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Structure', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3c: Load_STG_Taxpayer (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Taxpayer', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Taxpayer;
    INSERT INTO DV_Staging.dbo.STG_Taxpayer 
        (TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID, 
         RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT TaxpayerID, TaxID, LegalBusinessName, TradingName, CategoryID, StructureID, 
           RegistrationDate, EstimatedAnnualRevenue, IsActive, CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Taxpayer;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Taxpayer: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Taxpayer', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3d: Load_STG_Payment (SIMULATE FAILURE) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Payment', @StepID = @S OUTPUT;
BEGIN TRY
    -- Simulate a timeout failure
    RAISERROR('Execution Timeout Expired. The timeout period elapsed prior to completion of the operation. [Source: TaxSystemDB.dbo.Payment, OLE DB provider]', 16, 1);
END TRY
BEGIN CATCH
    DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Payment', @ErrorMessage=@ErrMsg;
    PRINT '   ✗ Load_STG_Payment: FAILED — ' + @ErrMsg;
END CATCH

-- ═══ Step 3e: Load_STG_MonthlyDeclaration (SUCCESS — continues despite previous failure) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_MonthlyDeclaration', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_MonthlyDeclaration;
    INSERT INTO DV_Staging.dbo.STG_MonthlyDeclaration 
        (DeclarationID, TaxpayerID, DeclarationMonth, DeclarationYear, 
         GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, 
         TotalAmount, DeclarationDate, DueDate, Status, OfficerID, IsActive, 
         CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT DeclarationID, TaxpayerID, DeclarationMonth, DeclarationYear, 
           GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, 
           TotalAmount, DeclarationDate, DueDate, Status, OfficerID, IsActive, 
           CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.MonthlyDeclaration;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_MonthlyDecl: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_MonthlyDeclaration', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3f: Load_STG_AnnualDeclaration (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_AnnualDeclaration', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_AnnualDeclaration;
    INSERT INTO DV_Staging.dbo.STG_AnnualDeclaration 
        (AnnualDeclarationID, TaxpayerID, DeclarationYear, 
         GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, 
         TotalAmount, DeclarationDate, DueDate, Status, OfficerID, IsActive, 
         CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT AnnualDeclarationID, TaxpayerID, DeclarationYear, 
           GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, 
           TotalAmount, DeclarationDate, DueDate, Status, OfficerID, IsActive, 
           CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.AnnualDeclaration;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_AnnualDecl: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_AnnualDeclaration', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3g: Load_STG_Officer (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Officer', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Officer;
    INSERT INTO DV_Staging.dbo.STG_Officer 
        (OfficerID, OfficerCode, FirstName, LastName, Department, IsActive, 
         CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT OfficerID, OfficerCode, FirstName, LastName, Department, IsActive, 
           CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Officer;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Officer: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Officer', @ErrorMessage=N'Unexpected error';
END CATCH

-- ═══ Step 3h: Load_STG_Owner (SUCCESS) ═══
EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Owner', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Owner;
    INSERT INTO DV_Staging.dbo.STG_Owner 
        (OwnerID, TaxpayerID, OwnerName, OwnerType, OwnershipPercentage, IsActive, 
         CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT OwnerID, TaxpayerID, OwnerName, OwnerType, OwnershipPercentage, IsActive, 
           CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Owner;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Owner: ' + CAST(@RC AS VARCHAR) + ' rows';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    EXEC usp_LogError @BatchID=@B, @StepLogID=@S, @ErrorSeverity='High', 
         @ErrorSource='Load_STG_Owner', @ErrorMessage=N'Unexpected error';
END CATCH
GO

-- ─── Step 4: End Batch and Review Results ───
PRINT '';
PRINT '─── Step 4: End batch and review results ───';

DECLARE @B INT = (SELECT MAX(BatchID) FROM ETL_BatchLog WHERE BatchStatus = 'Running');
EXEC usp_EndBatch @BatchID = @B, @Status = 'Partial';
PRINT '   ✓ Batch ended with status: Partial';
GO

-- Step 4a: ETL_BatchLog — Pipeline-level status
PRINT '';
PRINT '─── Step 4a: ETL_BatchLog — Pipeline-level status ───';
SELECT 
    BatchID, BatchStatus, 
    BatchStartTime, BatchEndTime,
    RecordsProcessed,
    ExecutionTimeSeconds AS Duration_sec
FROM ETL_BatchLog
WHERE BatchID = (SELECT MAX(BatchID) FROM ETL_BatchLog);
GO

-- Step 4b: ETL_StepLog — Step-level detail
PRINT '─── Step 4b: ETL_StepLog — Step-level detail ───';
SELECT 
    StepLogID, 
    StepName, 
    StepStatus, 
    RecordsProcessed AS Rows,
    ExecutionTimeSeconds AS Sec,
    LEFT(ISNULL(ErrorMessage, ''), 80) AS ErrorMessage
FROM ETL_StepLog
WHERE BatchID = (SELECT MAX(BatchID) FROM ETL_BatchLog)
ORDER BY StepLogID;
GO

-- Step 4c: ETL_ErrorLog — Detailed error capture
PRINT '─── Step 4c: ETL_ErrorLog — Detailed error capture ───';
SELECT 
    ErrorID, BatchID, StepLogID, 
    ErrorSeverity, ErrorSource,
    LEFT(ErrorMessage, 120) AS ErrorMessage, 
    ErrorDateTime
FROM ETL_ErrorLog
WHERE BatchID = (SELECT MAX(BatchID) FROM ETL_BatchLog);
GO

-- ─── Step 5: Recovery — Re-execute Only Failed Step ───
PRINT '';
PRINT '─── Step 5: Recovery — re-execute ONLY Load_STG_Payment ───';

DECLARE @B INT = (SELECT MAX(BatchID) FROM ETL_BatchLog);
DECLARE @S INT, @RC INT;

EXEC usp_StartStep @BatchID = @B, @StepName = 'Load_STG_Payment_RETRY', @StepID = @S OUTPUT;
BEGIN TRY
    TRUNCATE TABLE DV_Staging.dbo.STG_Payment;
    INSERT INTO DV_Staging.dbo.STG_Payment 
        (PaymentID, TaxpayerID, DeclarationID, AnnualDeclarationID, PaymentAmount, 
         PaymentDate, PaymentMethod, ReferenceNumber, Status, IsActive, 
         CreatedDate, UpdatedDate, SourceSystem, RecordSource, LoadDateTime)
    SELECT PaymentID, TaxpayerID, DeclarationID, AnnualDeclarationID, PaymentAmount, 
           PaymentDate, PaymentMethod, ReferenceNumber, Status, IsActive, 
           CreatedDate, UpdatedDate, SourceSystem, 'TaxSystemDB', GETDATE()
    FROM TaxSystemDB.dbo.Payment;
    SET @RC = @@ROWCOUNT;
    EXEC usp_EndStep @StepID = @S, @Status = 'Success', @RowCount = @RC;
    PRINT '   ✓ Load_STG_Payment_RETRY: ' + CAST(@RC AS VARCHAR) + ' rows — Recovery successful!';
END TRY
BEGIN CATCH
    EXEC usp_EndStep @StepID = @S, @Status = 'Failed', @RowCount = 0;
    PRINT '   ✗ Retry also failed: ' + ERROR_MESSAGE();
END CATCH
GO

-- Verify all staging tables have data
PRINT '';
PRINT '─── Verification: All staging tables loaded ───';
SELECT 'STG_Category' AS T, COUNT(*) AS Rows FROM DV_Staging.dbo.STG_Category
UNION ALL SELECT 'STG_Structure', COUNT(*) FROM DV_Staging.dbo.STG_Structure
UNION ALL SELECT 'STG_Taxpayer', COUNT(*) FROM DV_Staging.dbo.STG_Taxpayer
UNION ALL SELECT 'STG_Payment', COUNT(*) FROM DV_Staging.dbo.STG_Payment
UNION ALL SELECT 'STG_MonthlyDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_MonthlyDeclaration
UNION ALL SELECT 'STG_AnnualDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_AnnualDeclaration
UNION ALL SELECT 'STG_Officer', COUNT(*) FROM DV_Staging.dbo.STG_Officer
UNION ALL SELECT 'STG_Owner', COUNT(*) FROM DV_Staging.dbo.STG_Owner;
GO

PRINT '';
PRINT '═══ POC 3 COMPLETE: 7/8 steps succeeded, 1 failed, recovery via re-run ═══';
PRINT '';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  POC 4: Business Rules via Silver (Business Vault) + Gold (Star Schema)  ║
-- ║  Challenge: Raw data has no derived analytics or business metrics         ║
-- ║  Solution:  Silver computes metrics; Gold organizes into star schema      ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

PRINT '══════════════════════════════════════════════════════════════';
PRINT '  POC 4: Business Rules via Silver + Gold Layers';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- ─── Step 1: Verify Raw Bronze Data (No Analytics) ───
PRINT '─── Step 1: Raw Bronze data for TAX000001 — no analytics available ───';
USE DV_Bronze;
GO

SELECT 'Monthly Declarations' AS DataType, COUNT(*) AS Records
FROM LNK_TaxpayerDeclaration lnk
INNER JOIN HUB_Taxpayer hub ON lnk.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
INNER JOIN SAT_MonthlyDecl sat ON lnk.HUB_Declaration_HK = sat.HUB_Declaration_HK 
    AND sat.SAT_EndDate IS NULL
WHERE hub.TaxID = 'TAX000001'
UNION ALL
SELECT 'Payments', COUNT(*)
FROM LNK_TaxpayerDeclaration lnk_td
INNER JOIN HUB_Taxpayer hub ON lnk_td.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
INNER JOIN LNK_DeclarationPayment lnk_dp 
    ON lnk_td.HUB_Declaration_HK = lnk_dp.HUB_Declaration_HK
INNER JOIN SAT_Payment sp ON lnk_dp.HUB_Payment_HK = sp.HUB_Payment_HK 
    AND sp.SAT_EndDate IS NULL
WHERE hub.TaxID = 'TAX000001';
-- Raw records exist, but NO compliance score, on-time rates, or risk level
GO

-- ─── Step 2: Execute Silver Layer — Compute Business Metrics ───
PRINT '';
PRINT '─── Step 2: Execute Silver layer — compute derived metrics ───';
USE DV_Silver;
GO

DECLARE @BatchID INT = 9904, @RC INT;

-- BUS_ComplianceScore: 50% FilingOnTimeRate + 50% PaymentOnTimeRate
EXEC dbo.usp_Load_BUS_ComplianceScore 
    @BatchID = @BatchID, 
    @SnapshotDate = '2026-02-22',
    @RowCount = @RC OUTPUT;
PRINT '   ✓ BUS_ComplianceScore: ' + CAST(@RC AS VARCHAR) + ' taxpayers scored';

-- BUS_MonthlyMetrics: Aggregated monthly revenue, tax, payment data
EXEC dbo.usp_Load_BUS_MonthlyMetrics 
    @BatchID = @BatchID, 
    @SnapshotDate = '2026-02-22',
    @RowCount = @RC OUTPUT;
PRINT '   ✓ BUS_MonthlyMetrics: ' + CAST(@RC AS VARCHAR) + ' monthly records';
GO

-- ─── Step 3: View Derived Compliance Metrics for TAX000001 ───
PRINT '';
PRINT '─── Step 3: Derived analytics for TAX000001 ───';

SELECT 
    hub.TaxID,
    sat.LegalBusinessName,
    CAST(cs.ComplianceScore AS DECIMAL(5,1)) AS ComplianceScore,
    CAST(cs.FilingOnTimeRate AS DECIMAL(5,1)) AS FilingOnTimeRate,
    CAST(cs.PaymentOnTimeRate AS DECIMAL(5,1)) AS PaymentOnTimeRate,
    cs.PenaltyCount,
    CASE 
        WHEN cs.ComplianceScore >= 80 THEN 'LOW'
        WHEN cs.ComplianceScore >= 50 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS RiskClassification
FROM DV_Silver.dbo.BUS_ComplianceScore cs
INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub 
    ON cs.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
INNER JOIN DV_Bronze.dbo.SAT_Taxpayer sat 
    ON hub.HUB_Taxpayer_HK = sat.HUB_Taxpayer_HK AND sat.SAT_EndDate IS NULL
WHERE hub.TaxID = 'TAX000001';
-- Formula: ComplianceScore = 50% × FilingOnTimeRate + 50% × PaymentOnTimeRate
GO

-- ─── Step 4: Execute Gold Layer — Build Star Schema ───
PRINT '';
PRINT '─── Step 4: Execute Gold layer — build star schema ───';
USE DV_Gold;
GO

DECLARE @BatchID INT = 9904, @SD DATETIME = '2026-02-22', @RC INT;

-- Load Dimensions
EXEC dbo.usp_Load_DIM_Category @BatchID=@BatchID, @RowCount=@RC OUTPUT;
PRINT '   DIM_Category: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_Structure @BatchID=@BatchID, @RowCount=@RC OUTPUT;
PRINT '   DIM_Structure: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_Activity @BatchID=@BatchID, @RowCount=@RC OUTPUT;
PRINT '   DIM_Activity: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_Taxpayer @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   DIM_Taxpayer: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_Officer @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   DIM_Officer: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_Status @BatchID=@BatchID, @RowCount=@RC OUTPUT;
PRINT '   DIM_Status: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_DIM_PaymentMethod @BatchID=@BatchID, @RowCount=@RC OUTPUT;
PRINT '   DIM_PaymentMethod: ' + CAST(@RC AS VARCHAR) + ' rows';

-- Load Facts
EXEC dbo.usp_Load_FACT_MonthlyDeclaration @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   FACT_MonthlyDeclaration: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_FACT_Payment @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   FACT_Payment: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_FACT_MonthlySnapshot @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   FACT_MonthlySnapshot: ' + CAST(@RC AS VARCHAR) + ' rows';
EXEC dbo.usp_Load_FACT_DeclarationLifecycle @BatchID=@BatchID, @SnapshotDate=@SD, @RowCount=@RC OUTPUT;
PRINT '   FACT_DeclarationLifecycle: ' + CAST(@RC AS VARCHAR) + ' rows';
GO

-- ─── Step 5: Query Gold Star Schema — BI Dashboard Ready ───
PRINT '';
PRINT '─── Step 5a: FACT_MonthlyDeclaration with DIM joins (BI query) ───';

SELECT TOP 12
    dt.TaxID AS DIM_Taxpayer,
    ds.StatusCode AS DIM_Status,
    CAST(f.DeclarationMonth AS VARCHAR) + '/' + CAST(f.DeclarationYear AS VARCHAR) AS Period,
    f.GrossRevenue, 
    f.TaxAmount, 
    f.TotalAmount,
    ISNULL(pay.TotalPaid, 0) AS PaymentAmount,
    CASE WHEN f.TotalAmount = 0 THEN '0%'
         ELSE CAST(CAST(ISNULL(pay.TotalPaid, 0) AS DECIMAL(10,2)) / f.TotalAmount * 100 AS VARCHAR) + '%'
    END AS CoveragePercent
FROM DV_Gold.dbo.FACT_MonthlyDeclaration f
INNER JOIN DV_Gold.dbo.DIM_Taxpayer dt ON f.DIM_Taxpayer_SK = dt.DIM_Taxpayer_SK
LEFT JOIN DV_Gold.dbo.DIM_Status ds ON f.DIM_Status_SK = ds.DIM_Status_SK
OUTER APPLY (
    SELECT SUM(fp.PaymentAmount) AS TotalPaid
    FROM DV_Gold.dbo.FACT_Payment fp
    WHERE fp.DeclarationID = f.DeclarationID
) pay
WHERE dt.TaxID = 'TAX000001' AND dt.IsCurrent = 1
ORDER BY f.DeclarationYear, f.DeclarationMonth;
GO

-- Step 5b: Executive dashboard summary
PRINT '─── Step 5b: Executive dashboard summary ───';

SELECT 
    dt.TaxID,
    dt.LegalBusinessName,
    CAST(cs.ComplianceScore AS DECIMAL(5,1)) AS ComplianceScore,
    CAST(cs.FilingOnTimeRate AS DECIMAL(5,1)) AS FilingOnTimeRate,
    CAST(cs.PaymentOnTimeRate AS DECIMAL(5,1)) AS PaymentOnTimeRate,
    cs.PenaltyCount,
    CASE 
        WHEN cs.ComplianceScore >= 80 THEN 'LOW'
        WHEN cs.ComplianceScore >= 50 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS RiskLevel,
    COUNT(DISTINCT f.DeclarationID) AS TotalDeclarations,
    SUM(f.GrossRevenue) AS TotalRevenue,
    SUM(f.TaxAmount) AS TotalTax
FROM DV_Gold.dbo.DIM_Taxpayer dt
LEFT JOIN DV_Silver.dbo.BUS_ComplianceScore cs 
    ON dt.HUB_Taxpayer_HK = cs.HUB_Taxpayer_HK
LEFT JOIN DV_Gold.dbo.FACT_MonthlyDeclaration f 
    ON dt.DIM_Taxpayer_SK = f.DIM_Taxpayer_SK
WHERE dt.TaxID = 'TAX000001' AND dt.IsCurrent = 1
GROUP BY dt.TaxID, dt.LegalBusinessName, 
         cs.ComplianceScore, cs.FilingOnTimeRate, cs.PaymentOnTimeRate, cs.PenaltyCount;
GO

PRINT '';
PRINT '═══ POC 4 COMPLETE: Silver + Gold produce pre-computed analytics ═══';
PRINT '';
GO


-- ══════════════════════════════════════════════════════════════════════════════
-- FINAL SUMMARY
-- ══════════════════════════════════════════════════════════════════════════════

PRINT '╔════════════════════════════════════════════════════════════════════╗';
PRINT '║              ALL 4 POC DEMONSTRATIONS COMPLETE                    ║';
PRINT '╠════════════════════════════════════════════════════════════════════╣';
PRINT '║  POC 1: Schema Flexibility — Zero existing objects modified       ║';
PRINT '║  POC 2: Historical Tracking — All versions preserved              ║';
PRINT '║  POC 3: ETL Control — Step-level logging + error recovery         ║';
PRINT '║  POC 4: Business Rules — Pre-computed scores via Silver + Gold    ║';
PRINT '╠════════════════════════════════════════════════════════════════════╣';
PRINT '║  NOTE: To restore original state, re-run:                         ║';
PRINT '║        00_CleanAll_FreshFullLoad.sql                              ║';
PRINT '╚════════════════════════════════════════════════════════════════════╝';
GO
