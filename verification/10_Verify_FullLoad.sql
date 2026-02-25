-- ============================================================================
-- Script: 10_Verify_FullLoad.sql
-- Purpose: Comprehensive verification of full-load ETL execution.
--          Run in SSMS after Master_Complete_Pipeline finishes.
-- Author: Data Management Bureau — GDT Cambodia
-- Date: 2026-02-22
-- ============================================================================

SET NOCOUNT ON;

PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║         FULL-LOAD VERIFICATION REPORT                      ║';
PRINT '║         Generated: ' + CONVERT(VARCHAR(20), GETDATE(), 120) + '              ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';


-- ════════════════════════════════════════════════════════════════
-- 1. ETL BATCH STATUS
-- ════════════════════════════════════════════════════════════════
PRINT '═══ [1/7] ETL BATCH STATUS ═══';
PRINT '';

SELECT
    b.BatchID,
    b.BatchStartTime,
    b.BatchEndTime,
    b.BatchStatus,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS DurationSec,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID) AS TotalSteps,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Success') AS SuccessSteps,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Failed') AS FailedSteps
FROM ETL_Control.dbo.ETL_BatchLog b
ORDER BY b.BatchID DESC;


-- ════════════════════════════════════════════════════════════════
-- 2. ETL STEP LOG (Last Batch)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [2/7] ETL STEP LOG (Latest Batch) ═══';
PRINT '';

DECLARE @LastBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog);

SELECT
    s.StepLogID,
    s.StepName,
    s.StepStatus,
    s.StepStartTime,
    s.StepEndTime,
    DATEDIFF(SECOND, s.StepStartTime, s.StepEndTime) AS DurationSec,
    s.RecordsProcessed
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = @LastBatchID
ORDER BY s.StepLogID;


-- ════════════════════════════════════════════════════════════════
-- 3. ERROR LOG (if any)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [3/7] ERROR LOG ═══';
PRINT '';

IF EXISTS (SELECT 1 FROM ETL_Control.dbo.ETL_ErrorLog WHERE BatchID = @LastBatchID)
BEGIN
    SELECT
        e.ErrorID,
        e.BatchID,
        e.StepLogID,
        e.ErrorSeverity,
        e.ErrorSource,
        e.ErrorMessage,
        e.ErrorDateTime
    FROM ETL_Control.dbo.ETL_ErrorLog e
    WHERE e.BatchID = @LastBatchID
    ORDER BY e.ErrorID;
END
ELSE
    PRINT '   ✓ No errors logged for BatchID ' + CAST(@LastBatchID AS VARCHAR(10));


-- ════════════════════════════════════════════════════════════════
-- 4. ROW COUNTS — All Layers
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [4/7] ROW COUNTS — All Layers ═══';
PRINT '';

SELECT 'DV_Staging' AS [Db], 'STG_Category' AS [Table], COUNT(*) AS [Rows] FROM DV_Staging.dbo.STG_Category
UNION ALL SELECT 'DV_Staging', 'STG_Structure', COUNT(*) FROM DV_Staging.dbo.STG_Structure
UNION ALL SELECT 'DV_Staging', 'STG_Activity', COUNT(*) FROM DV_Staging.dbo.STG_Activity
UNION ALL SELECT 'DV_Staging', 'STG_Taxpayer', COUNT(*) FROM DV_Staging.dbo.STG_Taxpayer
UNION ALL SELECT 'DV_Staging', 'STG_Owner', COUNT(*) FROM DV_Staging.dbo.STG_Owner
UNION ALL SELECT 'DV_Staging', 'STG_Officer', COUNT(*) FROM DV_Staging.dbo.STG_Officer
UNION ALL SELECT 'DV_Staging', 'STG_MonthlyDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_MonthlyDeclaration
UNION ALL SELECT 'DV_Staging', 'STG_AnnualDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_AnnualDeclaration
UNION ALL SELECT 'DV_Staging', 'STG_Payment', COUNT(*) FROM DV_Staging.dbo.STG_Payment
-- Bronze Hubs
UNION ALL SELECT 'DV_Bronze', 'HUB_Category', COUNT(*) FROM DV_Bronze.dbo.HUB_Category
UNION ALL SELECT 'DV_Bronze', 'HUB_Structure', COUNT(*) FROM DV_Bronze.dbo.HUB_Structure
UNION ALL SELECT 'DV_Bronze', 'HUB_Activity', COUNT(*) FROM DV_Bronze.dbo.HUB_Activity
UNION ALL SELECT 'DV_Bronze', 'HUB_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'HUB_Owner', COUNT(*) FROM DV_Bronze.dbo.HUB_Owner
UNION ALL SELECT 'DV_Bronze', 'HUB_Officer', COUNT(*) FROM DV_Bronze.dbo.HUB_Officer
UNION ALL SELECT 'DV_Bronze', 'HUB_Declaration', COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration
UNION ALL SELECT 'DV_Bronze', 'HUB_Payment', COUNT(*) FROM DV_Bronze.dbo.HUB_Payment
UNION ALL SELECT 'DV_Bronze', 'HUB_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.HUB_AnnualDecl
-- Bronze Satellites
UNION ALL SELECT 'DV_Bronze', 'SAT_Category', COUNT(*) FROM DV_Bronze.dbo.SAT_Category
UNION ALL SELECT 'DV_Bronze', 'SAT_Structure', COUNT(*) FROM DV_Bronze.dbo.SAT_Structure
UNION ALL SELECT 'DV_Bronze', 'SAT_Activity', COUNT(*) FROM DV_Bronze.dbo.SAT_Activity
UNION ALL SELECT 'DV_Bronze', 'SAT_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'SAT_Owner', COUNT(*) FROM DV_Bronze.dbo.SAT_Owner
UNION ALL SELECT 'DV_Bronze', 'SAT_Officer', COUNT(*) FROM DV_Bronze.dbo.SAT_Officer
UNION ALL SELECT 'DV_Bronze', 'SAT_MonthlyDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_MonthlyDecl
UNION ALL SELECT 'DV_Bronze', 'SAT_Payment', COUNT(*) FROM DV_Bronze.dbo.SAT_Payment
UNION ALL SELECT 'DV_Bronze', 'SAT_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_AnnualDecl
-- Bronze Links
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerDeclaration', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
UNION ALL SELECT 'DV_Bronze', 'LNK_DeclarationPayment', COUNT(*) FROM DV_Bronze.dbo.LNK_DeclarationPayment
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerOfficer', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOfficer
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerOwner', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOwner
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerAnnualDecl', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerAnnualDecl
-- Silver
UNION ALL SELECT 'DV_Silver', 'PIT_Taxpayer', COUNT(*) FROM DV_Silver.dbo.PIT_Taxpayer
UNION ALL SELECT 'DV_Silver', 'PIT_Declaration', COUNT(*) FROM DV_Silver.dbo.PIT_Declaration
UNION ALL SELECT 'DV_Silver', 'PIT_Payment', COUNT(*) FROM DV_Silver.dbo.PIT_Payment
UNION ALL SELECT 'DV_Silver', 'BRG_Taxpayer_Owner', COUNT(*) FROM DV_Silver.dbo.BRG_Taxpayer_Owner
UNION ALL SELECT 'DV_Silver', 'BUS_ComplianceScore', COUNT(*) FROM DV_Silver.dbo.BUS_ComplianceScore
UNION ALL SELECT 'DV_Silver', 'BUS_MonthlyMetrics', COUNT(*) FROM DV_Silver.dbo.BUS_MonthlyMetrics
-- Gold Dimensions
UNION ALL SELECT 'DV_Gold', 'DIM_Taxpayer', COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer
UNION ALL SELECT 'DV_Gold', 'DIM_Officer', COUNT(*) FROM DV_Gold.dbo.DIM_Officer
UNION ALL SELECT 'DV_Gold', 'DIM_Category', COUNT(*) FROM DV_Gold.dbo.DIM_Category
UNION ALL SELECT 'DV_Gold', 'DIM_Structure', COUNT(*) FROM DV_Gold.dbo.DIM_Structure
UNION ALL SELECT 'DV_Gold', 'DIM_Activity', COUNT(*) FROM DV_Gold.dbo.DIM_Activity
UNION ALL SELECT 'DV_Gold', 'DIM_PaymentMethod', COUNT(*) FROM DV_Gold.dbo.DIM_PaymentMethod
UNION ALL SELECT 'DV_Gold', 'DIM_Status', COUNT(*) FROM DV_Gold.dbo.DIM_Status
-- Gold Facts
UNION ALL SELECT 'DV_Gold', 'FACT_MonthlyDeclaration', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration
UNION ALL SELECT 'DV_Gold', 'FACT_Payment', COUNT(*) FROM DV_Gold.dbo.FACT_Payment
UNION ALL SELECT 'DV_Gold', 'FACT_MonthlySnapshot', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlySnapshot
UNION ALL SELECT 'DV_Gold', 'FACT_DeclarationLifecycle', COUNT(*) FROM DV_Gold.dbo.FACT_DeclarationLifecycle
ORDER BY 1, 2;


-- ════════════════════════════════════════════════════════════════
-- 5. DATA INTEGRITY CHECKS
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [5/7] DATA INTEGRITY CHECKS ═══';
PRINT '';

-- 5a. Hub row counts must match Staging DISTINCT business keys
SELECT 'Hub vs Staging' AS [Check],
    'HUB_Taxpayer' AS [Table],
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer) AS [Hub_Rows],
    (SELECT COUNT(DISTINCT TaxID) FROM DV_Staging.dbo.STG_Taxpayer) AS [STG_Distinct_BK],
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer) =
              (SELECT COUNT(DISTINCT TaxID) FROM DV_Staging.dbo.STG_Taxpayer)
         THEN 'PASS' ELSE 'FAIL' END AS [Result]
UNION ALL
SELECT 'Hub vs Staging', 'HUB_Declaration',
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration),
    (SELECT COUNT(DISTINCT DeclarationID) FROM DV_Staging.dbo.STG_MonthlyDeclaration),
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration) =
              (SELECT COUNT(DISTINCT DeclarationID) FROM DV_Staging.dbo.STG_MonthlyDeclaration)
         THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Hub vs Staging', 'HUB_Payment',
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Payment),
    (SELECT COUNT(DISTINCT PaymentID) FROM DV_Staging.dbo.STG_Payment),
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Payment) =
              (SELECT COUNT(DISTINCT PaymentID) FROM DV_Staging.dbo.STG_Payment)
         THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Hub vs Staging', 'HUB_Category',
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Category),
    (SELECT COUNT(DISTINCT CategoryID) FROM DV_Staging.dbo.STG_Category),
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Category) =
              (SELECT COUNT(DISTINCT CategoryID) FROM DV_Staging.dbo.STG_Category)
         THEN 'PASS' ELSE 'FAIL' END;

-- 5b. Satellite current rows (SAT_EndDate IS NULL) = Hub rows (on full load)
SELECT 'SAT Current vs Hub' AS [Check],
    'SAT_Taxpayer' AS [Table],
    (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer WHERE SAT_EndDate IS NULL) AS [SAT_Current],
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer) AS [Hub_Rows],
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer WHERE SAT_EndDate IS NULL) =
              (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer)
         THEN 'PASS' ELSE 'FAIL' END AS [Result]
UNION ALL
SELECT 'SAT Current vs Hub', 'SAT_MonthlyDecl',
    (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_MonthlyDecl WHERE SAT_EndDate IS NULL),
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration),
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_MonthlyDecl WHERE SAT_EndDate IS NULL) =
              (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration)
         THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'SAT Current vs Hub', 'SAT_Payment',
    (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_Payment WHERE SAT_EndDate IS NULL),
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Payment),
    CASE WHEN (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_Payment WHERE SAT_EndDate IS NULL) =
              (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Payment)
         THEN 'PASS' ELSE 'FAIL' END;

-- 5c. DIM row counts vs Hub (SCD1 should match Hub, SCD2 >= Hub)
SELECT 'DIM vs Hub' AS [Check],
    'DIM_Category (SCD1)' AS [Table],
    (SELECT COUNT(*) FROM DV_Gold.dbo.DIM_Category) AS [DIM_Rows],
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Category) AS [Hub_Rows],
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.DIM_Category) =
              (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Category)
         THEN 'PASS' ELSE 'FAIL' END AS [Result]
UNION ALL
SELECT 'DIM vs Hub', 'DIM_Taxpayer (SCD2 current)',
    (SELECT COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer WHERE IsCurrent = 1),
    (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer),
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer WHERE IsCurrent = 1) =
              (SELECT COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer)
         THEN 'PASS' ELSE 'FAIL' END;

-- 5d. Fact rows should be > 0
SELECT 'Fact Non-Empty' AS [Check],
    'FACT_MonthlyDeclaration' AS [Table],
    (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration) AS [Rows],
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration) > 0
         THEN 'PASS' ELSE 'FAIL' END AS [Result]
UNION ALL
SELECT 'Fact Non-Empty', 'FACT_Payment',
    (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_Payment),
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_Payment) > 0
         THEN 'PASS' ELSE 'FAIL' END
UNION ALL
SELECT 'Fact Non-Empty', 'FACT_MonthlySnapshot',
    (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_MonthlySnapshot),
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_MonthlySnapshot) > 0
         THEN 'PASS' ELSE 'WARN - may be 0 if Silver not populated' END
UNION ALL
SELECT 'Fact Non-Empty', 'FACT_DeclarationLifecycle',
    (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_DeclarationLifecycle),
    CASE WHEN (SELECT COUNT(*) FROM DV_Gold.dbo.FACT_DeclarationLifecycle) > 0
         THEN 'PASS' ELSE 'FAIL' END;


-- ════════════════════════════════════════════════════════════════
-- 6. NULL SURROGATE KEY CHECK (Fact Lookups)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [6/7] NULL SURROGATE KEY CHECK (Fact FK Coverage) ═══';
PRINT '';

SELECT 'FACT_MonthlyDeclaration' AS [Table],
    COUNT(*) AS [Total_Rows],
    SUM(CASE WHEN DIM_Taxpayer_SK IS NULL THEN 1 ELSE 0 END) AS [NULL_Taxpayer_SK],
    SUM(CASE WHEN DIM_Category_SK IS NULL THEN 1 ELSE 0 END) AS [NULL_Category_SK],
    SUM(CASE WHEN DIM_Status_SK IS NULL THEN 1 ELSE 0 END) AS [NULL_Status_SK]
FROM DV_Gold.dbo.FACT_MonthlyDeclaration
UNION ALL
SELECT 'FACT_Payment',
    COUNT(*),
    SUM(CASE WHEN DIM_Taxpayer_SK IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN DIM_PaymentMethod_SK IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN DIM_Status_SK IS NULL THEN 1 ELSE 0 END)
FROM DV_Gold.dbo.FACT_Payment
UNION ALL
SELECT 'FACT_DeclarationLifecycle',
    COUNT(*),
    SUM(CASE WHEN DIM_Taxpayer_SK IS NULL THEN 1 ELSE 0 END),
    NULL,
    SUM(CASE WHEN DIM_Status_SK IS NULL THEN 1 ELSE 0 END)
FROM DV_Gold.dbo.FACT_DeclarationLifecycle;


-- ════════════════════════════════════════════════════════════════
-- 7. SUMMARY VERDICT
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [7/7] SUMMARY VERDICT ═══';
PRINT '';

DECLARE @BatchStatus VARCHAR(20) = (SELECT TOP 1 BatchStatus FROM ETL_Control.dbo.ETL_BatchLog ORDER BY BatchID DESC);
DECLARE @FailedSteps INT = (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog WHERE BatchID = @LastBatchID AND StepStatus = 'Failed');
DECLARE @ErrorCount INT = (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_ErrorLog WHERE BatchID = @LastBatchID);
DECLARE @EmptyTables INT = 0;

-- Count tables with 0 rows (excluding Silver which may legitimately be empty)
SELECT @EmptyTables = COUNT(*) FROM (
    SELECT 'HUB_Taxpayer' AS t, COUNT(*) AS c FROM DV_Bronze.dbo.HUB_Taxpayer
    UNION ALL SELECT 'SAT_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
    UNION ALL SELECT 'LNK_TaxpayerDeclaration', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
    UNION ALL SELECT 'DIM_Taxpayer', COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer
    UNION ALL SELECT 'DIM_Category', COUNT(*) FROM DV_Gold.dbo.DIM_Category
    UNION ALL SELECT 'FACT_MonthlyDeclaration', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration
    UNION ALL SELECT 'FACT_Payment', COUNT(*) FROM DV_Gold.dbo.FACT_Payment
) x WHERE x.c = 0;

PRINT '   Batch Status:    ' + ISNULL(@BatchStatus, 'N/A');
PRINT '   Failed Steps:    ' + CAST(@FailedSteps AS VARCHAR(10));
PRINT '   Errors Logged:   ' + CAST(@ErrorCount AS VARCHAR(10));
PRINT '   Empty Key Tables:' + CAST(@EmptyTables AS VARCHAR(10));
PRINT '';

IF @BatchStatus = 'Success' AND @FailedSteps = 0 AND @ErrorCount = 0 AND @EmptyTables = 0
    PRINT '   ╔═══════════════════════════════════════╗';
    PRINT '   ║  ✅  FULL LOAD: PASSED                ║';
    PRINT '   ╚═══════════════════════════════════════╝';
IF @BatchStatus = 'Success' AND @FailedSteps = 0 AND @EmptyTables > 0
    PRINT '   ╔═══════════════════════════════════════╗';
    PRINT '   ║  ⚠️  FULL LOAD: PASSED WITH WARNINGS  ║';
    PRINT '   ╚═══════════════════════════════════════╝';
IF @BatchStatus <> 'Success' OR @FailedSteps > 0
BEGIN
    PRINT '   ╔═══════════════════════════════════════╗';
    PRINT '   ║  ❌  FULL LOAD: FAILED                ║';
    PRINT '   ╚═══════════════════════════════════════╝';
    PRINT '';
    PRINT '   Check sections [2] and [3] above for failed steps and error details.';
END

PRINT '';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  Report Complete: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
GO
