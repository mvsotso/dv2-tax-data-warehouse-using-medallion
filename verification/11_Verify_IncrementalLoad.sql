-- ============================================================================
-- Script: 11_Verify_IncrementalLoad.sql
-- Purpose: Verify incremental ETL execution after source data changes.
--          Run in SSMS after Master_Complete_Pipeline (incremental) finishes.
-- Author: Data Management Bureau — GDT Cambodia
-- Date: 2026-02-22
-- ============================================================================

SET NOCOUNT ON;

PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║      INCREMENTAL LOAD VERIFICATION REPORT                  ║';
PRINT '║      Generated: ' + CONVERT(VARCHAR(20), GETDATE(), 120) + '              ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';


-- ════════════════════════════════════════════════════════════════
-- 1. BATCH COMPARISON (Full vs Incremental)
-- ════════════════════════════════════════════════════════════════
PRINT '═══ [1/8] BATCH COMPARISON ═══';
PRINT '';

SELECT
    b.BatchID,
    b.BatchStatus,
    b.BatchStartTime,
    b.BatchEndTime,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS DurationSec,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID) AS TotalSteps,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Success') AS SuccessSteps,
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Failed') AS FailedSteps
FROM ETL_Control.dbo.ETL_BatchLog b
ORDER BY b.BatchID DESC;


-- ════════════════════════════════════════════════════════════════
-- 2. INCREMENTAL STEP LOG (Latest Batch)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [2/8] INCREMENTAL STEP LOG (Latest Batch) ═══';
PRINT '';

DECLARE @LastBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog);
DECLARE @PrevBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog WHERE BatchID < @LastBatchID);

SELECT
    s.StepLogID,
    s.StepName,
    s.StepStatus,
    s.RecordsProcessed,
    DATEDIFF(SECOND, s.StepStartTime, s.StepEndTime) AS DurationSec,
    ISNULL(prev.RecordsProcessed, 0) AS PrevBatchRecords,
    s.RecordsProcessed - ISNULL(prev.RecordsProcessed, 0) AS Delta
FROM ETL_Control.dbo.ETL_StepLog s
LEFT JOIN ETL_Control.dbo.ETL_StepLog prev
    ON prev.StepName = s.StepName AND prev.BatchID = @PrevBatchID
WHERE s.BatchID = @LastBatchID
ORDER BY s.StepLogID;


-- ════════════════════════════════════════════════════════════════
-- 3. ERROR LOG
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [3/8] ERROR LOG ═══';
PRINT '';

IF EXISTS (SELECT 1 FROM ETL_Control.dbo.ETL_ErrorLog WHERE BatchID = @LastBatchID)
BEGIN
    SELECT
        e.ErrorID,
        e.BatchID,
        e.StepLogID,
        e.ErrorSeverity,
        e.ErrorSource,
        LEFT(e.ErrorMessage, 200) AS ErrorMessage,
        e.ErrorDateTime
    FROM ETL_Control.dbo.ETL_ErrorLog e
    WHERE e.BatchID = @LastBatchID
    ORDER BY e.ErrorID;
END
ELSE
    PRINT '   ✓ No errors logged for BatchID ' + CAST(@LastBatchID AS VARCHAR(10));


-- ════════════════════════════════════════════════════════════════
-- 4. WATERMARK STATUS
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [4/8] WATERMARK STATUS ═══';
PRINT '';

SELECT
    w.TableName,
    w.ColumnName,
    w.LastValue,
    w.LastLoadDate,
    w.UpdatedDate,
    w.IsActive,
    CASE 
        WHEN w.LastLoadDate >= DATEADD(HOUR, -1, GETDATE()) THEN 'UPDATED (recent)'
        ELSE 'STALE'
    END AS WatermarkStatus
FROM ETL_Control.dbo.ETL_Watermark w
WHERE w.IsActive = 1
ORDER BY w.TableName;


-- ════════════════════════════════════════════════════════════════
-- 5. ROW COUNT COMPARISON — All Layers
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [5/8] ROW COUNTS — All Layers ═══';
PRINT '';

SELECT 'DV_Staging' AS Layer, 'STG_Category' AS TableName, COUNT(*) AS Rows FROM DV_Staging.dbo.STG_Category
UNION ALL SELECT 'DV_Staging', 'STG_Structure', COUNT(*) FROM DV_Staging.dbo.STG_Structure
UNION ALL SELECT 'DV_Staging', 'STG_Activity', COUNT(*) FROM DV_Staging.dbo.STG_Activity
UNION ALL SELECT 'DV_Staging', 'STG_Taxpayer', COUNT(*) FROM DV_Staging.dbo.STG_Taxpayer
UNION ALL SELECT 'DV_Staging', 'STG_Owner', COUNT(*) FROM DV_Staging.dbo.STG_Owner
UNION ALL SELECT 'DV_Staging', 'STG_Officer', COUNT(*) FROM DV_Staging.dbo.STG_Officer
UNION ALL SELECT 'DV_Staging', 'STG_MonthlyDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_MonthlyDeclaration
UNION ALL SELECT 'DV_Staging', 'STG_AnnualDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_AnnualDeclaration
UNION ALL SELECT 'DV_Staging', 'STG_Payment', COUNT(*) FROM DV_Staging.dbo.STG_Payment
UNION ALL SELECT 'DV_Bronze', 'HUB_Category', COUNT(*) FROM DV_Bronze.dbo.HUB_Category
UNION ALL SELECT 'DV_Bronze', 'HUB_Structure', COUNT(*) FROM DV_Bronze.dbo.HUB_Structure
UNION ALL SELECT 'DV_Bronze', 'HUB_Activity', COUNT(*) FROM DV_Bronze.dbo.HUB_Activity
UNION ALL SELECT 'DV_Bronze', 'HUB_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'HUB_Owner', COUNT(*) FROM DV_Bronze.dbo.HUB_Owner
UNION ALL SELECT 'DV_Bronze', 'HUB_Officer', COUNT(*) FROM DV_Bronze.dbo.HUB_Officer
UNION ALL SELECT 'DV_Bronze', 'HUB_Declaration', COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration
UNION ALL SELECT 'DV_Bronze', 'HUB_Payment', COUNT(*) FROM DV_Bronze.dbo.HUB_Payment
UNION ALL SELECT 'DV_Bronze', 'HUB_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.HUB_AnnualDecl
UNION ALL SELECT 'DV_Bronze', 'SAT_Category', COUNT(*) FROM DV_Bronze.dbo.SAT_Category
UNION ALL SELECT 'DV_Bronze', 'SAT_Structure', COUNT(*) FROM DV_Bronze.dbo.SAT_Structure
UNION ALL SELECT 'DV_Bronze', 'SAT_Activity', COUNT(*) FROM DV_Bronze.dbo.SAT_Activity
UNION ALL SELECT 'DV_Bronze', 'SAT_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'SAT_Owner', COUNT(*) FROM DV_Bronze.dbo.SAT_Owner
UNION ALL SELECT 'DV_Bronze', 'SAT_Officer', COUNT(*) FROM DV_Bronze.dbo.SAT_Officer
UNION ALL SELECT 'DV_Bronze', 'SAT_MonthlyDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_MonthlyDecl
UNION ALL SELECT 'DV_Bronze', 'SAT_Payment', COUNT(*) FROM DV_Bronze.dbo.SAT_Payment
UNION ALL SELECT 'DV_Bronze', 'SAT_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_AnnualDecl
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerDeclaration', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
UNION ALL SELECT 'DV_Bronze', 'LNK_DeclarationPayment', COUNT(*) FROM DV_Bronze.dbo.LNK_DeclarationPayment
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerOfficer', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOfficer
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerOwner', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOwner
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerAnnualDecl', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerAnnualDecl
UNION ALL SELECT 'DV_Silver', 'PIT_Taxpayer', COUNT(*) FROM DV_Silver.dbo.PIT_Taxpayer
UNION ALL SELECT 'DV_Silver', 'PIT_Declaration', COUNT(*) FROM DV_Silver.dbo.PIT_Declaration
UNION ALL SELECT 'DV_Silver', 'PIT_Payment', COUNT(*) FROM DV_Silver.dbo.PIT_Payment
UNION ALL SELECT 'DV_Silver', 'BRG_Taxpayer_Owner', COUNT(*) FROM DV_Silver.dbo.BRG_Taxpayer_Owner
UNION ALL SELECT 'DV_Silver', 'BUS_ComplianceScore', COUNT(*) FROM DV_Silver.dbo.BUS_ComplianceScore
UNION ALL SELECT 'DV_Silver', 'BUS_MonthlyMetrics', COUNT(*) FROM DV_Silver.dbo.BUS_MonthlyMetrics
UNION ALL SELECT 'DV_Gold', 'DIM_Taxpayer', COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer
UNION ALL SELECT 'DV_Gold', 'DIM_Officer', COUNT(*) FROM DV_Gold.dbo.DIM_Officer
UNION ALL SELECT 'DV_Gold', 'DIM_Category', COUNT(*) FROM DV_Gold.dbo.DIM_Category
UNION ALL SELECT 'DV_Gold', 'DIM_Structure', COUNT(*) FROM DV_Gold.dbo.DIM_Structure
UNION ALL SELECT 'DV_Gold', 'DIM_Activity', COUNT(*) FROM DV_Gold.dbo.DIM_Activity
UNION ALL SELECT 'DV_Gold', 'DIM_PaymentMethod', COUNT(*) FROM DV_Gold.dbo.DIM_PaymentMethod
UNION ALL SELECT 'DV_Gold', 'DIM_Status', COUNT(*) FROM DV_Gold.dbo.DIM_Status
UNION ALL SELECT 'DV_Gold', 'FACT_MonthlyDeclaration', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration
UNION ALL SELECT 'DV_Gold', 'FACT_Payment', COUNT(*) FROM DV_Gold.dbo.FACT_Payment
UNION ALL SELECT 'DV_Gold', 'FACT_MonthlySnapshot', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlySnapshot
UNION ALL SELECT 'DV_Gold', 'FACT_DeclarationLifecycle', COUNT(*) FROM DV_Gold.dbo.FACT_DeclarationLifecycle
ORDER BY 1, 2;


-- ════════════════════════════════════════════════════════════════
-- 6. CHANGE DETECTION — Satellite History Tracking
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [6/8] CHANGE DETECTION — Satellite Versioning ═══';
PRINT '';

-- 6a. SAT_Taxpayer: Should have new versions for updated taxpayers
SELECT 'SAT_Taxpayer Versions' AS [Check],
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END) AS CurrentRows,
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) AS HistoricRows,
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS - history detected' ELSE 'INFO - no changes detected' END AS Result
FROM DV_Bronze.dbo.SAT_Taxpayer;

-- 6b. SAT_Officer: Should have new versions for updated officers
SELECT 'SAT_Officer Versions' AS [Check],
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END) AS CurrentRows,
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) AS HistoricRows,
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS - history detected' ELSE 'INFO - no changes detected' END AS Result
FROM DV_Bronze.dbo.SAT_Officer;

-- 6c. Taxpayers with multiple SAT versions (shows changed taxpayers)
SELECT 'Multi-version Taxpayers' AS [Check],
    COUNT(DISTINCT HUB_Taxpayer_HK) AS TaxpayersWithHistory
FROM DV_Bronze.dbo.SAT_Taxpayer
GROUP BY HUB_Taxpayer_HK
HAVING COUNT(*) > 1;


-- ════════════════════════════════════════════════════════════════
-- 7. SCD TYPE 2 — Gold DIM Version Tracking
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [7/8] SCD TYPE 2 — Dimension Versioning ═══';
PRINT '';

-- 7a. DIM_Taxpayer SCD2 versions
SELECT 'DIM_Taxpayer SCD2' AS [Check],
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
    SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS ExpiredRows,
    CASE WHEN SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) > 0
         THEN 'PASS - SCD2 versions created' ELSE 'INFO - no SCD2 changes' END AS Result
FROM DV_Gold.dbo.DIM_Taxpayer;

-- 7b. DIM_Officer SCD2 versions
SELECT 'DIM_Officer SCD2' AS [Check],
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END) AS CurrentRows,
    SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) AS ExpiredRows,
    CASE WHEN SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) > 0
         THEN 'PASS - SCD2 versions created' ELSE 'INFO - no SCD2 changes' END AS Result
FROM DV_Gold.dbo.DIM_Officer;

-- 7c. Show actual SCD2 history for modified taxpayers
SELECT TOP 10
    dt.TaxID,
    dt.LegalBusinessName,
    dt.EstimatedAnnualRevenue,
    dt.IsCurrent,
    dt.EffectiveDate,
    dt.ExpiryDate
FROM DV_Gold.dbo.DIM_Taxpayer dt
WHERE dt.HUB_Taxpayer_HK IN (
    SELECT HUB_Taxpayer_HK FROM DV_Gold.dbo.DIM_Taxpayer
    GROUP BY HUB_Taxpayer_HK HAVING COUNT(*) > 1
)
ORDER BY dt.TaxID, dt.EffectiveDate;


-- ════════════════════════════════════════════════════════════════
-- 8. SUMMARY VERDICT
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══ [8/8] SUMMARY VERDICT ═══';
PRINT '';

DECLARE @BatchStatus VARCHAR(20) = (SELECT TOP 1 BatchStatus FROM ETL_Control.dbo.ETL_BatchLog ORDER BY BatchID DESC);
DECLARE @FailedSteps INT = (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog WHERE BatchID = @LastBatchID AND StepStatus = 'Failed');
DECLARE @ErrorCount INT = (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_ErrorLog WHERE BatchID = @LastBatchID);
DECLARE @SATHistory INT = (SELECT COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer WHERE SAT_EndDate IS NOT NULL);
DECLARE @DIMHistory INT = (SELECT COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer WHERE IsCurrent = 0);

PRINT '   Batch Status:     ' + ISNULL(@BatchStatus, 'N/A');
PRINT '   Failed Steps:     ' + CAST(@FailedSteps AS VARCHAR(10));
PRINT '   Errors Logged:    ' + CAST(@ErrorCount AS VARCHAR(10));
PRINT '   SAT History Rows: ' + CAST(@SATHistory AS VARCHAR(10));
PRINT '   DIM Expired Rows: ' + CAST(@DIMHistory AS VARCHAR(10));
PRINT '';

IF @BatchStatus = 'Success' AND @FailedSteps = 0 AND @ErrorCount = 0
BEGIN
    IF @SATHistory > 0 OR @DIMHistory > 0
    BEGIN
        PRINT '   ╔═══════════════════════════════════════════════════════╗';
        PRINT '   ║  ✅  INCREMENTAL LOAD: PASSED — Changes detected     ║';
        PRINT '   ╚═══════════════════════════════════════════════════════╝';
    END
    ELSE
    BEGIN
        PRINT '   ╔═══════════════════════════════════════════════════════╗';
        PRINT '   ║  ⚠️  INCREMENTAL LOAD: PASSED — No changes detected  ║';
        PRINT '   ╚═══════════════════════════════════════════════════════╝';
        PRINT '';
        PRINT '   No SAT history or DIM expired rows found.';
        PRINT '   Verify source data was modified before running incremental.';
    END
END
ELSE
BEGIN
    PRINT '   ╔═══════════════════════════════════════════════════════╗';
    PRINT '   ║  ❌  INCREMENTAL LOAD: FAILED                        ║';
    PRINT '   ╚═══════════════════════════════════════════════════════╝';
    PRINT '';
    PRINT '   Check sections [2] and [3] above for failed steps and errors.';
END

PRINT '';
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  Report Complete: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
GO
