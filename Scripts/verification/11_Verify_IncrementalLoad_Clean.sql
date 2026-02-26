-- ============================================================================
-- Script: 11_Verify_IncrementalLoad_Clean.sql
-- Purpose: Clean, consolidated output for Figure 5.9
--          "Incremental Load Change Detection"
-- Run:     SSMS → Ctrl+D (Grid mode) → F5
-- Produces: 4 clean result grids for screenshot
-- ============================================================================

SET NOCOUNT ON;

DECLARE @LastBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog);
DECLARE @PrevBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog WHERE BatchID < @LastBatchID);

-- ════════════════════════════════════════════════════════════════
-- GRID 1: Batch Comparison — Full Load vs Incremental Load
-- ════════════════════════════════════════════════════════════════

SELECT
    b.BatchID,
    CASE 
        WHEN b.BatchID = @PrevBatchID THEN 'Full Load'
        WHEN b.BatchID = @LastBatchID THEN 'Incremental'
        ELSE 'Other'
    END AS [Load Type],
    b.BatchStatus AS [Status],
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS [Duration (sec)],
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Success') AS [Steps OK],
    (SELECT COUNT(*) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID AND s.StepStatus = 'Failed') AS [Failed],
    (SELECT SUM(s.RecordsProcessed) FROM ETL_Control.dbo.ETL_StepLog s WHERE s.BatchID = b.BatchID) AS [Total Records]
FROM ETL_Control.dbo.ETL_BatchLog b
WHERE b.BatchStatus = 'Success'
ORDER BY b.BatchID;

-- ════════════════════════════════════════════════════════════════
-- GRID 2: Incremental Step Log — Delta Changes Only
--         (Showing only steps with Delta <> 0 or new records)
-- ════════════════════════════════════════════════════════════════

SELECT
    s.StepName AS [ETL Step],
    s.StepStatus AS [Status],
    s.RecordsProcessed AS [Records],
    ISNULL(prev.RecordsProcessed, 0) AS [Prev Batch],
    s.RecordsProcessed - ISNULL(prev.RecordsProcessed, 0) AS [Delta],
    DATEDIFF(SECOND, s.StepStartTime, s.StepEndTime) AS [Sec]
FROM ETL_Control.dbo.ETL_StepLog s
LEFT JOIN ETL_Control.dbo.ETL_StepLog prev
    ON prev.StepName = s.StepName AND prev.BatchID = @PrevBatchID
WHERE s.BatchID = @LastBatchID
  AND (s.RecordsProcessed - ISNULL(prev.RecordsProcessed, 0)) <> 0
ORDER BY s.StepLogID;

-- ════════════════════════════════════════════════════════════════
-- GRID 3: SAT Versioning + DIM SCD2 — Change Detection Proof
-- ════════════════════════════════════════════════════════════════

SELECT
    'SAT_Taxpayer' AS [Entity],
    COUNT(*) AS [Total Rows],
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END) AS [Current],
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) AS [Historic],
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END AS [Result]
FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL
SELECT 'SAT_Officer', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Bronze.dbo.SAT_Officer
UNION ALL
SELECT 'SAT_Owner', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Bronze.dbo.SAT_Owner
UNION ALL
SELECT 'SAT_MonthlyDecl', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Bronze.dbo.SAT_MonthlyDecl
UNION ALL
SELECT 'SAT_Payment', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Bronze.dbo.SAT_Payment
UNION ALL
SELECT '--- DIM_Taxpayer (SCD2)', COUNT(*),
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Gold.dbo.DIM_Taxpayer
UNION ALL
SELECT '--- DIM_Officer (SCD2)', COUNT(*),
    SUM(CASE WHEN IsCurrent = 1 THEN 1 ELSE 0 END),
    SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN IsCurrent = 0 THEN 1 ELSE 0 END) > 0
         THEN 'PASS' ELSE 'NO CHANGE' END
FROM DV_Gold.dbo.DIM_Officer;

-- ════════════════════════════════════════════════════════════════
-- GRID 4: SCD2 History Detail — Proof of Version Tracking
-- ════════════════════════════════════════════════════════════════

SELECT
    dt.TaxID,
    dt.LegalBusinessName AS [Business Name],
    dt.EstimatedAnnualRevenue AS [Revenue],
    CASE WHEN dt.IsCurrent = 1 THEN 'Current' ELSE 'Expired' END AS [Version],
    FORMAT(dt.EffectiveDate, 'yyyy-MM-dd HH:mm:ss') AS [Effective],
    ISNULL(FORMAT(dt.ExpiryDate, 'yyyy-MM-dd HH:mm:ss'), '(active)') AS [Expiry]
FROM DV_Gold.dbo.DIM_Taxpayer dt
WHERE dt.HUB_Taxpayer_HK IN (
    SELECT HUB_Taxpayer_HK FROM DV_Gold.dbo.DIM_Taxpayer
    GROUP BY HUB_Taxpayer_HK HAVING COUNT(*) > 1
)
ORDER BY dt.TaxID, dt.EffectiveDate;
GO
