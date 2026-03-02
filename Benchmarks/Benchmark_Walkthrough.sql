-- ══════════════════════════════════════════════════════════════════════════════
-- BENCHMARK WALKTHROUGH — ACTUAL RESULTS CAPTURE
-- Data Vault 2.0 Tax System Data Warehouse
-- ══════════════════════════════════════════════════════════════════════════════
-- Author:      Mr. Sot So
-- Supervisor:  Mr. Chap Chanpiseth
-- Institution: RUPP — Master of Science in Data Science and Engineering
-- Date:        March 2026
--
-- PURPOSE:
--   Walk through ALL benchmarks step-by-step on GCP VM to capture
--   actual, accurate numbers for thesis Chapter 5 (Tables 5.3, 5.4, 5.5).
--
-- EXECUTION PLAN (5 Phases):
--   Phase 1: Pre-flight checks (databases exist, objects correct)
--   Phase 2: Full Load benchmark (Table 5.3 — Full column)
--   Phase 3: Incremental Load benchmark (Table 5.3 — Incremental column)
--   Phase 4: Query Performance benchmark (Table 5.4)
--   Phase 5: Object Count verification (67 objects, 60 SSIS packages)
--
-- NOTE: Scalability (Table 5.5) requires 5 separate pipeline runs
--       with different taxpayer counts — handled in Phase 6 (separate).
--
-- HOW TO USE:
--   Run each Phase as a SEPARATE execution in SSMS.
--   After each phase, record the actual results in the RESULTS COLLECTION
--   section at the bottom, then bring results back to Claude to update docs.
-- ══════════════════════════════════════════════════════════════════════════════


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  PHASE 1: PRE-FLIGHT CHECKS                                             ║
-- ║  Verify all 6 databases exist, objects are correct, data is clean        ║
-- ╚════════════════════════════════════════════════════════════════════════════╝
-- Run this FIRST before any benchmarks.

SET NOCOUNT ON;
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  PHASE 1: PRE-FLIGHT CHECKS';
PRINT '  Time: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- 1a. Check all 6 databases exist
PRINT '--- [1a] Database Existence Check ---';
SELECT 
    name AS [Database],
    CASE 
        WHEN name = 'TaxSystemDB' THEN 'Source'
        WHEN name = 'ETL_Control' THEN 'Control'
        WHEN name = 'DV_Staging' THEN 'Staging'
        WHEN name = 'DV_Bronze' THEN 'Bronze'
        WHEN name = 'DV_Silver' THEN 'Silver'
        WHEN name = 'DV_Gold' THEN 'Gold'
    END AS Layer,
    state_desc AS [State],
    create_date AS [Created]
FROM sys.databases
WHERE name IN ('TaxSystemDB','ETL_Control','DV_Staging','DV_Bronze','DV_Silver','DV_Gold')
ORDER BY CASE 
    WHEN name = 'TaxSystemDB' THEN 1 WHEN name = 'ETL_Control' THEN 2
    WHEN name = 'DV_Staging' THEN 3 WHEN name = 'DV_Bronze' THEN 4
    WHEN name = 'DV_Silver' THEN 5 WHEN name = 'DV_Gold' THEN 6
END;

-- 1b. Object count per database
PRINT '';
PRINT '--- [1b] Object Count per Database ---';
SELECT 'TaxSystemDB' AS [Database], 'Source' AS [Layer], COUNT(*) AS [Tables]
FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'spt_%' AND TABLE_NAME NOT LIKE 'MS%'
UNION ALL
SELECT 'ETL_Control', 'Control', COUNT(*) FROM ETL_Control.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Staging', 'Staging', COUNT(*) FROM DV_Staging.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Bronze', 'Bronze', COUNT(*) FROM DV_Bronze.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Silver', 'Silver', COUNT(*) FROM DV_Silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Gold', 'Gold', COUNT(*) FROM DV_Gold.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';

-- 1c. Source database record counts
PRINT '';
PRINT '--- [1c] Source Database Record Counts ---';
SELECT 'Category' AS [Table], 'Lookup' AS [Type], COUNT(*) AS [Records] FROM TaxSystemDB.dbo.Category
UNION ALL SELECT 'Structure', 'Lookup', COUNT(*) FROM TaxSystemDB.dbo.Structure
UNION ALL SELECT 'Activity', 'Lookup', COUNT(*) FROM TaxSystemDB.dbo.Activity
UNION ALL SELECT 'Taxpayer', 'Reference', COUNT(*) FROM TaxSystemDB.dbo.Taxpayer
UNION ALL SELECT 'Owner', 'Reference', COUNT(*) FROM TaxSystemDB.dbo.Owner
UNION ALL SELECT 'Officer', 'Reference', COUNT(*) FROM TaxSystemDB.dbo.Officer
UNION ALL SELECT 'MonthlyDeclaration', 'Transaction', COUNT(*) FROM TaxSystemDB.dbo.MonthlyDeclaration
UNION ALL SELECT 'AnnualDeclaration', 'Transaction', COUNT(*) FROM TaxSystemDB.dbo.AnnualDeclaration
UNION ALL SELECT 'Payment', 'Transaction', COUNT(*) FROM TaxSystemDB.dbo.Payment
ORDER BY CASE WHEN [Type] = 'Lookup' THEN 1 WHEN [Type] = 'Reference' THEN 2 ELSE 3 END, [Table];

-- 1d. Source total
PRINT '';
PRINT '--- [1d] Source Database Total ---';
SELECT 
    (SELECT COUNT(*) FROM TaxSystemDB.dbo.Taxpayer) AS [Taxpayers],
    (SELECT SUM(cnt) FROM (
        SELECT COUNT(*) AS cnt FROM TaxSystemDB.dbo.Category
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Structure
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Activity
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Taxpayer
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Owner
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Officer
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.MonthlyDeclaration
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.AnnualDeclaration
        UNION ALL SELECT COUNT(*) FROM TaxSystemDB.dbo.Payment
    ) t) AS [Total Source Records];

-- 1e. Check if any previous batch exists
PRINT '';
PRINT '--- [1e] Previous Batch History ---';
SELECT 
    BatchID, BatchType, BatchStatus, 
    CAST(DATEDIFF(MILLISECOND, BatchStartTime, BatchEndTime) / 1000.0 AS DECIMAL(10,1)) AS [Duration_Sec],
    BatchStartTime, BatchEndTime
FROM ETL_Control.dbo.ETL_BatchLog
ORDER BY BatchID;

PRINT '';
PRINT '═══ PHASE 1 COMPLETE ═══';
PRINT 'RECORD these results, then proceed to Phase 2.';
PRINT '';
PRINT 'BEFORE Phase 2:';
PRINT '  1. Run 00_CleanAll_FreshFullLoad.sql to reset all databases';
PRINT '  2. Then run Master_Complete_Pipeline.dtsx (Full Load) in Visual Studio';
PRINT '  3. After SSIS finishes, run Phase 2 below';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  PHASE 2: FULL LOAD BENCHMARK (Table 5.3 — Full Load Column)            ║
-- ║  Run AFTER Master_Complete_Pipeline.dtsx completes Full Load             ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

SET NOCOUNT ON;
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  PHASE 2: FULL LOAD BENCHMARK';
PRINT '  Time: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- 2a. Batch-level result
PRINT '--- [2a] Full Load Batch Result ---';
SELECT
    b.BatchID,
    b.BatchType,
    b.BatchStatus,
    b.BatchStartTime,
    b.BatchEndTime,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS [Duration_Sec_Int],
    CAST(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS DECIMAL(10,1)) AS [Duration_Sec]
FROM ETL_Control.dbo.ETL_BatchLog b
WHERE b.BatchType = 'Full'
ORDER BY b.BatchID DESC;

-- 2b. Per-Layer timing — THIS FEEDS TABLE 5.3 DIRECTLY
PRINT '';
PRINT '--- [2b] Per-Layer Timing (TABLE 5.3 — Full Load) ---';

DECLARE @FullBatchID INT = (
    SELECT TOP 1 BatchID FROM ETL_Control.dbo.ETL_BatchLog 
    WHERE BatchType = 'Full' AND BatchStatus = 'Success'
    ORDER BY BatchID DESC
);

PRINT '  Using BatchID: ' + CAST(ISNULL(@FullBatchID, 0) AS VARCHAR);

SELECT
    CASE
        WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
        WHEN StepName LIKE 'Load_HUB_%' OR StepName LIKE 'Load_SAT_%' 
             OR StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
        WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
             OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
        WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
        ELSE '0. Other'
    END AS [Layer],
    COUNT(*) AS [Steps],
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS [Full_Load_Sec],
    SUM(RecordsProcessed) AS [Records_Processed]
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @FullBatchID
GROUP BY CASE
    WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
    WHEN StepName LIKE 'Load_HUB_%' OR StepName LIKE 'Load_SAT_%' 
         OR StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
    WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
         OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
    WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
    ELSE '0. Other'
END
ORDER BY [Layer];

-- 2c. Pipeline totals for Full Load
PRINT '';
PRINT '--- [2c] Full Load Pipeline Totals ---';
SELECT
    COUNT(*) AS [Total_Steps],
    SUM(CASE WHEN StepStatus = 'Success' THEN 1 ELSE 0 END) AS [Success_Steps],
    SUM(CASE WHEN StepStatus = 'Failed' THEN 1 ELSE 0 END) AS [Failed_Steps],
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS [Total_Sec],
    SUM(RecordsProcessed) AS [Total_Records],
    CASE 
        WHEN SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) > 0
        THEN CAST(SUM(RecordsProcessed) * 1000.0 / SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) AS INT)
        ELSE 0
    END AS [Records_Per_Sec]
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @FullBatchID;

-- 2d. All individual steps
PRINT '';
PRINT '--- [2d] All Individual Steps (Full Load) ---';
SELECT
    ROW_NUMBER() OVER (ORDER BY s.StepLogID) AS [#],
    s.StepName,
    s.StepStatus,
    CAST(DATEDIFF(MILLISECOND, s.StepStartTime, s.StepEndTime) / 1000.0 AS DECIMAL(10,2)) AS [Duration_Sec],
    s.RecordsProcessed AS [Records]
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = @FullBatchID
ORDER BY s.StepLogID;

-- 2e. Error count
PRINT '';
PRINT '--- [2e] Error Count ---';
SELECT 
    COUNT(*) AS [Error_Count],
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS [Result]
FROM ETL_Control.dbo.ETL_ErrorLog
WHERE BatchID = @FullBatchID;

-- 2f. Row counts across all layers (for verification)
PRINT '';
PRINT '--- [2f] Row Counts All Layers (Full Load) ---';
SELECT 'Staging' AS [Layer], 'STG_Category' AS [Table], COUNT(*) AS [Rows] FROM DV_Staging.dbo.STG_Category
UNION ALL SELECT 'Staging', 'STG_Structure', COUNT(*) FROM DV_Staging.dbo.STG_Structure
UNION ALL SELECT 'Staging', 'STG_Activity', COUNT(*) FROM DV_Staging.dbo.STG_Activity
UNION ALL SELECT 'Staging', 'STG_Taxpayer', COUNT(*) FROM DV_Staging.dbo.STG_Taxpayer
UNION ALL SELECT 'Staging', 'STG_Owner', COUNT(*) FROM DV_Staging.dbo.STG_Owner
UNION ALL SELECT 'Staging', 'STG_Officer', COUNT(*) FROM DV_Staging.dbo.STG_Officer
UNION ALL SELECT 'Staging', 'STG_MonthlyDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_MonthlyDeclaration
UNION ALL SELECT 'Staging', 'STG_AnnualDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_AnnualDeclaration
UNION ALL SELECT 'Staging', 'STG_Payment', COUNT(*) FROM DV_Staging.dbo.STG_Payment
UNION ALL SELECT 'Bronze', 'HUB_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer
UNION ALL SELECT 'Bronze', 'HUB_Declaration', COUNT(*) FROM DV_Bronze.dbo.HUB_Declaration
UNION ALL SELECT 'Bronze', 'HUB_Payment', COUNT(*) FROM DV_Bronze.dbo.HUB_Payment
UNION ALL SELECT 'Bronze', 'HUB_Officer', COUNT(*) FROM DV_Bronze.dbo.HUB_Officer
UNION ALL SELECT 'Bronze', 'HUB_Owner', COUNT(*) FROM DV_Bronze.dbo.HUB_Owner
UNION ALL SELECT 'Bronze', 'HUB_Category', COUNT(*) FROM DV_Bronze.dbo.HUB_Category
UNION ALL SELECT 'Bronze', 'HUB_Structure', COUNT(*) FROM DV_Bronze.dbo.HUB_Structure
UNION ALL SELECT 'Bronze', 'HUB_Activity', COUNT(*) FROM DV_Bronze.dbo.HUB_Activity
UNION ALL SELECT 'Bronze', 'HUB_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.HUB_AnnualDecl
UNION ALL SELECT 'Bronze', 'SAT_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL SELECT 'Bronze', 'SAT_MonthlyDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_MonthlyDecl
UNION ALL SELECT 'Bronze', 'SAT_Payment', COUNT(*) FROM DV_Bronze.dbo.SAT_Payment
UNION ALL SELECT 'Bronze', 'SAT_Officer', COUNT(*) FROM DV_Bronze.dbo.SAT_Officer
UNION ALL SELECT 'Bronze', 'SAT_Owner', COUNT(*) FROM DV_Bronze.dbo.SAT_Owner
UNION ALL SELECT 'Bronze', 'SAT_Category', COUNT(*) FROM DV_Bronze.dbo.SAT_Category
UNION ALL SELECT 'Bronze', 'SAT_Structure', COUNT(*) FROM DV_Bronze.dbo.SAT_Structure
UNION ALL SELECT 'Bronze', 'SAT_Activity', COUNT(*) FROM DV_Bronze.dbo.SAT_Activity
UNION ALL SELECT 'Bronze', 'SAT_AnnualDecl', COUNT(*) FROM DV_Bronze.dbo.SAT_AnnualDecl
UNION ALL SELECT 'Bronze', 'LNK_TaxpayerDeclaration', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
UNION ALL SELECT 'Bronze', 'LNK_DeclarationPayment', COUNT(*) FROM DV_Bronze.dbo.LNK_DeclarationPayment
UNION ALL SELECT 'Bronze', 'LNK_TaxpayerOfficer', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOfficer
UNION ALL SELECT 'Bronze', 'LNK_TaxpayerOwner', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerOwner
UNION ALL SELECT 'Bronze', 'LNK_TaxpayerAnnualDecl', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerAnnualDecl
UNION ALL SELECT 'Silver', 'PIT_Taxpayer', COUNT(*) FROM DV_Silver.dbo.PIT_Taxpayer
UNION ALL SELECT 'Silver', 'PIT_Declaration', COUNT(*) FROM DV_Silver.dbo.PIT_Declaration
UNION ALL SELECT 'Silver', 'PIT_Payment', COUNT(*) FROM DV_Silver.dbo.PIT_Payment
UNION ALL SELECT 'Silver', 'BRG_Taxpayer_Owner', COUNT(*) FROM DV_Silver.dbo.BRG_Taxpayer_Owner
UNION ALL SELECT 'Silver', 'BUS_ComplianceScore', COUNT(*) FROM DV_Silver.dbo.BUS_ComplianceScore
UNION ALL SELECT 'Silver', 'BUS_MonthlyMetrics', COUNT(*) FROM DV_Silver.dbo.BUS_MonthlyMetrics
UNION ALL SELECT 'Gold', 'DIM_Taxpayer', COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer
UNION ALL SELECT 'Gold', 'DIM_Officer', COUNT(*) FROM DV_Gold.dbo.DIM_Officer
UNION ALL SELECT 'Gold', 'DIM_Category', COUNT(*) FROM DV_Gold.dbo.DIM_Category
UNION ALL SELECT 'Gold', 'DIM_Structure', COUNT(*) FROM DV_Gold.dbo.DIM_Structure
UNION ALL SELECT 'Gold', 'DIM_Activity', COUNT(*) FROM DV_Gold.dbo.DIM_Activity
UNION ALL SELECT 'Gold', 'DIM_PaymentMethod', COUNT(*) FROM DV_Gold.dbo.DIM_PaymentMethod
UNION ALL SELECT 'Gold', 'DIM_Status', COUNT(*) FROM DV_Gold.dbo.DIM_Status
UNION ALL SELECT 'Gold', 'FACT_MonthlyDeclaration', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration
UNION ALL SELECT 'Gold', 'FACT_Payment', COUNT(*) FROM DV_Gold.dbo.FACT_Payment
UNION ALL SELECT 'Gold', 'FACT_MonthlySnapshot', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlySnapshot
UNION ALL SELECT 'Gold', 'FACT_DeclarationLifecycle', COUNT(*) FROM DV_Gold.dbo.FACT_DeclarationLifecycle
ORDER BY 1, 2;

PRINT '';
PRINT '═══ PHASE 2 COMPLETE ═══';
PRINT 'RECORD results from grids [2a] through [2f].';
PRINT '';
PRINT 'BEFORE Phase 3:';
PRINT '  1. Run 12_IncrementalTest_SourceChanges.sql';
PRINT '  2. Run Master_Complete_Pipeline.dtsx again (it should detect INCREMENTAL)';
PRINT '  3. After SSIS finishes, run Phase 3 below';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  PHASE 3: INCREMENTAL LOAD BENCHMARK (Table 5.3 — Incremental Column)   ║
-- ║  Run AFTER 12_IncrementalTest + Master_Complete_Pipeline (incremental)   ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

SET NOCOUNT ON;
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  PHASE 3: INCREMENTAL LOAD BENCHMARK';
PRINT '  Time: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- 3a. Incremental Batch result
PRINT '--- [3a] Incremental Load Batch Result ---';
SELECT
    b.BatchID,
    b.BatchType,
    b.BatchStatus,
    b.BatchStartTime,
    b.BatchEndTime,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS [Duration_Sec_Int],
    CAST(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS DECIMAL(10,1)) AS [Duration_Sec]
FROM ETL_Control.dbo.ETL_BatchLog b
WHERE b.BatchType = 'Incremental'
ORDER BY b.BatchID DESC;

-- 3b. Per-Layer timing — TABLE 5.3 Incremental column
PRINT '';
PRINT '--- [3b] Per-Layer Timing (TABLE 5.3 — Incremental Load) ---';

DECLARE @IncrBatchID INT = (
    SELECT TOP 1 BatchID FROM ETL_Control.dbo.ETL_BatchLog 
    WHERE BatchType = 'Incremental' AND BatchStatus = 'Success'
    ORDER BY BatchID DESC
);

PRINT '  Using BatchID: ' + CAST(ISNULL(@IncrBatchID, 0) AS VARCHAR);

SELECT
    CASE
        WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
        WHEN StepName LIKE 'Load_HUB_%' OR StepName LIKE 'Load_SAT_%' 
             OR StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
        WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
             OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
        WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
        ELSE '0. Other'
    END AS [Layer],
    COUNT(*) AS [Steps],
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS [Incr_Load_Sec],
    SUM(RecordsProcessed) AS [Records_Processed]
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @IncrBatchID
GROUP BY CASE
    WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
    WHEN StepName LIKE 'Load_HUB_%' OR StepName LIKE 'Load_SAT_%' 
         OR StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
    WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
         OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
    WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
    ELSE '0. Other'
END
ORDER BY [Layer];

-- 3c. Incremental pipeline totals
PRINT '';
PRINT '--- [3c] Incremental Load Pipeline Totals ---';
SELECT
    COUNT(*) AS [Total_Steps],
    SUM(CASE WHEN StepStatus = 'Success' THEN 1 ELSE 0 END) AS [Success_Steps],
    SUM(CASE WHEN StepStatus = 'Failed' THEN 1 ELSE 0 END) AS [Failed_Steps],
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS [Total_Sec],
    SUM(RecordsProcessed) AS [Total_Records],
    CASE 
        WHEN SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) > 0
        THEN CAST(SUM(RecordsProcessed) * 1000.0 / SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) AS INT)
        ELSE 0
    END AS [Records_Per_Sec]
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @IncrBatchID;

-- 3d. Incremental change detection evidence
PRINT '';
PRINT '--- [3d] Incremental Change Detection (SAT Versioning) ---';
SELECT 
    'SAT_Taxpayer' AS [Satellite],
    COUNT(*) AS [Total_Rows],
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END) AS [Current_Versions],
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END) AS [Historic_Versions]
FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL
SELECT 'SAT_Officer', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END)
FROM DV_Bronze.dbo.SAT_Officer
UNION ALL
SELECT 'SAT_Category', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END)
FROM DV_Bronze.dbo.SAT_Category
UNION ALL
SELECT 'SAT_Owner', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END)
FROM DV_Bronze.dbo.SAT_Owner
UNION ALL
SELECT 'SAT_MonthlyDecl', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END)
FROM DV_Bronze.dbo.SAT_MonthlyDecl
UNION ALL
SELECT 'SAT_Payment', COUNT(*),
    SUM(CASE WHEN SAT_EndDate IS NULL THEN 1 ELSE 0 END),
    SUM(CASE WHEN SAT_EndDate IS NOT NULL THEN 1 ELSE 0 END)
FROM DV_Bronze.dbo.SAT_Payment;

PRINT '';
PRINT '═══ PHASE 3 COMPLETE ═══';
PRINT 'RECORD results from grids [3a] through [3d].';
PRINT 'Proceed to Phase 4 for Query Performance.';
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  PHASE 4: QUERY PERFORMANCE BENCHMARK (Table 5.4)                        ║
-- ║                                                                          ║
-- ║  IMPORTANT: Run each query block SEPARATELY (not all at once).           ║
-- ║  For each query:                                                         ║
-- ║    1. Run it 10 times                                                    ║
-- ║    2. Discard the FIRST run (cold cache)                                 ║
-- ║    3. Record the elapsed time from Messages tab for runs 2-10            ║
-- ║    4. Calculate average of 9 runs                                        ║
-- ║                                                                          ║
-- ║  TIP: After each block, check Messages tab for:                          ║
-- ║       "SQL Server Execution Times: CPU time = X ms, elapsed time = Y ms" ║
-- ║       Record the "elapsed time" value.                                   ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────
-- 4a. SIMPLE QUERY — GOLD LAYER (2-table join)
-- Expected: ~24ms (thesis value)
-- Run this block 10 times. Record elapsed time from runs 2-10.
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT
    c.CategoryName,
    SUM(f.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Gold.dbo.FACT_MonthlyDeclaration f
JOIN DV_Gold.dbo.DIM_Category c ON f.DIM_Category_SK = c.DIM_Category_SK
GROUP BY c.CategoryName
ORDER BY TotalTax DESC;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Run 1(discard)=___ms, Run2=___ms, Run3=___ms, Run4=___ms, 
-- Run5=___ms, Run6=___ms, Run7=___ms, Run8=___ms, Run9=___ms, Run10=___ms
-- AVERAGE (runs 2-10): ___ms


-- ─────────────────────────────────────────────────────────────
-- 4b. SIMPLE QUERY — RAW VAULT (multi-join Bronze)
-- Expected: ~199ms (thesis value)
-- Run this block 10 times. Record elapsed time from runs 2-10.
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT
    sc.CategoryName,
    SUM(sd.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Bronze.dbo.HUB_Declaration hd
JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
    ON hd.HUB_Declaration_HK = sd.HUB_Declaration_HK AND sd.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration ltd
    ON hd.HUB_Declaration_HK = ltd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.HUB_Taxpayer ht
    ON ltd.HUB_Taxpayer_HK = ht.HUB_Taxpayer_HK
JOIN DV_Bronze.dbo.SAT_Taxpayer st
    ON ht.HUB_Taxpayer_HK = st.HUB_Taxpayer_HK AND st.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.HUB_Category hc
    ON HASHBYTES('SHA2_256', CAST(st.CategoryID AS VARCHAR(20))) = hc.HUB_Category_HK
JOIN DV_Bronze.dbo.SAT_Category sc
    ON hc.HUB_Category_HK = sc.HUB_Category_HK AND sc.SAT_EndDate IS NULL
GROUP BY sc.CategoryName
ORDER BY TotalTax DESC;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Run 1(discard)=___ms, Run2=___ms, Run3=___ms, Run4=___ms, 
-- Run5=___ms, Run6=___ms, Run7=___ms, Run8=___ms, Run9=___ms, Run10=___ms
-- AVERAGE (runs 2-10): ___ms
-- SPEEDUP: Raw Vault avg / Gold avg = ___x


-- ─────────────────────────────────────────────────────────────
-- 4c. MEDIUM QUERY — GOLD LAYER (4-table join, TOP 10)
-- Expected: ~18ms (thesis value)
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT TOP 10
    t.TaxID, t.LegalBusinessName,
    c.CategoryName,
    SUM(f.PaymentAmount) AS TotalPayment
FROM DV_Gold.dbo.FACT_Payment f
JOIN DV_Gold.dbo.DIM_Taxpayer t ON f.DIM_Taxpayer_SK = t.DIM_Taxpayer_SK
JOIN DV_Gold.dbo.DIM_Category c ON t.DIM_Category_SK = c.DIM_Category_SK
JOIN DV_Gold.dbo.DIM_PaymentMethod pm ON f.DIM_PaymentMethod_SK = pm.DIM_PaymentMethod_SK
WHERE t.IsCurrent = 1
GROUP BY t.TaxID, t.LegalBusinessName, c.CategoryName
ORDER BY TotalPayment DESC;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Average (runs 2-10): ___ms


-- ─────────────────────────────────────────────────────────────
-- 4d. MEDIUM QUERY — RAW VAULT
-- Expected: ~123ms (thesis value)
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT TOP 10
    st.LegalBusinessName,
    sc.CategoryName,
    SUM(sp.PaymentAmount) AS TotalPayment
FROM DV_Bronze.dbo.HUB_Payment hp
JOIN DV_Bronze.dbo.SAT_Payment sp
    ON hp.HUB_Payment_HK = sp.HUB_Payment_HK AND sp.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.LNK_DeclarationPayment ldp
    ON hp.HUB_Payment_HK = ldp.HUB_Payment_HK
JOIN DV_Bronze.dbo.HUB_Declaration hd
    ON ldp.HUB_Declaration_HK = hd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration ltd
    ON hd.HUB_Declaration_HK = ltd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.HUB_Taxpayer ht
    ON ltd.HUB_Taxpayer_HK = ht.HUB_Taxpayer_HK
JOIN DV_Bronze.dbo.SAT_Taxpayer st
    ON ht.HUB_Taxpayer_HK = st.HUB_Taxpayer_HK AND st.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.HUB_Category hc
    ON HASHBYTES('SHA2_256', CAST(st.CategoryID AS VARCHAR(20))) = hc.HUB_Category_HK
JOIN DV_Bronze.dbo.SAT_Category sc
    ON hc.HUB_Category_HK = sc.HUB_Category_HK AND sc.SAT_EndDate IS NULL
GROUP BY st.LegalBusinessName, sc.CategoryName
ORDER BY TotalPayment DESC;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Average (runs 2-10): ___ms
-- SPEEDUP: Raw Vault avg / Gold avg = ___x


-- ─────────────────────────────────────────────────────────────
-- 4e. COMPLEX QUERY — GOLD LAYER (4-table join, full aggregation)
-- Expected: ~771ms (thesis value)
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT
    d.CalendarYear, d.MonthNumber, d.MonthName,
    c.CategoryName,
    s.StatusDescription,
    SUM(f.GrossRevenue) AS TotalRevenue,
    SUM(f.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Gold.dbo.FACT_MonthlyDeclaration f
JOIN DV_Gold.dbo.DIM_Date d ON f.DIM_Date_SK = d.DIM_Date_SK
JOIN DV_Gold.dbo.DIM_Category c ON f.DIM_Category_SK = c.DIM_Category_SK
JOIN DV_Gold.dbo.DIM_Status s ON f.DIM_Status_SK = s.DIM_Status_SK
GROUP BY d.CalendarYear, d.MonthNumber, d.MonthName,
         c.CategoryName, s.StatusDescription
ORDER BY d.CalendarYear, d.MonthNumber, c.CategoryName;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Average (runs 2-10): ___ms


-- ─────────────────────────────────────────────────────────────
-- 4f. COMPLEX QUERY — RAW VAULT
-- Expected: ~798ms (thesis value)
-- ─────────────────────────────────────────────────────────────
SET STATISTICS TIME ON;
GO

SELECT
    sd.DeclarationYear AS CalendarYear,
    sd.DeclarationMonth AS MonthNumber,
    sc.CategoryName,
    sd.Status AS DeclarationStatus,
    SUM(sd.GrossRevenue) AS TotalRevenue,
    SUM(sd.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Bronze.dbo.HUB_Declaration hd
JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
    ON hd.HUB_Declaration_HK = sd.HUB_Declaration_HK AND sd.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration ltd
    ON hd.HUB_Declaration_HK = ltd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.HUB_Taxpayer ht
    ON ltd.HUB_Taxpayer_HK = ht.HUB_Taxpayer_HK
JOIN DV_Bronze.dbo.SAT_Taxpayer st
    ON ht.HUB_Taxpayer_HK = st.HUB_Taxpayer_HK AND st.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.HUB_Category hc
    ON HASHBYTES('SHA2_256', CAST(st.CategoryID AS VARCHAR(20))) = hc.HUB_Category_HK
JOIN DV_Bronze.dbo.SAT_Category sc
    ON hc.HUB_Category_HK = sc.HUB_Category_HK AND sc.SAT_EndDate IS NULL
GROUP BY sd.DeclarationYear, sd.DeclarationMonth,
         sc.CategoryName, sd.Status
ORDER BY CalendarYear, MonthNumber, sc.CategoryName;
GO

SET STATISTICS TIME OFF;
GO
-- RECORD: Average (runs 2-10): ___ms
-- SPEEDUP: Raw Vault avg / Gold avg = ___x


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  PHASE 5: OBJECT COUNT & SSIS PACKAGE VERIFICATION                       ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

SET NOCOUNT ON;
PRINT '══════════════════════════════════════════════════════════════';
PRINT '  PHASE 5: OBJECT COUNT & SSIS VERIFICATION';
PRINT '  Time: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
PRINT '══════════════════════════════════════════════════════════════';
PRINT '';

-- 5a. Database object count (expected: 67)
PRINT '--- [5a] Database Object Count ---';
SELECT 'TaxSystemDB' AS [Database], 'Source' AS [Layer], COUNT(*) AS [Tables]
FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'spt_%' AND TABLE_NAME NOT LIKE 'MS%'
UNION ALL SELECT 'ETL_Control', 'Control', COUNT(*) FROM ETL_Control.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL SELECT 'DV_Staging', 'Staging', COUNT(*) FROM DV_Staging.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL SELECT 'DV_Bronze', 'Bronze', COUNT(*) FROM DV_Bronze.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL SELECT 'DV_Silver', 'Silver', COUNT(*) FROM DV_Silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL SELECT 'DV_Gold', 'Gold', COUNT(*) FROM DV_Gold.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';

-- 5b. Grand total
SELECT SUM(cnt) AS [Total_Objects] FROM (
    SELECT COUNT(*) AS cnt FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'spt_%' AND TABLE_NAME NOT LIKE 'MS%'
    UNION ALL SELECT COUNT(*) FROM ETL_Control.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Staging.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Bronze.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Gold.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
) t;

-- 5c. SSIS packages (expected: 60)
PRINT '';
PRINT '--- [5c] SSIS Package Inventory ---';
SELECT
    pk.name AS [Package_Name],
    CASE
        WHEN pk.name LIKE 'Master_%' THEN 'Master'
        WHEN pk.name LIKE '%_All_%' OR pk.name LIKE '%_Load_All%' THEN 'Orchestrator'
        ELSE 'Child'
    END AS [Type]
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
ORDER BY [Type], pk.name;

-- 5d. SSIS package count by type
PRINT '';
PRINT '--- [5d] SSIS Package Summary ---';
SELECT
    CASE
        WHEN pk.name LIKE 'Master_%' THEN 'Master'
        WHEN pk.name LIKE '%_All_%' OR pk.name LIKE '%_Load_All%' THEN 'Orchestrator'
        ELSE 'Child'
    END AS [Type],
    COUNT(*) AS [Count]
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id
GROUP BY CASE
    WHEN pk.name LIKE 'Master_%' THEN 'Master'
    WHEN pk.name LIKE '%_All_%' OR pk.name LIKE '%_Load_All%' THEN 'Orchestrator'
    ELSE 'Child'
END;

-- 5e. Total SSIS packages
SELECT COUNT(*) AS [Total_SSIS_Packages]
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id;
GO


-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║  RESULTS COLLECTION TEMPLATE                                             ║
-- ║  Fill in actual numbers after running each phase, then bring back        ║
-- ║  to Claude to update thesis Chapter 5 and companion documents.           ║
-- ╚════════════════════════════════════════════════════════════════════════════╝

/*
═══════════════════════════════════════════════════════════════════
  ACTUAL BENCHMARK RESULTS — Fill in after running on GCP VM
  Date Run: ____________________
  Environment: GCP e2-standard-4, 16GB RAM, SQL Server 2025
═══════════════════════════════════════════════════════════════════

--- TABLE 5.3: ETL Load Time Benchmarks ---

FULL LOAD:
  Staging:  Steps=___, Time=___s, Records=___
  Bronze:   Steps=___, Time=___s, Records=___
  Silver:   Steps=___, Time=___s, Records=___
  Gold:     Steps=___, Time=___s, Records=___
  TOTAL:    Steps=___, Time=___s, Records=___, Rec/Sec=___

INCREMENTAL LOAD:
  Staging:  Steps=___, Time=___s, Records=___
  Bronze:   Steps=___, Time=___s, Records=___
  Silver:   Steps=___, Time=___s, Records=___
  Gold:     Steps=___, Time=___s, Records=___
  TOTAL:    Steps=___, Time=___s, Records=___, Rec/Sec=___


--- TABLE 5.4: Query Performance Benchmarks ---

SIMPLE (Total tax by category):
  Gold Layer:  ___ms (avg of 9 runs)
  Raw Vault:   ___ms (avg of 9 runs)
  Speedup:     ___x

MEDIUM (Top 10 by payment):
  Gold Layer:  ___ms (avg of 9 runs)
  Raw Vault:   ___ms (avg of 9 runs)
  Speedup:     ___x

COMPLEX (Monthly revenue trend):
  Gold Layer:  ___ms (avg of 9 runs)
  Raw Vault:   ___ms (avg of 9 runs)
  Speedup:     ___x


--- OBJECT COUNTS ---

Database Objects:
  TaxSystemDB:  ___
  ETL_Control:  ___
  DV_Staging:   ___
  DV_Bronze:    ___
  DV_Silver:    ___
  DV_Gold:      ___
  TOTAL:        ___

SSIS Packages:
  Master:       ___
  Orchestrator: ___
  Child:        ___
  TOTAL:        ___


--- SAT VERSIONING (After Incremental) ---
  SAT_Taxpayer:    Current=___, Historic=___
  SAT_Officer:     Current=___, Historic=___
  SAT_Category:    Current=___, Historic=___
  SAT_Owner:       Current=___, Historic=___
  SAT_MonthlyDecl: Current=___, Historic=___
  SAT_Payment:     Current=___, Historic=___

═══════════════════════════════════════════════════════════════════
*/
