-- ============================================================================
-- PERFORMANCE BENCHMARKS VERIFICATION SCRIPT
-- Data Vault 2.0 Tax System Data Warehouse
-- Mr. Sot So — RUPP 2026
-- ============================================================================
-- Schema alignment verified against actual database (2026-02-25):
--   ETL_Control: ETL_BatchLog, ETL_StepLog, ETL_Process
--   DV_Bronze:   HUB_*, SAT_*, LNK_* tables
--   DV_Gold:     DIM_* (IsActive/IsCurrent), FACT_* tables
-- ============================================================================
-- Run this script in SSMS
-- Screenshot each result for your Performance Benchmarks Verification document
-- ============================================================================


-- ================================================================
--  SECTION 1: ETL LOAD TIME BENCHMARKS (Section 5.3.1)
-- ================================================================

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 1: ETL LOAD TIME BENCHMARKS';
PRINT '================================================================';

-- ────────────────────────────────────────────
-- 1.1 Batch-Level Summary
-- Shows total pipeline duration for Full and Incremental loads
-- ────────────────────────────────────────────
PRINT '--- 1.1 Batch-Level Summary ---';
SELECT
    b.BatchID,
    p.ProcessName,
    b.BatchStatus,
    b.RecordsProcessed,
    DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS [Duration (sec)],
    CAST(b.RecordsProcessed * 1.0 / NULLIF(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0, 0) AS INT) AS [Records/Second]
FROM ETL_Control.dbo.ETL_BatchLog b
JOIN ETL_Control.dbo.ETL_Process p ON b.ProcessID = p.ProcessID
ORDER BY b.BatchID;
-- Expected: BatchID 1 (Full Load) ~120.9s, 336,677 records, ~2,785 rec/s
--           BatchID 2 (Incremental) ~93.6s, 206,780 records, ~2,209 rec/s


-- ────────────────────────────────────────────
-- 1.2 Step-Level Grouped by Layer
-- Shows per-layer breakdown for each batch
-- ────────────────────────────────────────────
PRINT '--- 1.2 Step-Level by Layer ---';
SELECT
    CASE
        WHEN s.StepName LIKE 'Load_STG_%' THEN '1. Staging'
        WHEN s.StepName LIKE 'Load_HUB_%'
             OR s.StepName LIKE 'Load_SAT_%' OR s.StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
        WHEN s.StepName LIKE 'Load_PIT_%'
             OR s.StepName LIKE 'Load_BRG_%' OR s.StepName LIKE 'Load_BUS_%' THEN '3. Silver'
        WHEN s.StepName LIKE 'Load_DIM_%'
             OR s.StepName LIKE 'Load_FACT_%' THEN '4. Gold'
        ELSE '5. Other'
    END AS [Layer],
    COUNT(*) AS [Step Count],
    SUM(s.RecordsProcessed) AS [Total Records],
    CAST(SUM(DATEDIFF(MILLISECOND, s.StepStartTime, s.StepEndTime)) / 1000.0
         AS DECIMAL(10,1)) AS [Total Duration (sec)]
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = 1  -- Change to 2 for incremental
GROUP BY CASE
    WHEN s.StepName LIKE 'Load_STG_%' THEN '1. Staging'
    WHEN s.StepName LIKE 'Load_HUB_%'
         OR s.StepName LIKE 'Load_SAT_%' OR s.StepName LIKE 'Load_LNK_%' THEN '2. Bronze'
    WHEN s.StepName LIKE 'Load_PIT_%'
         OR s.StepName LIKE 'Load_BRG_%' OR s.StepName LIKE 'Load_BUS_%' THEN '3. Silver'
    WHEN s.StepName LIKE 'Load_DIM_%'
         OR s.StepName LIKE 'Load_FACT_%' THEN '4. Gold'
    ELSE '5. Other'
END
ORDER BY [Layer];
-- Expected (BatchID 1): Staging=22.8s, Bronze=39.5s, Silver=3.4s, Gold=11.0s


-- ────────────────────────────────────────────
-- 1.3 Individual Step Breakdown (All 49 Steps)
-- ────────────────────────────────────────────
PRINT '--- 1.3 All 49 Steps ---';
SELECT
    ROW_NUMBER() OVER (ORDER BY s.StepStartTime) AS [Step #],
    s.StepName,
    s.StepStartTime,
    s.StepEndTime,
    DATEDIFF(MILLISECOND, s.StepStartTime, s.StepEndTime) / 1000.0 AS [Duration (sec)],
    s.RecordsProcessed,
    s.StepStatus,
    s.ErrorMessage
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = 1  -- Change to 2 for incremental
ORDER BY s.StepStartTime;
-- Expected: 49 rows, all StepStatus = 'Success', ErrorMessage = NULL


-- ────────────────────────────────────────────
-- 1.4 Record Count Verification
-- ────────────────────────────────────────────
PRINT '--- 1.4 Record Counts by Layer ---';

SELECT 'Bronze (Hubs)' AS [Layer], (SELECT SUM(p.rows) FROM DV_Bronze.sys.partitions p
    JOIN DV_Bronze.sys.tables t ON p.object_id = t.object_id
    WHERE t.name LIKE 'HUB_%' AND p.index_id IN (0,1)) AS [Record Count]
UNION ALL
SELECT 'Bronze (SATs)', (SELECT SUM(p.rows) FROM DV_Bronze.sys.partitions p
    JOIN DV_Bronze.sys.tables t ON p.object_id = t.object_id
    WHERE t.name LIKE 'SAT_%' AND p.index_id IN (0,1))
UNION ALL
SELECT 'Bronze (Links)', (SELECT SUM(p.rows) FROM DV_Bronze.sys.partitions p
    JOIN DV_Bronze.sys.tables t ON p.object_id = t.object_id
    WHERE t.name LIKE 'LNK_%' AND p.index_id IN (0,1))
UNION ALL
SELECT 'Silver', (SELECT SUM(p.rows) FROM DV_Silver.sys.partitions p
    JOIN DV_Silver.sys.tables t ON p.object_id = t.object_id
    AND p.index_id IN (0,1))
UNION ALL
SELECT 'Gold', (SELECT SUM(p.rows) FROM DV_Gold.sys.partitions p
    JOIN DV_Gold.sys.tables t ON p.object_id = t.object_id
    WHERE (t.name LIKE 'DIM_%' OR t.name LIKE 'FACT_%')
    AND p.index_id IN (0,1));
-- Note: Staging shows NULL because STG tables are truncated before each load


-- ================================================================
--  SECTION 2: QUERY PERFORMANCE BENCHMARKS (Section 5.3.2)
-- ================================================================
-- Run each query multiple times. First execution excluded (cold cache).
-- Capture elapsed time from SET STATISTICS TIME ON.
-- ================================================================

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 2: QUERY PERFORMANCE BENCHMARKS';
PRINT '================================================================';

-- ────────────────────────────────────────────
-- 2.1 SIMPLE: Total Tax by Category
-- Gold:      FACT_MonthlyDeclaration → DIM_Category (1 join)
-- Raw Vault: SAT_MonthlyDecl → LNK_TaxpayerDeclaration → SAT_Taxpayer
--            → HUB_Category → SAT_Category (5 joins)
-- Measured: Gold ~24ms, Raw Vault ~199ms (8.3x speedup)
-- ────────────────────────────────────────────

-- 2.1a Gold: Simple Query
PRINT '--- 2.1a Gold: Simple Query ---';
USE DV_Gold;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT
    d.CategoryName,
    SUM(f.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM FACT_MonthlyDeclaration f
INNER JOIN DIM_Category d ON f.DIM_Category_SK = d.DIM_Category_SK
WHERE d.IsActive = 1
GROUP BY d.CategoryName
ORDER BY TotalTax DESC;
GO

-- 2.1b Raw Vault: Simple Query (5 joins)
-- Path: HUB_Declaration → SAT_MonthlyDecl → LNK_TaxpayerDeclaration
--       → SAT_Taxpayer (CategoryID) → HUB_Category → SAT_Category
PRINT '--- 2.1b Raw Vault: Simple Query ---';
USE DV_Bronze;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT
    cat_sat.CategoryName,
    SUM(decl_sat.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM HUB_Declaration hub_md
INNER JOIN SAT_MonthlyDecl decl_sat
    ON hub_md.HUB_Declaration_HK = decl_sat.HUB_Declaration_HK
    AND decl_sat.SAT_EndDate IS NULL
INNER JOIN LNK_TaxpayerDeclaration lnk
    ON hub_md.HUB_Declaration_HK = lnk.HUB_Declaration_HK
INNER JOIN SAT_Taxpayer tp_sat
    ON lnk.HUB_Taxpayer_HK = tp_sat.HUB_Taxpayer_HK
    AND tp_sat.SAT_EndDate IS NULL
INNER JOIN HUB_Category hub_cat
    ON tp_sat.CategoryID = hub_cat.CategoryID
INNER JOIN SAT_Category cat_sat
    ON hub_cat.HUB_Category_HK = cat_sat.HUB_Category_HK
    AND cat_sat.SAT_EndDate IS NULL
GROUP BY cat_sat.CategoryName
ORDER BY TotalTax DESC;
GO


-- ────────────────────────────────────────────
-- 2.2 MEDIUM: Top 10 Taxpayers by Payment Amount
-- Gold:      FACT_Payment → DIM_Taxpayer (1 join)
-- Raw Vault: HUB_Payment → SAT_Payment → LNK_DeclarationPayment
--            → LNK_TaxpayerDeclaration → HUB_Taxpayer → SAT_Taxpayer (6 joins)
-- Measured: Gold ~18ms, Raw Vault ~123ms (6.8x speedup)
-- ────────────────────────────────────────────

-- 2.2a Gold: Medium Query
PRINT '--- 2.2a Gold: Medium Query ---';
USE DV_Gold;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT TOP 10
    t.TradingName,
    t.TaxID,
    SUM(f.PaymentAmount) AS TotalPayments,
    COUNT(*) AS PaymentCount
FROM FACT_Payment f
INNER JOIN DIM_Taxpayer t ON f.DIM_Taxpayer_SK = t.DIM_Taxpayer_SK
WHERE t.IsCurrent = 1
GROUP BY t.TradingName, t.TaxID
ORDER BY TotalPayments DESC;
GO

-- 2.2b Raw Vault: Medium Query (6 joins)
-- Path: HUB_Payment → SAT_Payment → LNK_DeclarationPayment
--       → LNK_TaxpayerDeclaration → HUB_Taxpayer → SAT_Taxpayer
PRINT '--- 2.2b Raw Vault: Medium Query ---';
USE DV_Bronze;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT TOP 10
    tp_sat.TradingName,
    hub_tp.TaxID,
    SUM(pay_sat.PaymentAmount) AS TotalPayments,
    COUNT(*) AS PaymentCount
FROM HUB_Payment hub_pay
INNER JOIN SAT_Payment pay_sat
    ON hub_pay.HUB_Payment_HK = pay_sat.HUB_Payment_HK
    AND pay_sat.SAT_EndDate IS NULL
INNER JOIN LNK_DeclarationPayment lnk_dp
    ON hub_pay.HUB_Payment_HK = lnk_dp.HUB_Payment_HK
INNER JOIN LNK_TaxpayerDeclaration lnk_td
    ON lnk_dp.HUB_Declaration_HK = lnk_td.HUB_Declaration_HK
INNER JOIN HUB_Taxpayer hub_tp
    ON lnk_td.HUB_Taxpayer_HK = hub_tp.HUB_Taxpayer_HK
INNER JOIN SAT_Taxpayer tp_sat
    ON hub_tp.HUB_Taxpayer_HK = tp_sat.HUB_Taxpayer_HK
    AND tp_sat.SAT_EndDate IS NULL
GROUP BY tp_sat.TradingName, hub_tp.TaxID
ORDER BY TotalPayments DESC;
GO


-- ────────────────────────────────────────────
-- 2.3 COMPLEX: Monthly Revenue Trend by Category
-- Gold:      FACT_MonthlyDeclaration → DIM_Taxpayer + DIM_Category + DIM_Status (3 joins)
-- Raw Vault: HUB_Declaration → SAT_MonthlyDecl → LNK_TaxpayerDeclaration
--            → HUB_Taxpayer → SAT_Taxpayer → HUB_Category → SAT_Category (7 joins)
-- Measured: Gold ~771ms, Raw Vault ~798ms (1.03x speedup)
-- Note: Both approaches are comparable for this large-result complex query
-- ────────────────────────────────────────────

-- 2.3a Gold: Complex Query
PRINT '--- 2.3a Gold: Complex Query ---';
USE DV_Gold;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT
    t.TaxID,
    c.CategoryName,
    f.DeclarationYear,
    f.DeclarationMonth,
    SUM(f.TaxAmount) AS MonthlyTax,
    SUM(f.PenaltyAmount) AS MonthlyPenalty,
    SUM(f.TotalAmount) AS MonthlyTotal,
    COUNT(*) AS DeclarationCount
FROM FACT_MonthlyDeclaration f
INNER JOIN DIM_Taxpayer t ON f.DIM_Taxpayer_SK = t.DIM_Taxpayer_SK
INNER JOIN DIM_Category c ON f.DIM_Category_SK = c.DIM_Category_SK
INNER JOIN DIM_Status s ON f.DIM_Status_SK = s.DIM_Status_SK
WHERE t.IsCurrent = 1
  AND c.IsActive = 1
GROUP BY t.TaxID, c.CategoryName, f.DeclarationYear, f.DeclarationMonth
ORDER BY c.CategoryName, f.DeclarationYear, f.DeclarationMonth;
GO

-- 2.3b Raw Vault: Complex Query (7 joins)
-- Path: HUB_Declaration → SAT_MonthlyDecl → LNK_TaxpayerDeclaration
--       → HUB_Taxpayer → SAT_Taxpayer → HUB_Category → SAT_Category
PRINT '--- 2.3b Raw Vault: Complex Query ---';
USE DV_Bronze;
GO
SET STATISTICS TIME ON;
SET STATISTICS IO ON;
GO

SELECT
    hub_tp.TaxID,
    cat_sat.CategoryName,
    decl_sat.DeclarationYear,
    decl_sat.DeclarationMonth,
    SUM(decl_sat.TaxAmount) AS MonthlyTax,
    SUM(decl_sat.PenaltyAmount) AS MonthlyPenalty,
    SUM(decl_sat.TotalAmount) AS MonthlyTotal,
    COUNT(*) AS DeclarationCount
FROM HUB_Declaration hub_md
INNER JOIN SAT_MonthlyDecl decl_sat
    ON hub_md.HUB_Declaration_HK = decl_sat.HUB_Declaration_HK
    AND decl_sat.SAT_EndDate IS NULL
INNER JOIN LNK_TaxpayerDeclaration lnk_td
    ON hub_md.HUB_Declaration_HK = lnk_td.HUB_Declaration_HK
INNER JOIN HUB_Taxpayer hub_tp
    ON lnk_td.HUB_Taxpayer_HK = hub_tp.HUB_Taxpayer_HK
INNER JOIN SAT_Taxpayer tp_sat
    ON hub_tp.HUB_Taxpayer_HK = tp_sat.HUB_Taxpayer_HK
    AND tp_sat.SAT_EndDate IS NULL
INNER JOIN HUB_Category hub_cat
    ON tp_sat.CategoryID = hub_cat.CategoryID
INNER JOIN SAT_Category cat_sat
    ON hub_cat.HUB_Category_HK = cat_sat.HUB_Category_HK
    AND cat_sat.SAT_EndDate IS NULL
GROUP BY hub_tp.TaxID, cat_sat.CategoryName, decl_sat.DeclarationYear, decl_sat.DeclarationMonth
ORDER BY cat_sat.CategoryName, decl_sat.DeclarationYear, decl_sat.DeclarationMonth;
GO

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
GO


-- ================================================================
--  SECTION 3: SCALABILITY ANALYSIS (Section 5.3.3)
-- ================================================================

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 3: SCALABILITY ANALYSIS';
PRINT '================================================================';

-- ────────────────────────────────────────────
-- 3.1 Database Sizes
-- ────────────────────────────────────────────
PRINT '--- 3.1 Database Sizes ---';
SELECT
    DB_NAME(database_id) AS DatabaseName,
    CAST(SUM(size) * 8.0 / 1024 AS DECIMAL(10,1)) AS [Size (MB)]
FROM sys.master_files
WHERE DB_NAME(database_id) IN ('TaxSystemDB','ETL_Control','DV_Staging','DV_Bronze','DV_Silver','DV_Gold')
GROUP BY DB_NAME(database_id)
ORDER BY [Size (MB)] DESC;

-- ────────────────────────────────────────────
-- 3.2 Object Counts per Layer
-- ────────────────────────────────────────────
PRINT '--- 3.2 Object Counts ---';
SELECT 'Staging' AS Layer, COUNT(*) AS [Table Count]
FROM DV_Staging.sys.tables WHERE SCHEMA_NAME(schema_id) = 'stg'
UNION ALL
SELECT 'Bronze (Hubs)', COUNT(*) FROM DV_Bronze.sys.tables WHERE name LIKE 'HUB_%'
UNION ALL
SELECT 'Bronze (SATs)', COUNT(*) FROM DV_Bronze.sys.tables WHERE name LIKE 'SAT_%'
UNION ALL
SELECT 'Bronze (Links)', COUNT(*) FROM DV_Bronze.sys.tables WHERE name LIKE 'LNK_%'
UNION ALL
SELECT 'Silver', COUNT(*) FROM DV_Silver.sys.tables
UNION ALL
SELECT 'Gold (DIMs)', COUNT(*) FROM DV_Gold.sys.tables WHERE name LIKE 'DIM_%'
UNION ALL
SELECT 'Gold (FACTs)', COUNT(*) FROM DV_Gold.sys.tables WHERE name LIKE 'FACT_%';
-- Expected: 9 STG, 9 HUB, 10 SAT, 5 LNK, 6 Silver, 7 DIM, 4 FACT = 50 tables

-- ────────────────────────────────────────────
-- 3.3 Stored Procedure Count
-- ────────────────────────────────────────────
PRINT '--- 3.3 Stored Procedures ---';
SELECT
    DB_NAME() AS [Database],
    COUNT(*) AS [SP Count]
FROM ETL_Control.sys.procedures
UNION ALL
SELECT 'DV_Staging', COUNT(*) FROM DV_Staging.sys.procedures
UNION ALL
SELECT 'DV_Gold', COUNT(*) FROM DV_Gold.sys.procedures;
-- Expected: 15 total (12 ETL_Control + 3 Staging helpers)

-- ────────────────────────────────────────────
-- 3.4 ETL Throughput Summary
-- ────────────────────────────────────────────
PRINT '--- 3.4 Throughput ---';
SELECT
    b.BatchID,
    p.ProcessName,
    b.BatchStatus,
    b.RecordsProcessed,
    DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS [Duration (sec)],
    CAST(b.RecordsProcessed * 1.0 / NULLIF(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0, 0) AS INT) AS [Records/Second]
FROM ETL_Control.dbo.ETL_BatchLog b
JOIN ETL_Control.dbo.ETL_Process p ON b.ProcessID = p.ProcessID
ORDER BY b.BatchID;

PRINT '';
PRINT '================================================================';
PRINT '  BENCHMARK VERIFICATION COMPLETE';
PRINT '================================================================';
