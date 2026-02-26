-- ============================================================================
-- Script: 14_Benchmarks_Verification.sql
-- Purpose: Reproduce all performance benchmarks from Chapter 5 (Tables 5.3-5.5)
--          Run each section in SSMS and capture screenshots for evidence.
-- Author: Mr. Sot So — Data Management Bureau, GDT Cambodia
-- Date: 2026-02-26
-- Reference: Chapter 5, Sections 5.3.1–5.3.4
-- ============================================================================

SET NOCOUNT ON;

PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  PERFORMANCE BENCHMARKS VERIFICATION                       ║';
PRINT '║  Chapter 5 — Tables 5.3, 5.4, 5.5                          ║';
PRINT '║  Generated: ' + CONVERT(VARCHAR(20), GETDATE(), 120) + '              ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';


-- ════════════════════════════════════════════════════════════════
-- BENCHMARK 1: ETL LOAD TIME (Table 5.3)
-- ════════════════════════════════════════════════════════════════
PRINT '═══════════════════════════════════════════════════════════';
PRINT '  BENCHMARK 1: ETL LOAD TIME (Table 5.3)';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

-- 1a. Batch-level summary
PRINT '--- [1a] Batch Log Summary ---';
SELECT
    b.BatchID,
    b.BatchType,
    b.BatchStartTime,
    b.BatchEndTime,
    b.BatchStatus,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS Duration_Sec,
    CAST(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS DECIMAL(10,1)) AS Duration_SecDec
FROM ETL_Control.dbo.ETL_BatchLog b
ORDER BY b.BatchID DESC;

-- 1b. Per-Layer timing (matches Table 5.3 rows)
PRINT '';
PRINT '--- [1b] Per-Layer Timing (Table 5.3) ---';
DECLARE @LastBatchID INT = (SELECT MAX(BatchID) FROM ETL_Control.dbo.ETL_BatchLog);

SELECT
    CASE
        WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
        WHEN StepName LIKE 'Load_HUB_%' THEN '2a. Bronze (Hub)'
        WHEN StepName LIKE 'Load_SAT_%' THEN '2b. Bronze (Sat)'
        WHEN StepName LIKE 'Load_LNK_%' THEN '2c. Bronze (Link)'
        WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
             OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
        WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%'
             THEN '4. Gold'
        ELSE '0. Other'
    END AS Layer,
    COUNT(*) AS Steps,
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS TotalSec,
    SUM(RecordsProcessed) AS TotalRecords
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @LastBatchID
GROUP BY CASE
    WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
    WHEN StepName LIKE 'Load_HUB_%' THEN '2a. Bronze (Hub)'
    WHEN StepName LIKE 'Load_SAT_%' THEN '2b. Bronze (Sat)'
    WHEN StepName LIKE 'Load_LNK_%' THEN '2c. Bronze (Link)'
    WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%'
         OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
    WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%'
         THEN '4. Gold'
    ELSE '0. Other'
END
ORDER BY Layer;

-- 1c. All 49 steps detail
PRINT '';
PRINT '--- [1c] All 49 Individual Steps ---';
SELECT
    ROW_NUMBER() OVER (ORDER BY s.StepLogID) AS [#],
    s.StepName,
    s.StepStatus,
    CAST(DATEDIFF(MILLISECOND, s.StepStartTime, s.StepEndTime) / 1000.0 AS DECIMAL(10,2)) AS Duration_Sec,
    s.RecordsProcessed
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = @LastBatchID
ORDER BY s.StepLogID;

-- 1d. Pipeline totals
PRINT '';
PRINT '--- [1d] Pipeline Totals ---';
SELECT
    COUNT(*) AS TotalSteps,
    SUM(CASE WHEN StepStatus = 'Success' THEN 1 ELSE 0 END) AS SuccessSteps,
    SUM(CASE WHEN StepStatus = 'Failed' THEN 1 ELSE 0 END) AS FailedSteps,
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS TotalSec,
    SUM(RecordsProcessed) AS TotalRecords,
    CAST(SUM(RecordsProcessed) / (SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0) AS INT) AS RecordsPerSec
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @LastBatchID;

-- 1e. Error count
PRINT '';
PRINT '--- [1e] Error Count ---';
SELECT COUNT(*) AS ErrorCount
FROM ETL_Control.dbo.ETL_ErrorLog
WHERE BatchID = @LastBatchID;
GO


-- ════════════════════════════════════════════════════════════════
-- BENCHMARK 2: QUERY PERFORMANCE (Table 5.4)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '  BENCHMARK 2: QUERY PERFORMANCE (Table 5.4)';
PRINT '  NOTE: Enable SET STATISTICS TIME ON to see elapsed time';
PRINT '        in the Messages tab. Run each query 10 times;';
PRINT '        discard first run (cold cache), average remaining 9.';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

-- 2a. SIMPLE QUERY — Gold (2-table join)
PRINT '--- [2a] Simple Query: Gold Layer (Expected: ~24ms) ---';
SET STATISTICS TIME ON;

SELECT
    c.CategoryName,
    SUM(f.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Gold.dbo.FACT_MonthlyDeclaration f
JOIN DV_Gold.dbo.DIM_Category c ON f.DIM_Category_SK = c.DIM_Category_SK
GROUP BY c.CategoryName
ORDER BY TotalTax DESC;

SET STATISTICS TIME OFF;
GO

-- 2b. SIMPLE QUERY — Raw Vault equivalent
PRINT '';
PRINT '--- [2b] Simple Query: Raw Vault (Expected: ~199ms) ---';
SET STATISTICS TIME ON;

SELECT
    sc.CategoryName,
    SUM(sd.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Bronze.dbo.HUB_Declaration hd
JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
    ON hd.HUB_Declaration_HK = sd.HUB_Declaration_HK
    AND sd.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration ltd
    ON hd.HUB_Declaration_HK = ltd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.HUB_Taxpayer ht
    ON ltd.HUB_Taxpayer_HK = ht.HUB_Taxpayer_HK
JOIN DV_Bronze.dbo.SAT_Taxpayer st
    ON ht.HUB_Taxpayer_HK = st.HUB_Taxpayer_HK
    AND st.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.HUB_Category hc
    ON HASHBYTES('SHA2_256', CAST(st.CategoryID AS VARCHAR(20))) = hc.HUB_Category_HK
JOIN DV_Bronze.dbo.SAT_Category sc
    ON hc.HUB_Category_HK = sc.HUB_Category_HK
    AND sc.SAT_EndDate IS NULL
GROUP BY sc.CategoryName
ORDER BY TotalTax DESC;

SET STATISTICS TIME OFF;
PRINT '   >>> Speedup: 199 / 24 = 8.3x';
GO

-- 2c. MEDIUM QUERY — Gold (4-table join)
PRINT '';
PRINT '--- [2c] Medium Query: Gold Layer (Expected: ~18ms) ---';
SET STATISTICS TIME ON;

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

SET STATISTICS TIME OFF;
GO

-- 2d. MEDIUM QUERY — Raw Vault equivalent
PRINT '';
PRINT '--- [2d] Medium Query: Raw Vault (Expected: ~123ms) ---';
SET STATISTICS TIME ON;

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

SET STATISTICS TIME OFF;
PRINT '   >>> Speedup: 123 / 18 = 6.8x';
GO

-- 2e. COMPLEX QUERY — Gold (4-table join, full aggregation)
PRINT '';
PRINT '--- [2e] Complex Query: Gold Layer (Expected: ~771ms) ---';
SET STATISTICS TIME ON;

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

SET STATISTICS TIME OFF;
GO

-- 2f. COMPLEX QUERY — Raw Vault equivalent
PRINT '';
PRINT '--- [2f] Complex Query: Raw Vault (Expected: ~798ms) ---';
SET STATISTICS TIME ON;

SELECT
    YEAR(sd.DeclarationMonth) AS CalendarYear,
    MONTH(sd.DeclarationMonth) AS MonthNumber,
    sc.CategoryName,
    sd.DeclarationStatus,
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
GROUP BY YEAR(sd.DeclarationMonth), MONTH(sd.DeclarationMonth),
         sc.CategoryName, sd.DeclarationStatus
ORDER BY CalendarYear, MonthNumber, sc.CategoryName;

SET STATISTICS TIME OFF;
PRINT '   >>> Speedup: 798 / 771 = 1.03x';
GO


-- ════════════════════════════════════════════════════════════════
-- BENCHMARK 3: DATABASE OBJECT COUNT (67 Objects)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '  BENCHMARK 3: DATABASE OBJECT COUNT';
PRINT '  Expected: 67 objects across 6 databases';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

SELECT 'TaxSystemDB' AS [Database], 'Source' AS Layer, COUNT(*) AS Objects
FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'ETL_Control', 'Control', COUNT(*)
FROM ETL_Control.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Staging', 'Staging', COUNT(*)
FROM DV_Staging.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Bronze', 'Bronze', COUNT(*)
FROM DV_Bronze.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Silver', 'Silver', COUNT(*)
FROM DV_Silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
UNION ALL
SELECT 'DV_Gold', 'Gold', COUNT(*)
FROM DV_Gold.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY Layer;

-- Grand total
SELECT SUM(cnt) AS TotalObjects FROM (
    SELECT COUNT(*) AS cnt FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM ETL_Control.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Staging.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Bronze.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Silver.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    UNION ALL SELECT COUNT(*) FROM DV_Gold.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
) t;
GO


-- ════════════════════════════════════════════════════════════════
-- BENCHMARK 4: SSIS PACKAGE COUNT (60 Packages)
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '  BENCHMARK 4: SSIS PACKAGE COUNT';
PRINT '  Expected: 60 packages (3 master + 8 orchestrator + 49 child)';
PRINT '═══════════════════════════════════════════════════════════';
PRINT '';

-- List all packages from SSISDB catalog
SELECT
    f.name AS FolderName,
    p.name AS ProjectName,
    pk.name AS PackageName
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
ORDER BY pk.name;

-- Package count
SELECT COUNT(*) AS TotalSSISPackages
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id;
GO


-- ════════════════════════════════════════════════════════════════
-- SUMMARY
-- ════════════════════════════════════════════════════════════════
PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  BENCHMARK VERIFICATION COMPLETE                            ║';
PRINT '║                                                              ║';
PRINT '║  Checklist:                                                  ║';
PRINT '║  [  ] 1a. Batch Log — Full Load Duration = ~120.9s          ║';
PRINT '║  [  ] 1b. Per-Layer Timing matches Table 5.3                ║';
PRINT '║  [  ] 1c. All 49 steps = Success                            ║';
PRINT '║  [  ] 1d. Total Records = 336,677 (Full)                    ║';
PRINT '║  [  ] 1e. Error Count = 0                                   ║';
PRINT '║  [  ] 2a. Simple Gold = ~24ms                               ║';
PRINT '║  [  ] 2b. Simple Raw Vault = ~199ms (8.3x)                  ║';
PRINT '║  [  ] 2c. Medium Gold = ~18ms                               ║';
PRINT '║  [  ] 2d. Medium Raw Vault = ~123ms (6.8x)                  ║';
PRINT '║  [  ] 2e. Complex Gold = ~771ms                             ║';
PRINT '║  [  ] 2f. Complex Raw Vault = ~798ms (1.03x)                ║';
PRINT '║  [  ] 3.  Database Objects = 67                              ║';
PRINT '║  [  ] 4.  SSIS Packages = 60                                ║';
PRINT '║                                                              ║';
PRINT '║  Capture SSMS screenshot for each benchmark above.          ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';
PRINT '  Report Complete: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
GO
