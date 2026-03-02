-- ============================================================================
-- CONSOLIDATED SCREENSHOT CAPTURE SCRIPT
-- Metric Benchmarks Verification — Query Performance & Object Counts
-- ============================================================================
-- HOW TO USE:
--   1. Connect SSMS to ETL_Control (and cross-DB queries)
--   2. Set Results to Grid mode (Ctrl+D)
--   3. Run each section separately as instructed
--   4. For query performance: use Messages tab for timing screenshots
-- ============================================================================
-- PREREQUISITES:
--   - Full Load pipeline executed successfully (BatchStatus = 'Success')
--   - For incremental benchmarks: 12_IncrementalTest_SourceChanges.sql + 
--     Master_Complete_Pipeline.dtsx (incremental) already completed
-- ============================================================================

SET NOCOUNT ON;
GO

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 1: ETL BatchLog — Full + Incremental           ║
-- ║  (Benchmarks Doc Sections 2.2 + 2.6)                       ║
-- ╚══════════════════════════════════════════════════════════════╝
SELECT
    b.BatchID,
    b.BatchType,
    b.BatchStartTime,
    b.BatchEndTime,
    b.BatchStatus,
    DATEDIFF(SECOND, b.BatchStartTime, b.BatchEndTime) AS [Duration (sec)],
    CAST(DATEDIFF(MILLISECOND, b.BatchStartTime, b.BatchEndTime) / 1000.0 AS DECIMAL(10,1)) AS [Duration (dec)]
FROM ETL_Control.dbo.ETL_BatchLog b
WHERE b.BatchStatus = 'Success'
ORDER BY b.BatchID;

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 2: Per-Layer Timing Summary                    ║
-- ║  (Benchmarks Doc Section 2.3 — Table 5.3)                  ║
-- ╚══════════════════════════════════════════════════════════════╝
DECLARE @FullBatchID INT = (
    SELECT TOP 1 BatchID FROM ETL_Control.dbo.ETL_BatchLog 
    WHERE BatchStatus = 'Success' AND BatchType = 'Full'
    ORDER BY BatchID ASC
);

SELECT
    CASE
        WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
        WHEN StepName LIKE 'Load_HUB_%' THEN '2. Bronze (Hub)'
        WHEN StepName LIKE 'Load_SAT_%' THEN '2. Bronze (Sat)'
        WHEN StepName LIKE 'Load_LNK_%' THEN '2. Bronze (Link)'
        WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%' 
            OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
        WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
        ELSE '0. Other'
    END AS [Layer],
    COUNT(*) AS [Steps],
    CAST(SUM(DATEDIFF(MILLISECOND, StepStartTime, StepEndTime)) / 1000.0 AS DECIMAL(10,1)) AS [Total (sec)],
    SUM(RecordsProcessed) AS [Records Processed]
FROM ETL_Control.dbo.ETL_StepLog
WHERE BatchID = @FullBatchID
GROUP BY CASE
    WHEN StepName LIKE 'Load_STG_%' THEN '1. Staging'
    WHEN StepName LIKE 'Load_HUB_%' THEN '2. Bronze (Hub)'
    WHEN StepName LIKE 'Load_SAT_%' THEN '2. Bronze (Sat)'
    WHEN StepName LIKE 'Load_LNK_%' THEN '2. Bronze (Link)'
    WHEN StepName LIKE 'Load_PIT_%' OR StepName LIKE 'Load_BRG_%' 
        OR StepName LIKE 'Load_BUS_%' THEN '3. Silver'
    WHEN StepName LIKE 'Load_DIM_%' OR StepName LIKE 'Load_FACT_%' THEN '4. Gold'
    ELSE '0. Other'
END
ORDER BY [Layer];

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 3: All 49 Steps Detail (Full Load)             ║
-- ║  (Benchmarks Doc Section 2.4)                              ║
-- ╚══════════════════════════════════════════════════════════════╝
SELECT
    ROW_NUMBER() OVER (ORDER BY s.StepLogID) AS [#],
    s.StepName AS [Step Name],
    s.StepStatus AS [Status],
    CAST(DATEDIFF(MILLISECOND, s.StepStartTime, s.StepEndTime) / 1000.0 AS DECIMAL(10,2)) AS [Duration (sec)],
    s.RecordsProcessed AS [Records]
FROM ETL_Control.dbo.ETL_StepLog s
WHERE s.BatchID = @FullBatchID
ORDER BY s.StepLogID;

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 4: Zero Error Verification                     ║
-- ║  (Benchmarks Doc Section 2.5)                              ║
-- ╚══════════════════════════════════════════════════════════════╝
SELECT 
    @FullBatchID AS [BatchID],
    COUNT(*) AS [Error Count],
    CASE WHEN COUNT(*) = 0 THEN 'PASS — No errors' ELSE 'FAIL — Errors found' END AS [Result]
FROM ETL_Control.dbo.ETL_ErrorLog
WHERE BatchID = @FullBatchID;

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 5: Database Object Count (67 Total)            ║
-- ║  (Benchmarks Doc Section 5)                                ║
-- ╚══════════════════════════════════════════════════════════════╝
SELECT 'TaxSystemDB' AS [Database], 'Source' AS [Layer], COUNT(*) AS [Objects]
FROM TaxSystemDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
    AND TABLE_NAME NOT LIKE 'MS%' AND TABLE_NAME NOT LIKE 'spt_%' AND TABLE_NAME NOT LIKE 'sys%'
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
ORDER BY [Layer];

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  BENCH GRID 6: SSIS Package Inventory (60 Packages)        ║
-- ║  (Benchmarks Doc Section 6)                                ║
-- ╚══════════════════════════════════════════════════════════════╝
SELECT
    f.name AS [Folder],
    p.name AS [Project],
    pk.name AS [Package Name],
    CASE
        WHEN pk.name LIKE 'Master_%' THEN 'Master'
        WHEN pk.name LIKE '%_All_%' OR pk.name LIKE '%_Load_All%' THEN 'Orchestrator'
        ELSE 'Child'
    END AS [Type]
FROM SSISDB.catalog.packages pk
JOIN SSISDB.catalog.projects p ON pk.project_id = p.project_id
JOIN SSISDB.catalog.folders f ON p.folder_id = f.folder_id
ORDER BY 
    CASE WHEN pk.name LIKE 'Master_%' THEN 1 
         WHEN pk.name LIKE '%_All_%' OR pk.name LIKE '%_Load_All%' THEN 2 
         ELSE 3 END,
    pk.name;
GO

-- ============================================================================
-- QUERY PERFORMANCE BENCHMARKS
-- Run each query SEPARATELY with SET STATISTICS TIME ON
-- Capture the Messages tab showing execution time for each
-- ============================================================================

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PERF 1a: GOLD — Simple Query (Expected ~24ms)             ║
-- ║  Capture: Messages tab → elapsed time                      ║
-- ╚══════════════════════════════════════════════════════════════╝
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

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PERF 1b: RAW VAULT — Simple Query (Expected ~199ms)       ║
-- ║  Capture: Messages tab → elapsed time                      ║
-- ╚══════════════════════════════════════════════════════════════╝
SET STATISTICS TIME ON;
GO
SELECT
    sc.CategoryName,
    SUM(sd.TaxAmount) AS TotalTax,
    COUNT(*) AS DeclarationCount
FROM DV_Bronze.dbo.HUB_Declaration hd
JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd ON hd.HUB_Declaration_HK = sd.HUB_Declaration_HK AND sd.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration ltd ON hd.HUB_Declaration_HK = ltd.HUB_Declaration_HK
JOIN DV_Bronze.dbo.HUB_Taxpayer ht ON ltd.HUB_Taxpayer_HK = ht.HUB_Taxpayer_HK
JOIN DV_Bronze.dbo.SAT_Taxpayer st ON ht.HUB_Taxpayer_HK = st.HUB_Taxpayer_HK AND st.SAT_EndDate IS NULL
JOIN DV_Bronze.dbo.HUB_Category hc ON HASHBYTES('SHA2_256', CAST(st.CategoryID AS VARCHAR(20))) = hc.HUB_Category_HK
JOIN DV_Bronze.dbo.SAT_Category sc ON hc.HUB_Category_HK = sc.HUB_Category_HK AND sc.SAT_EndDate IS NULL
GROUP BY sc.CategoryName
ORDER BY TotalTax DESC;
GO
SET STATISTICS TIME OFF;
GO

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PERF 2a: GOLD — Medium Query (Expected ~18ms)             ║
-- ╚══════════════════════════════════════════════════════════════╝
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

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PERF 3a: GOLD — Complex Query (Expected ~771ms)           ║
-- ╚══════════════════════════════════════════════════════════════╝
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
GROUP BY d.CalendarYear, d.MonthNumber, d.MonthName, c.CategoryName, s.StatusDescription
ORDER BY d.CalendarYear, d.MonthNumber, c.CategoryName;
GO
SET STATISTICS TIME OFF;
GO

-- ============================================================================
-- DONE! You should now have all benchmark grids + performance timings.
-- Insert screenshots into Metric_Benchmarks_Verification.docx
-- ============================================================================
