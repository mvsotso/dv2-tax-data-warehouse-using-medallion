-- ============================================================================
-- SOURCE DATABASE & SAMPLE DATA VERIFICATION SCRIPT
-- Data Vault 2.0 Tax System Data Warehouse
-- Mr. Sot So — RUPP 2026
-- ============================================================================
-- Run this script in SSMS connected to TaxSystemDB
-- Screenshot each result for your defense document
-- ============================================================================

USE TaxSystemDB;
GO

PRINT '================================================================';
PRINT '  SECTION 1: DATABASE STRUCTURE VERIFICATION';
PRINT '================================================================';

-- 1.1 Table Inventory & Record Counts
PRINT '';
PRINT '--- 1.1 Table Inventory & Record Counts ---';
SELECT 
    t.TABLE_SCHEMA AS [Schema],
    t.TABLE_NAME AS [Table Name],
    CASE 
        WHEN t.TABLE_NAME IN ('Category','Structure','Activity') THEN 'Lookup'
        WHEN t.TABLE_NAME IN ('Taxpayer','Owner','Officer') THEN 'Reference'
        ELSE 'Transaction'
    END AS [Table Type],
    p.rows AS [Record Count]
FROM INFORMATION_SCHEMA.TABLES t
INNER JOIN sys.partitions p 
    ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
    AND p.index_id IN (0,1)
WHERE t.TABLE_TYPE = 'BASE TABLE'
    AND t.TABLE_NAME NOT LIKE 'MS%'
    AND t.TABLE_NAME NOT LIKE 'spt_%'
    AND t.TABLE_NAME NOT LIKE 'sys%'
ORDER BY 
    CASE 
        WHEN t.TABLE_NAME IN ('Category','Structure','Activity') THEN 1
        WHEN t.TABLE_NAME IN ('Taxpayer','Owner','Officer') THEN 2
        ELSE 3
    END, 
    t.TABLE_NAME;

-- 1.2 Column counts per table
PRINT '';
PRINT '--- 1.2 Column Structure per Table ---';
SELECT 
    TABLE_NAME AS [Table],
    COUNT(*) AS [Column Count],
    STRING_AGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS [Columns]
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME NOT LIKE 'MS%'
    AND TABLE_NAME NOT LIKE 'spt_%'
    AND TABLE_NAME NOT LIKE 'sys%'
    AND TABLE_NAME NOT LIKE 'vw_%'
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- 1.3 Foreign key relationships
PRINT '';
PRINT '--- 1.3 Referential Integrity (Foreign Keys) ---';
SELECT 
    fk.name AS [FK Name],
    OBJECT_NAME(fk.parent_object_id) AS [Child Table],
    COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS [Child Column],
    OBJECT_NAME(fk.referenced_object_id) AS [Parent Table],
    COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS [Parent Column]
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
ORDER BY OBJECT_NAME(fk.parent_object_id), fk.name;

-- 1.4 Check constraints
PRINT '';
PRINT '--- 1.4 Data Quality Constraints (CHECK) ---';
SELECT 
    OBJECT_NAME(parent_object_id) AS [Table],
    name AS [Constraint Name],
    definition AS [Constraint Definition]
FROM sys.check_constraints
ORDER BY OBJECT_NAME(parent_object_id);

-- 1.5 Indexes
PRINT '';
PRINT '--- 1.5 Indexes ---';
SELECT 
    OBJECT_NAME(i.object_id) AS [Table],
    i.name AS [Index Name],
    i.type_desc AS [Type],
    i.is_unique AS [Unique],
    STRING_AGG(COL_NAME(ic.object_id, ic.column_id), ', ') 
        WITHIN GROUP (ORDER BY ic.key_ordinal) AS [Columns]
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE OBJECT_SCHEMA_NAME(i.object_id) = 'dbo'
    AND i.name IS NOT NULL
    AND OBJECT_NAME(i.object_id) NOT LIKE 'MS%'
    AND OBJECT_NAME(i.object_id) NOT LIKE 'spt_%'
    AND OBJECT_NAME(i.object_id) NOT LIKE 'sys%'
    AND OBJECT_NAME(i.object_id) NOT LIKE 'vw_%'
GROUP BY i.object_id, i.name, i.type_desc, i.is_unique
ORDER BY OBJECT_NAME(i.object_id), i.name;

GO

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 2: DATA DISTRIBUTION & QUALITY VERIFICATION';
PRINT '================================================================';

-- 2.1 Taxpayer category distribution
PRINT '';
PRINT '--- 2.1 Taxpayer Distribution by Category ---';
SELECT 
    c.CategoryName,
    COUNT(t.TaxpayerID) AS [Taxpayer Count],
    CAST(COUNT(t.TaxpayerID) * 100.0 / (SELECT COUNT(*) FROM Taxpayer) AS DECIMAL(5,1)) AS [Percentage %],
    FORMAT(AVG(t.EstimatedAnnualRevenue), 'N0') AS [Avg Revenue],
    FORMAT(MIN(t.EstimatedAnnualRevenue), 'N0') AS [Min Revenue],
    FORMAT(MAX(t.EstimatedAnnualRevenue), 'N0') AS [Max Revenue]
FROM Taxpayer t
LEFT JOIN Category c ON t.CategoryID = c.CategoryID
GROUP BY c.CategoryName
ORDER BY COUNT(t.TaxpayerID) DESC;

-- 2.2 Temporal coverage of declarations (yearly)
PRINT '';
PRINT '--- 2.2 Monthly Declaration Coverage (Year-Level) ---';
SELECT 
    DeclarationYear AS [Year],
    COUNT(*) AS [Total Declarations],
    COUNT(DISTINCT TaxpayerID) AS [Unique Taxpayers],
    CAST(COUNT(DISTINCT TaxpayerID) * 100.0 / 
        (SELECT COUNT(*) FROM Taxpayer) AS DECIMAL(5,1)) AS [Coverage %],
    FORMAT(SUM(TotalAmount), 'N0') AS [Total Amount]
FROM MonthlyDeclaration
GROUP BY DeclarationYear
ORDER BY DeclarationYear;

-- 2.3 Monthly distribution within a year (2023 sample)
PRINT '';
PRINT '--- 2.3 Monthly Distribution (2023 Sample) ---';
SELECT 
    DeclarationMonth AS [Month],
    COUNT(*) AS [Declarations],
    COUNT(DISTINCT TaxpayerID) AS [Taxpayers],
    FORMAT(SUM(TotalAmount), 'N0') AS [Total Amount],
    FORMAT(AVG(TaxAmount), 'N0') AS [Avg Tax per Declaration]
FROM MonthlyDeclaration
WHERE DeclarationYear = 2023
GROUP BY DeclarationMonth
ORDER BY DeclarationMonth;

-- 2.4 Payment method distribution
PRINT '';
PRINT '--- 2.4 Payment Distribution ---';
SELECT 
    PaymentMethod,
    COUNT(*) AS [Payment Count],
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Payment) AS DECIMAL(5,1)) AS [Percentage %],
    FORMAT(SUM(PaymentAmount), 'N0') AS [Total Amount],
    FORMAT(AVG(PaymentAmount), 'N0') AS [Avg Amount]
FROM Payment
GROUP BY PaymentMethod
ORDER BY COUNT(*) DESC;

-- 2.5 Declaration status distribution
PRINT '';
PRINT '--- 2.5 Declaration Status Distribution ---';
SELECT 
    Status,
    COUNT(*) AS [Count],
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM MonthlyDeclaration) AS DECIMAL(5,1)) AS [Percentage %]
FROM MonthlyDeclaration
GROUP BY Status
ORDER BY COUNT(*) DESC;

-- 2.6 Revenue distribution (realistic ranges)
PRINT '';
PRINT '--- 2.6 Taxpayer Revenue Distribution ---';
SELECT 
    CASE 
        WHEN EstimatedAnnualRevenue < 50000 THEN '< $50K'
        WHEN EstimatedAnnualRevenue < 200000 THEN '$50K - $200K'
        WHEN EstimatedAnnualRevenue < 500000 THEN '$200K - $500K'
        WHEN EstimatedAnnualRevenue < 1000000 THEN '$500K - $1M'
        ELSE '> $1M'
    END AS [Revenue Range],
    COUNT(*) AS [Taxpayer Count],
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Taxpayer) AS DECIMAL(5,1)) AS [Percentage %]
FROM Taxpayer
GROUP BY CASE 
    WHEN EstimatedAnnualRevenue < 50000 THEN '< $50K'
    WHEN EstimatedAnnualRevenue < 200000 THEN '$50K - $200K'
    WHEN EstimatedAnnualRevenue < 500000 THEN '$200K - $500K'
    WHEN EstimatedAnnualRevenue < 1000000 THEN '$500K - $1M'
    ELSE '> $1M'
END
ORDER BY MIN(EstimatedAnnualRevenue);

GO

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 3: REFERENTIAL INTEGRITY PROOF';
PRINT '================================================================';

-- 3.1 Orphan record check (should all be 0)
PRINT '';
PRINT '--- 3.1 Orphan Record Check (All Should Be 0) ---';
SELECT 'MonthlyDeclaration -> Taxpayer' AS [Relationship],
    COUNT(*) AS [Orphan Count]
FROM MonthlyDeclaration md
WHERE NOT EXISTS (SELECT 1 FROM Taxpayer t WHERE t.TaxpayerID = md.TaxpayerID)
UNION ALL
SELECT 'Payment -> Taxpayer',
    COUNT(*)
FROM Payment p
WHERE NOT EXISTS (SELECT 1 FROM Taxpayer t WHERE t.TaxpayerID = p.TaxpayerID)
UNION ALL
SELECT 'Payment -> MonthlyDeclaration',
    COUNT(*)
FROM Payment p
WHERE p.DeclarationID IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM MonthlyDeclaration md WHERE md.DeclarationID = p.DeclarationID)
UNION ALL
SELECT 'Owner -> Taxpayer',
    COUNT(*)
FROM Owner o
WHERE NOT EXISTS (SELECT 1 FROM Taxpayer t WHERE t.TaxpayerID = o.TaxpayerID)
UNION ALL
SELECT 'Taxpayer -> Category',
    COUNT(*)
FROM Taxpayer t
WHERE NOT EXISTS (SELECT 1 FROM Category c WHERE c.CategoryID = t.CategoryID)
UNION ALL
SELECT 'Taxpayer -> Structure',
    COUNT(*)
FROM Taxpayer t
WHERE t.StructureID IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM Structure s WHERE s.StructureID = t.StructureID)
UNION ALL
SELECT 'MonthlyDeclaration -> Officer',
    COUNT(*)
FROM MonthlyDeclaration md
WHERE md.OfficerID IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM Officer o WHERE o.OfficerID = md.OfficerID);

-- 3.2 NULL check on critical columns
PRINT '';
PRINT '--- 3.2 NULL Check on Critical Columns ---';
SELECT 'Taxpayer.TaxID' AS [Column], COUNT(*) AS [NULL Count] FROM Taxpayer WHERE TaxID IS NULL
UNION ALL
SELECT 'Taxpayer.LegalBusinessName', COUNT(*) FROM Taxpayer WHERE LegalBusinessName IS NULL
UNION ALL
SELECT 'Taxpayer.EstimatedAnnualRevenue', COUNT(*) FROM Taxpayer WHERE EstimatedAnnualRevenue IS NULL
UNION ALL
SELECT 'MonthlyDeclaration.TaxpayerID', COUNT(*) FROM MonthlyDeclaration WHERE TaxpayerID IS NULL
UNION ALL
SELECT 'MonthlyDeclaration.TaxAmount', COUNT(*) FROM MonthlyDeclaration WHERE TaxAmount IS NULL
UNION ALL
SELECT 'MonthlyDeclaration.TotalAmount', COUNT(*) FROM MonthlyDeclaration WHERE TotalAmount IS NULL
UNION ALL
SELECT 'Payment.PaymentAmount', COUNT(*) FROM Payment WHERE PaymentAmount IS NULL
UNION ALL
SELECT 'Payment.TaxpayerID', COUNT(*) FROM Payment WHERE TaxpayerID IS NULL;

-- 3.3 Uniqueness check
PRINT '';
PRINT '--- 3.3 Uniqueness Verification ---';
SELECT 'Taxpayer.TaxID' AS [Column],
    COUNT(*) AS [Total],
    COUNT(DISTINCT TaxID) AS [Distinct],
    CASE WHEN COUNT(*) = COUNT(DISTINCT TaxID) THEN 'PASS' ELSE 'FAIL' END AS [Unique?]
FROM Taxpayer;

GO

PRINT '';
PRINT '================================================================';
PRINT '  SECTION 4: SUMMARY STATISTICS';
PRINT '================================================================';

PRINT '';
PRINT '--- 4.1 Overall Database Summary ---';
SELECT 
    (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE 'MS%' AND TABLE_NAME NOT LIKE 'spt_%' AND TABLE_NAME NOT LIKE 'sys%') AS [Total Tables],
    (SELECT SUM(p.rows) FROM sys.partitions p 
     INNER JOIN INFORMATION_SCHEMA.TABLES t 
     ON OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME) = p.object_id
     WHERE t.TABLE_TYPE = 'BASE TABLE' AND p.index_id IN (0,1)
        AND t.TABLE_NAME NOT LIKE 'MS%' AND t.TABLE_NAME NOT LIKE 'spt_%' AND t.TABLE_NAME NOT LIKE 'sys%') AS [Total Records],
    (SELECT COUNT(*) FROM Taxpayer) AS [Taxpayers],
    (SELECT COUNT(*) FROM MonthlyDeclaration) AS [Monthly Decl],
    (SELECT COUNT(*) FROM AnnualDeclaration) AS [Annual Decl],
    (SELECT COUNT(*) FROM Payment) AS [Payments],
    (SELECT COUNT(*) FROM Owner) AS [Owners],
    (SELECT COUNT(*) FROM Officer) AS [Officers],
    (SELECT MIN(DeclarationYear) FROM MonthlyDeclaration) AS [Year Start],
    (SELECT MAX(DeclarationYear) FROM MonthlyDeclaration) AS [Year End];

-- 4.2 Database size
PRINT '';
PRINT '--- 4.2 Database Size ---';
SELECT 
    DB_NAME() AS [Database],
    CAST(SUM(size * 8.0 / 1024) AS DECIMAL(10,2)) AS [Size (MB)]
FROM sys.database_files;

-- 4.3 Data Quality Overview (your custom view)
PRINT '';
PRINT '--- 4.3 Data Quality Overview ---';
IF OBJECT_ID('vw_DataQuality_Overview', 'V') IS NOT NULL
    EXEC('SELECT * FROM vw_DataQuality_Overview;');
ELSE
    PRINT 'View vw_DataQuality_Overview does not exist.';

PRINT '';
PRINT '================================================================';
PRINT '  VERIFICATION COMPLETE';
PRINT '  Screenshot each result set for your defense document';
PRINT '================================================================';
GO
