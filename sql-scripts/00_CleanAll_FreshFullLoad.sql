-- ============================================================================
-- Script: 00_CleanAll_FreshFullLoad.sql
-- Purpose: Truncate ALL data across ETL Control, Staging, Bronze, Silver,
--          and Gold databases to prepare for a fresh full-load test.
-- Author: Data Management Bureau — GDT Cambodia
-- Date: 2026-02-22
-- ============================================================================
-- WARNING: This script PERMANENTLY deletes ALL data in all 5 databases.
--          Only run this in DEV/TEST environments. NEVER run in PRODUCTION.
-- ============================================================================
-- Execution Order (respects FK constraints — delete child tables first):
--   1. Gold      (Fact → Dimension)
--   2. Silver    (BUS → BRG → PIT)
--   3. Bronze    (LNK → SAT → HUB)
--   4. Staging   (all STG tables)
--   5. ETL       (logs → watermark → batch)
-- ============================================================================

SET NOCOUNT ON;
PRINT '============================================================';
PRINT '  CLEAN ALL — Fresh Full-Load Reset';
PRINT '  Started: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '============================================================';
PRINT '';


-- ════════════════════════════════════════════════════════════════
-- 1. GOLD LAYER (DV_Gold) — Fact tables first, then Dimensions
-- ════════════════════════════════════════════════════════════════
USE DV_Gold;
GO

PRINT '── [1/5] DV_Gold ──';

-- 1a. Fact Tables (no FK dependencies on other Facts)
TRUNCATE TABLE dbo.FACT_MonthlyDeclaration;
TRUNCATE TABLE dbo.FACT_Payment;
TRUNCATE TABLE dbo.FACT_MonthlySnapshot;
TRUNCATE TABLE dbo.FACT_DeclarationLifecycle;
PRINT '   ✓ 4 Fact tables truncated';

-- 1b. Dimension Tables
-- DIM_Taxpayer and DIM_Officer are referenced by Fact FKs,
-- but TRUNCATE won't work if FK exists. Use DELETE instead.
DELETE FROM dbo.DIM_Taxpayer;
DELETE FROM dbo.DIM_Officer;
DELETE FROM dbo.DIM_Category;
DELETE FROM dbo.DIM_Structure;
DELETE FROM dbo.DIM_Activity;
DELETE FROM dbo.DIM_PaymentMethod;
DELETE FROM dbo.DIM_Status;
PRINT '   ✓ 7 Dimension tables deleted';

-- 1c. Reset IDENTITY seeds
DBCC CHECKIDENT ('DIM_Taxpayer', RESEED, 0);
DBCC CHECKIDENT ('DIM_Officer', RESEED, 0);
DBCC CHECKIDENT ('DIM_Category', RESEED, 0);
DBCC CHECKIDENT ('DIM_Structure', RESEED, 0);
DBCC CHECKIDENT ('DIM_Activity', RESEED, 0);
DBCC CHECKIDENT ('DIM_PaymentMethod', RESEED, 0);
DBCC CHECKIDENT ('DIM_Status', RESEED, 0);
DBCC CHECKIDENT ('FACT_MonthlyDeclaration', RESEED, 0);
DBCC CHECKIDENT ('FACT_Payment', RESEED, 0);
DBCC CHECKIDENT ('FACT_MonthlySnapshot', RESEED, 0);
DBCC CHECKIDENT ('FACT_DeclarationLifecycle', RESEED, 0);
PRINT '   ✓ 11 IDENTITY seeds reset to 0';
PRINT '';
GO


-- ════════════════════════════════════════════════════════════════
-- 2. SILVER LAYER (DV_Silver) — BUS → BRG → PIT
-- ════════════════════════════════════════════════════════════════
USE DV_Silver;
GO

PRINT '── [2/5] DV_Silver ──';

-- 2a. Business Vault Tables
TRUNCATE TABLE dbo.BUS_ComplianceScore;
TRUNCATE TABLE dbo.BUS_MonthlyMetrics;
PRINT '   ✓ 2 Business Vault tables truncated';

-- 2b. Bridge Table
TRUNCATE TABLE dbo.BRG_Taxpayer_Owner;
PRINT '   ✓ 1 Bridge table truncated';

-- 2c. PIT Tables
TRUNCATE TABLE dbo.PIT_Taxpayer;
TRUNCATE TABLE dbo.PIT_Declaration;
TRUNCATE TABLE dbo.PIT_Payment;
PRINT '   ✓ 3 PIT tables truncated';
PRINT '';
GO


-- ════════════════════════════════════════════════════════════════
-- 3. BRONZE LAYER (DV_Bronze) — Links → Satellites → Hubs
-- ════════════════════════════════════════════════════════════════
USE DV_Bronze;
GO

PRINT '── [3/5] DV_Bronze ──';

-- 3a. Link Tables (reference Hubs via FK)
DELETE FROM dbo.LNK_TaxpayerDeclaration;
DELETE FROM dbo.LNK_DeclarationPayment;
DELETE FROM dbo.LNK_TaxpayerOfficer;
DELETE FROM dbo.LNK_TaxpayerOwner;
DELETE FROM dbo.LNK_TaxpayerAnnualDecl;
PRINT '   ✓ 5 Link tables deleted';

-- 3b. Satellite Tables (reference Hubs via FK)
DELETE FROM dbo.SAT_Category;
DELETE FROM dbo.SAT_Structure;
DELETE FROM dbo.SAT_Activity;
DELETE FROM dbo.SAT_Taxpayer;
DELETE FROM dbo.SAT_Owner;
DELETE FROM dbo.SAT_Officer;
DELETE FROM dbo.SAT_MonthlyDecl;
DELETE FROM dbo.SAT_Payment;
DELETE FROM dbo.SAT_AnnualDecl;
PRINT '   ✓ 9 Satellite tables deleted';

-- 3c. Hub Tables (parent tables — delete last)
DELETE FROM dbo.HUB_Category;
DELETE FROM dbo.HUB_Structure;
DELETE FROM dbo.HUB_Activity;
DELETE FROM dbo.HUB_Taxpayer;
DELETE FROM dbo.HUB_Owner;
DELETE FROM dbo.HUB_Officer;
DELETE FROM dbo.HUB_Declaration;
DELETE FROM dbo.HUB_Payment;
DELETE FROM dbo.HUB_AnnualDecl;
PRINT '   ✓ 9 Hub tables deleted';
PRINT '';
GO


-- ════════════════════════════════════════════════════════════════
-- 4. STAGING LAYER (DV_Staging) — All STG tables
-- ════════════════════════════════════════════════════════════════
USE DV_Staging;
GO

PRINT '── [4/5] DV_Staging ──';

TRUNCATE TABLE dbo.STG_Category;
TRUNCATE TABLE dbo.STG_Structure;
TRUNCATE TABLE dbo.STG_Activity;
TRUNCATE TABLE dbo.STG_Taxpayer;
TRUNCATE TABLE dbo.STG_Owner;
TRUNCATE TABLE dbo.STG_Officer;
TRUNCATE TABLE dbo.STG_MonthlyDeclaration;
TRUNCATE TABLE dbo.STG_AnnualDeclaration;
TRUNCATE TABLE dbo.STG_Payment;
PRINT '   ✓ 9 Staging tables truncated';
PRINT '';
GO


-- ════════════════════════════════════════════════════════════════
-- 5. ETL CONTROL (ETL_Control) — Logs → Watermark → Batch
-- ════════════════════════════════════════════════════════════════
USE ETL_Control;
GO

PRINT '── [5/5] ETL_Control ──';

-- 5a. Child log tables first
TRUNCATE TABLE dbo.ETL_ErrorLog;
TRUNCATE TABLE dbo.ETL_AlertLog;
TRUNCATE TABLE dbo.ETL_DataQualityResult;
PRINT '   ✓ 3 Log/Result tables truncated';

-- 5b. Step log (references BatchLog via FK)
DELETE FROM dbo.ETL_StepLog;
PRINT '   ✓ ETL_StepLog deleted';

-- 5c. Batch log
DELETE FROM dbo.ETL_BatchLog;
PRINT '   ✓ ETL_BatchLog deleted';

-- 5d. Watermark (reset to initial state for full load)
DELETE FROM dbo.ETL_Watermark;
PRINT '   ✓ ETL_Watermark deleted (full load will re-initialize)';

-- 5e. Reset IDENTITY seeds
DBCC CHECKIDENT ('ETL_BatchLog', RESEED, 0);
DBCC CHECKIDENT ('ETL_StepLog', RESEED, 0);
DBCC CHECKIDENT ('ETL_ErrorLog', RESEED, 0);
DBCC CHECKIDENT ('ETL_AlertLog', RESEED, 0);
DBCC CHECKIDENT ('ETL_DataQualityResult', RESEED, 0);
PRINT '   ✓ 5 ETL IDENTITY seeds reset to 0';

-- 5f. Keep ETL_Configuration and ETL_Process intact (reference data)
-- 5g. Keep ETL_DataQualityCheck intact (rule definitions)
PRINT '   ℹ ETL_Configuration, ETL_Process, ETL_DataQualityCheck preserved';
PRINT '';
GO


-- ════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ════════════════════════════════════════════════════════════════
USE ETL_Control;
GO

PRINT '============================================================';
PRINT '  VERIFICATION — Row Counts After Clean';
PRINT '============================================================';

-- Quick row count check across all databases
SELECT 'ETL_Control' AS [Database], 'ETL_BatchLog' AS [Table], COUNT(*) AS [Rows] FROM ETL_Control.dbo.ETL_BatchLog
UNION ALL SELECT 'ETL_Control', 'ETL_StepLog', COUNT(*) FROM ETL_Control.dbo.ETL_StepLog
UNION ALL SELECT 'ETL_Control', 'ETL_ErrorLog', COUNT(*) FROM ETL_Control.dbo.ETL_ErrorLog
UNION ALL SELECT 'ETL_Control', 'ETL_Watermark', COUNT(*) FROM ETL_Control.dbo.ETL_Watermark
UNION ALL SELECT 'DV_Staging', 'STG_Category', COUNT(*) FROM DV_Staging.dbo.STG_Category
UNION ALL SELECT 'DV_Staging', 'STG_Taxpayer', COUNT(*) FROM DV_Staging.dbo.STG_Taxpayer
UNION ALL SELECT 'DV_Staging', 'STG_MonthlyDeclaration', COUNT(*) FROM DV_Staging.dbo.STG_MonthlyDeclaration
UNION ALL SELECT 'DV_Staging', 'STG_Payment', COUNT(*) FROM DV_Staging.dbo.STG_Payment
UNION ALL SELECT 'DV_Bronze', 'HUB_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.HUB_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'SAT_Taxpayer', COUNT(*) FROM DV_Bronze.dbo.SAT_Taxpayer
UNION ALL SELECT 'DV_Bronze', 'LNK_TaxpayerDeclaration', COUNT(*) FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration
UNION ALL SELECT 'DV_Silver', 'PIT_Taxpayer', COUNT(*) FROM DV_Silver.dbo.PIT_Taxpayer
UNION ALL SELECT 'DV_Silver', 'BUS_MonthlyMetrics', COUNT(*) FROM DV_Silver.dbo.BUS_MonthlyMetrics
UNION ALL SELECT 'DV_Gold', 'DIM_Taxpayer', COUNT(*) FROM DV_Gold.dbo.DIM_Taxpayer
UNION ALL SELECT 'DV_Gold', 'FACT_MonthlyDeclaration', COUNT(*) FROM DV_Gold.dbo.FACT_MonthlyDeclaration
ORDER BY [Database], [Table];

PRINT '';
PRINT '============================================================';
PRINT '  CLEAN ALL — Complete!';
PRINT '  Finished: ' + CONVERT(VARCHAR(30), GETDATE(), 121);
PRINT '  All tables should show 0 rows above.';
PRINT '  Ready for fresh full-load test.';
PRINT '============================================================';
GO
