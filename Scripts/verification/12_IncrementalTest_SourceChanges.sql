-- ============================================================================
-- Script: 12_IncrementalTest_SourceChanges.sql
-- Purpose: Insert new records and update existing records in TaxSystemDB
--          to test incremental load change detection across all layers.
-- Run:     Execute in SSMS AFTER a successful full load, BEFORE incremental.
-- Author:  Data Management Bureau — GDT Cambodia
-- Date:    2026-02-22
-- ============================================================================

USE TaxSystemDB;
GO

SET NOCOUNT ON;

PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  INCREMENTAL TEST — Source Data Changes                     ║';
PRINT '║  Run: ' + CONVERT(VARCHAR(20), GETDATE(), 120) + '                           ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
PRINT '';


-- ════════════════════════════════════════════════════════════════
-- 1. UPDATE existing records (triggers SAT change detection + SCD2)
-- ════════════════════════════════════════════════════════════════

PRINT '═══ [1] UPDATES — Existing Records ═══';
PRINT '';

-- 1a. Update 3 Taxpayers (LegalBusinessName + Revenue change)
--     → SAT_Taxpayer: new version (old row gets SAT_EndDate, new row inserted)
--     → DIM_Taxpayer: SCD2 new version (old row IsCurrent=0, new row IsCurrent=1)
UPDATE Taxpayer 
SET LegalBusinessName = LegalBusinessName + ' (AMENDED)',
    EstimatedAnnualRevenue = EstimatedAnnualRevenue + 5000.00,
    UpdatedDate = GETDATE()
WHERE TaxpayerID IN (1, 2, 3);

PRINT '   ✓ Updated 3 Taxpayers (TaxpayerID 1,2,3) — LegalBusinessName + Revenue';

-- 1b. Update 2 Officers (Department change)
--     → SAT_Officer: new version
--     → DIM_Officer: SCD2 new version
UPDATE Officer
SET Department = 'Reassigned - ' + Department,
    UpdatedDate = GETDATE()
WHERE OfficerID IN (1, 2);

PRINT '   ✓ Updated 2 Officers (OfficerID 1,2) — Department';

-- 1c. Update 1 Category (Description change)
--     → SAT_Category: new version
--     → DIM_Category: SCD1 in-place update
UPDATE Category
SET CategoryDescription = CategoryDescription + ' (Updated Feb 2026)',
    UpdatedDate = GETDATE()
WHERE CategoryID = 1;

PRINT '   ✓ Updated 1 Category (CategoryID 1) — Description';

-- 1d. Update 2 Owners (OwnershipPercentage change)
--     → SAT_Owner: new version
UPDATE Owner
SET OwnershipPercentage = OwnershipPercentage + 5.00,
    UpdatedDate = GETDATE()
WHERE OwnerID IN (1, 2);

PRINT '   ✓ Updated 2 Owners (OwnerID 1,2) — OwnershipPercentage';

-- 1e. Update 5 MonthlyDeclarations (Status change: Submitted → Approved)
--     → SAT_MonthlyDecl: new version
UPDATE MonthlyDeclaration
SET Status = 'Approved',
    UpdatedDate = GETDATE()
WHERE DeclarationID IN (1, 2, 3, 4, 5)
  AND Status = 'Submitted';

PRINT '   ✓ Updated 5 MonthlyDeclarations (ID 1-5) — Status to Approved';

-- 1f. Update 3 Payments (Status change)
--     → SAT_Payment: new version
UPDATE Payment
SET Status = 'Verified',
    UpdatedDate = GETDATE()
WHERE PaymentID IN (1, 2, 3);

PRINT '   ✓ Updated 3 Payments (PaymentID 1,2,3) — Status to Verified';

PRINT '';


-- ════════════════════════════════════════════════════════════════
-- 2. INSERT new records (triggers Hub + SAT + Link + DIM inserts)
-- ════════════════════════════════════════════════════════════════

PRINT '═══ [2] INSERTS — New Records ═══';
PRINT '';

-- 2a. New Taxpayer
--     → HUB_Taxpayer: new Hub row
--     → SAT_Taxpayer: new SAT row
--     → DIM_Taxpayer: new DIM row
INSERT INTO Taxpayer (TaxID, LegalBusinessName, TradingName, CategoryID, StructureID, 
                      RegistrationDate, EstimatedAnnualRevenue, IsActive)
VALUES 
    ('TAX-INC-001', 'Incremental Test Company A', 'ITC-A Trading', 1, 1, 
     '2026-02-22', 75000.00, 1),
    ('TAX-INC-002', 'Incremental Test Company B', 'ITC-B Trading', 2, 2, 
     '2026-02-22', 120000.00, 1);

DECLARE @NewTaxpayer1 INT = (SELECT TaxpayerID FROM Taxpayer WHERE TaxID = 'TAX-INC-001');
DECLARE @NewTaxpayer2 INT = (SELECT TaxpayerID FROM Taxpayer WHERE TaxID = 'TAX-INC-002');

PRINT '   ✓ Inserted 2 new Taxpayers (TAX-INC-001, TAX-INC-002)';

-- 2b. New Owners for new taxpayers
--     → HUB_Owner: new Hub rows
--     → SAT_Owner: new SAT rows
--     → LNK_TaxpayerOwner: new Link rows
INSERT INTO Owner (TaxpayerID, OwnerName, OwnerType, OwnershipPercentage, IsActive)
VALUES 
    (@NewTaxpayer1, 'Mr. Incremental Owner A', 'Individual', 100.00, 1),
    (@NewTaxpayer2, 'Mrs. Incremental Owner B', 'Individual', 60.00, 1),
    (@NewTaxpayer2, 'Mr. Incremental Partner B', 'Individual', 40.00, 1);

PRINT '   ✓ Inserted 3 new Owners (for new Taxpayers)';

-- 2c. New MonthlyDeclarations
--     → HUB_Declaration: new Hub rows
--     → SAT_MonthlyDecl: new SAT rows
--     → LNK_TaxpayerDeclaration: new Link rows
INSERT INTO MonthlyDeclaration (TaxpayerID, DeclarationMonth, DeclarationYear, 
    GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, TotalAmount,
    DeclarationDate, DueDate, Status, OfficerID, IsActive)
VALUES 
    (@NewTaxpayer1, 1, 2026, 50000.00, 40000.00, 4000.00, 0, 0, 4000.00,
     '2026-02-22', '2026-03-22', 'Submitted', 1, 1),
    (@NewTaxpayer1, 2, 2026, 55000.00, 44000.00, 4400.00, 0, 0, 4400.00,
     '2026-02-22', '2026-03-22', 'Pending', 2, 1),
    (@NewTaxpayer2, 1, 2026, 80000.00, 64000.00, 6400.00, 200.00, 50.00, 6650.00,
     '2026-02-22', '2026-03-22', 'Submitted', 3, 1),
    -- Also add declaration for EXISTING taxpayer (TaxpayerID=10)
    (10, 2, 2026, 30000.00, 24000.00, 2400.00, 0, 0, 2400.00,
     '2026-02-22', '2026-03-22', 'Submitted', 5, 1);

PRINT '   ✓ Inserted 4 new MonthlyDeclarations (3 for new taxpayers + 1 for existing)';

-- 2d. New Payments
--     → HUB_Payment: new Hub rows
--     → SAT_Payment: new SAT rows
--     → LNK_DeclarationPayment: new Link rows
DECLARE @NewDecl1 INT = (SELECT MAX(DeclarationID) FROM MonthlyDeclaration WHERE TaxpayerID = @NewTaxpayer1 AND DeclarationMonth = 1);
DECLARE @NewDecl2 INT = (SELECT MAX(DeclarationID) FROM MonthlyDeclaration WHERE TaxpayerID = @NewTaxpayer2 AND DeclarationMonth = 1);

INSERT INTO Payment (TaxpayerID, DeclarationID, PaymentAmount, PaymentDate, PaymentMethod, 
                     ReferenceNumber, Status, IsActive)
VALUES 
    (@NewTaxpayer1, @NewDecl1, 4000.00, '2026-02-22', 'Bank Transfer', 
     'REF-INC-001', 'Completed', 1),
    (@NewTaxpayer2, @NewDecl2, 6650.00, '2026-02-22', 'Cash', 
     'REF-INC-002', 'Completed', 1),
    -- Payment for existing taxpayer's new declaration
    (10, (SELECT MAX(DeclarationID) FROM MonthlyDeclaration WHERE TaxpayerID = 10 AND DeclarationMonth = 2 AND DeclarationYear = 2026), 
     2400.00, '2026-02-22', 'Check', 'REF-INC-003', 'Pending', 1);

PRINT '   ✓ Inserted 3 new Payments (2 for new taxpayers + 1 for existing)';

-- 2e. New AnnualDeclaration
--     → HUB_AnnualDecl: new Hub row
--     → SAT_AnnualDecl: new SAT row
--     → LNK_TaxpayerAnnualDecl: new Link row
INSERT INTO AnnualDeclaration (TaxpayerID, DeclarationYear, 
    GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, TotalAmount,
    DeclarationDate, DueDate, Status, OfficerID, IsActive)
VALUES 
    (@NewTaxpayer1, 2025, 600000.00, 480000.00, 48000.00, 0, 0, 48000.00,
     '2026-02-22', '2026-03-31', 'Submitted', 1, 1);

PRINT '   ✓ Inserted 1 new AnnualDeclaration (for new taxpayer)';


-- ════════════════════════════════════════════════════════════════
-- 3. VERIFICATION — Before/After Summary
-- ════════════════════════════════════════════════════════════════

PRINT '';
PRINT '═══ [3] EXPECTED CHANGES SUMMARY ═══';
PRINT '';
PRINT '  UPDATES (SAT change detection → new SAT versions):';
PRINT '    Taxpayer:           3 updated (ID 1,2,3 — LegalBusinessName + Revenue)';
PRINT '    Officer:            2 updated (ID 1,2 — Department)';
PRINT '    Category:           1 updated (ID 1 — Description)';
PRINT '    Owner:              2 updated (ID 1,2 — OwnershipPercentage)';
PRINT '    MonthlyDeclaration: 5 updated (ID 1-5 — Status)';
PRINT '    Payment:            3 updated (ID 1-3 — Status)';
PRINT '';
PRINT '  INSERTS (New Hub + SAT + Link + DIM rows):';
PRINT '    Taxpayer:           +2 new (TAX-INC-001, TAX-INC-002)';
PRINT '    Owner:              +3 new (for new taxpayers)';
PRINT '    MonthlyDeclaration: +4 new (3 for new + 1 for existing taxpayer)';
PRINT '    Payment:            +3 new (2 for new + 1 for existing taxpayer)';
PRINT '    AnnualDeclaration:  +1 new (for new taxpayer)';
PRINT '';
PRINT '  EXPECTED INCREMENTAL RESULTS:';
PRINT '    Bronze Hubs:  +2 HUB_Taxpayer, +3 HUB_Owner, +4 HUB_Declaration, +3 HUB_Payment, +1 HUB_AnnualDecl';
PRINT '    Bronze SATs:  new versions for 16 updated + 13 new = ~29 new SAT rows';
PRINT '    Bronze Links: +4 LNK_TaxpayerDeclaration, +3 LNK_DeclarationPayment, +3 LNK_TaxpayerOwner, +1 LNK_TaxpayerAnnualDecl';
PRINT '    Gold DIMs:    +2 DIM_Taxpayer (SCD2), +2 DIM_Officer (SCD2), 1 DIM_Category (SCD1 update)';
PRINT '';

-- Show current watermark values for comparison
PRINT '═══ [4] CURRENT WATERMARK VALUES ═══';
PRINT '';
SELECT 
    w.TableName, 
    w.LastValue,
    w.LastLoadDate
FROM ETL_Control.dbo.ETL_Watermark w
WHERE w.IsActive = 1
ORDER BY w.TableName;

-- Show current source table counts
PRINT '';
PRINT '═══ [5] SOURCE TABLE COUNTS (After Changes) ═══';
PRINT '';
SELECT 'Category' AS TableName, COUNT(*) AS TotalRows, MAX(UpdatedDate) AS LatestUpdate FROM Category
UNION ALL SELECT 'Structure', COUNT(*), MAX(UpdatedDate) FROM Structure
UNION ALL SELECT 'Activity', COUNT(*), MAX(UpdatedDate) FROM Activity
UNION ALL SELECT 'Taxpayer', COUNT(*), MAX(UpdatedDate) FROM Taxpayer
UNION ALL SELECT 'Owner', COUNT(*), MAX(UpdatedDate) FROM Owner
UNION ALL SELECT 'Officer', COUNT(*), MAX(UpdatedDate) FROM Officer
UNION ALL SELECT 'MonthlyDeclaration', COUNT(*), MAX(UpdatedDate) FROM MonthlyDeclaration
UNION ALL SELECT 'AnnualDeclaration', COUNT(*), MAX(UpdatedDate) FROM AnnualDeclaration
UNION ALL SELECT 'Payment', COUNT(*), MAX(UpdatedDate) FROM Payment
ORDER BY 1;

PRINT '';
PRINT '╔══════════════════════════════════════════════════════════════╗';
PRINT '║  ✓ Source changes applied. Now run Master_Complete_Pipeline ║';
PRINT '║    (incremental) and then 11_Verify_IncrementalLoad.sql    ║';
PRINT '╚══════════════════════════════════════════════════════════════╝';
GO
