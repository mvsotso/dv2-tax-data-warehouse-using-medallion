-- =============================================
-- 06_ETL_Bronze_Procedures.sql
-- Bronze Layer (Raw Data Vault) Stored Procedures
-- Aligned with DV2_Complete_Architecture_Guide.docx
-- 9 Hub SPs + 9 Satellite SPs + 5 Link SPs = 23 total
-- Hash: HASHBYTES('SHA2_256', ...) → VARBINARY(32)
-- =============================================

USE DV_Bronze;
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  HUB STORED PROCEDURES (9)                               ║
-- ║  Pattern: INSERT WHERE NOT EXISTS (idempotent)            ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── HUB_Category (BK: CategoryID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Category
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Category (HUB_Category_HK, CategoryID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.CategoryID AS NVARCHAR(MAX))),
            stg.CategoryID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Category stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Category hub
            WHERE hub.HUB_Category_HK = HASHBYTES('SHA2_256', CAST(stg.CategoryID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Structure (BK: StructureID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Structure
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Structure (HUB_Structure_HK, StructureID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.StructureID AS NVARCHAR(MAX))),
            stg.StructureID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Structure stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Structure hub
            WHERE hub.HUB_Structure_HK = HASHBYTES('SHA2_256', CAST(stg.StructureID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Activity (BK: ActivityID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Activity
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Activity (HUB_Activity_HK, ActivityID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.ActivityID AS NVARCHAR(MAX))),
            stg.ActivityID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Activity stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Activity hub
            WHERE hub.HUB_Activity_HK = HASHBYTES('SHA2_256', CAST(stg.ActivityID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Taxpayer (BK: TaxID VARCHAR(20)) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Taxpayer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Taxpayer (HUB_Taxpayer_HK, TaxID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg.TaxID AS NVARCHAR(MAX))))),
            stg.TaxID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Taxpayer stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Taxpayer hub
            WHERE hub.HUB_Taxpayer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg.TaxID AS NVARCHAR(MAX)))))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Owner (BK: OwnerID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Owner
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Owner (HUB_Owner_HK, OwnerID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.OwnerID AS NVARCHAR(MAX))),
            stg.OwnerID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Owner stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Owner hub
            WHERE hub.HUB_Owner_HK = HASHBYTES('SHA2_256', CAST(stg.OwnerID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Officer (BK: OfficerCode VARCHAR(20)) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Officer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Officer (HUB_Officer_HK, OfficerCode, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg.OfficerCode AS NVARCHAR(MAX))))),
            stg.OfficerCode, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Officer stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Officer hub
            WHERE hub.HUB_Officer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg.OfficerCode AS NVARCHAR(MAX)))))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Declaration (BK: DeclarationID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Declaration
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Declaration (HUB_Declaration_HK, DeclarationID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX))),
            stg.DeclarationID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_MonthlyDeclaration stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Declaration hub
            WHERE hub.HUB_Declaration_HK = HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_Payment (BK: PaymentID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_Payment
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_Payment (HUB_Payment_HK, PaymentID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.PaymentID AS NVARCHAR(MAX))),
            stg.PaymentID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Payment stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_Payment hub
            WHERE hub.HUB_Payment_HK = HASHBYTES('SHA2_256', CAST(stg.PaymentID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── HUB_AnnualDecl (BK: AnnualDeclarationID INT) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_HUB_AnnualDecl
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.HUB_AnnualDecl (HUB_AnnualDecl_HK, AnnualDeclarationID, HUB_LoadDate, HUB_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CAST(stg.AnnualDeclarationID AS NVARCHAR(MAX))),
            stg.AnnualDeclarationID, GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_AnnualDeclaration stg
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.HUB_AnnualDecl hub
            WHERE hub.HUB_AnnualDecl_HK = HASHBYTES('SHA2_256', CAST(stg.AnnualDeclarationID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 9 Hub procedures created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  SATELLITE STORED PROCEDURES (9)                          ║
-- ║  Pattern: CTE(hash) → UPDATE end-date → INSERT new       ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── SAT_Category ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Category
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT
                HASHBYTES('SHA2_256', CAST(CategoryID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CategoryName,''), '|',
                    ISNULL(CategoryDescription,''), '|',
                    ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD,
                CategoryName, CategoryDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Category
        )
        -- End-date changed records
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Category sat
        INNER JOIN StgHashed src ON sat.HUB_Category_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT
                HASHBYTES('SHA2_256', CAST(CategoryID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CategoryName,''), '|',
                    ISNULL(CategoryDescription,''), '|',
                    ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD,
                CategoryName, CategoryDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Category
        )
        INSERT INTO DV_Bronze.dbo.SAT_Category
            (HUB_Category_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
             CategoryName, CategoryDescription, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
               src.CategoryName, src.CategoryDescription, src.IsActive
        FROM StgHashed src
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Category sat
            WHERE sat.HUB_Category_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── SAT_Structure ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Structure
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(StructureID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(StructureName,''),'|',ISNULL(StructureDescription,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD,
                StructureName, StructureDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Structure
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Structure sat INNER JOIN StgHashed src ON sat.HUB_Structure_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(StructureID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(StructureName,''),'|',ISNULL(StructureDescription,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD,
                StructureName, StructureDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Structure
        )
        INSERT INTO DV_Bronze.dbo.SAT_Structure (HUB_Structure_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource, StructureName, StructureDescription, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource, src.StructureName, src.StructureDescription, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Structure sat WHERE sat.HUB_Structure_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_Activity ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Activity
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(ActivityID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(ActivityName,''),'|',ISNULL(ActivityDescription,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD,
                ActivityName, ActivityDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Activity
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Activity sat INNER JOIN StgHashed src ON sat.HUB_Activity_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(ActivityID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(ActivityName,''),'|',ISNULL(ActivityDescription,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD,
                ActivityName, ActivityDescription, IsActive, RecordSource
            FROM DV_Staging.dbo.STG_Activity
        )
        INSERT INTO DV_Bronze.dbo.SAT_Activity (HUB_Activity_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource, ActivityName, ActivityDescription, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource, src.ActivityName, src.ActivityDescription, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Activity sat WHERE sat.HUB_Activity_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_Taxpayer ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Taxpayer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', UPPER(TRIM(CAST(TaxID AS NVARCHAR(MAX))))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(LegalBusinessName,''),'|',ISNULL(TradingName,''),'|',
                    ISNULL(CAST(CategoryID AS NVARCHAR(20)),''),'|',ISNULL(CAST(StructureID AS NVARCHAR(20)),''),'|',
                    ISNULL(CAST(RegistrationDate AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(EstimatedAnnualRevenue AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_Taxpayer
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Taxpayer sat INNER JOIN StgHashed src ON sat.HUB_Taxpayer_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', UPPER(TRIM(CAST(TaxID AS NVARCHAR(MAX))))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(LegalBusinessName,''),'|',ISNULL(TradingName,''),'|',
                    ISNULL(CAST(CategoryID AS NVARCHAR(20)),''),'|',ISNULL(CAST(StructureID AS NVARCHAR(20)),''),'|',
                    ISNULL(CAST(RegistrationDate AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(EstimatedAnnualRevenue AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_Taxpayer
        )
        INSERT INTO DV_Bronze.dbo.SAT_Taxpayer (HUB_Taxpayer_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            LegalBusinessName, TradingName, CategoryID, StructureID, RegistrationDate, EstimatedAnnualRevenue, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.LegalBusinessName, src.TradingName, src.CategoryID, src.StructureID, src.RegistrationDate, src.EstimatedAnnualRevenue, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Taxpayer sat WHERE sat.HUB_Taxpayer_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_Owner ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Owner
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(OwnerID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(OwnerName,''),'|',ISNULL(OwnerType,''),'|',ISNULL(CAST(OwnershipPercentage AS NVARCHAR(10)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD, *
            FROM DV_Staging.dbo.STG_Owner
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Owner sat INNER JOIN StgHashed src ON sat.HUB_Owner_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(OwnerID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(OwnerName,''),'|',ISNULL(OwnerType,''),'|',ISNULL(CAST(OwnershipPercentage AS NVARCHAR(10)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD, *
            FROM DV_Staging.dbo.STG_Owner
        )
        INSERT INTO DV_Bronze.dbo.SAT_Owner (HUB_Owner_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            TaxpayerID, OwnerName, OwnerType, OwnershipPercentage, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.TaxpayerID, src.OwnerName, src.OwnerType, src.OwnershipPercentage, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Owner sat WHERE sat.HUB_Owner_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_Officer ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Officer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', UPPER(TRIM(CAST(OfficerCode AS NVARCHAR(MAX))))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(FirstName,''),'|',ISNULL(LastName,''),'|',ISNULL(Department,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD, *
            FROM DV_Staging.dbo.STG_Officer
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Officer sat INNER JOIN StgHashed src ON sat.HUB_Officer_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', UPPER(TRIM(CAST(OfficerCode AS NVARCHAR(MAX))))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(FirstName,''),'|',ISNULL(LastName,''),'|',ISNULL(Department,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),''))) AS HD, *
            FROM DV_Staging.dbo.STG_Officer
        )
        INSERT INTO DV_Bronze.dbo.SAT_Officer (HUB_Officer_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            OfficerID, FirstName, LastName, Department, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.OfficerID, src.FirstName, src.LastName, src.Department, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Officer sat WHERE sat.HUB_Officer_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_MonthlyDecl ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_MonthlyDecl
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(DeclarationID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationMonth AS NVARCHAR(2)),''),'|',ISNULL(CAST(DeclarationYear AS NVARCHAR(4)),''),'|',
                    ISNULL(CAST(GrossRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxableRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(PenaltyAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(InterestAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(TotalAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(DeclarationDate AS NVARCHAR(30)),''),'|',ISNULL(CAST(DueDate AS NVARCHAR(30)),''),'|',ISNULL(Status,''),'|',
                    ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_MonthlyDeclaration
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_MonthlyDecl sat INNER JOIN StgHashed src ON sat.HUB_Declaration_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(DeclarationID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationMonth AS NVARCHAR(2)),''),'|',ISNULL(CAST(DeclarationYear AS NVARCHAR(4)),''),'|',
                    ISNULL(CAST(GrossRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxableRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(PenaltyAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(InterestAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(TotalAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(DeclarationDate AS NVARCHAR(30)),''),'|',ISNULL(CAST(DueDate AS NVARCHAR(30)),''),'|',ISNULL(Status,''),'|',
                    ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_MonthlyDeclaration
        )
        INSERT INTO DV_Bronze.dbo.SAT_MonthlyDecl (HUB_Declaration_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            TaxpayerID, DeclarationMonth, DeclarationYear, GrossRevenue, TaxableRevenue, TaxAmount,
            PenaltyAmount, InterestAmount, TotalAmount, DeclarationDate, DueDate, Status, OfficerID, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.TaxpayerID, src.DeclarationMonth, src.DeclarationYear, src.GrossRevenue, src.TaxableRevenue, src.TaxAmount,
            src.PenaltyAmount, src.InterestAmount, src.TotalAmount, src.DeclarationDate, src.DueDate, src.Status, src.OfficerID, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_MonthlyDecl sat WHERE sat.HUB_Declaration_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_Payment ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_Payment
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(PaymentID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationID AS NVARCHAR(20)),''),'|',ISNULL(CAST(AnnualDeclarationID AS NVARCHAR(20)),''),'|',
                    ISNULL(CAST(PaymentAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(PaymentDate AS NVARCHAR(30)),''),'|',ISNULL(PaymentMethod,''),'|',
                    ISNULL(ReferenceNumber,''),'|',ISNULL(Status,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_Payment
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_Payment sat INNER JOIN StgHashed src ON sat.HUB_Payment_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(PaymentID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationID AS NVARCHAR(20)),''),'|',ISNULL(CAST(AnnualDeclarationID AS NVARCHAR(20)),''),'|',
                    ISNULL(CAST(PaymentAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(PaymentDate AS NVARCHAR(30)),''),'|',ISNULL(PaymentMethod,''),'|',
                    ISNULL(ReferenceNumber,''),'|',ISNULL(Status,''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_Payment
        )
        INSERT INTO DV_Bronze.dbo.SAT_Payment (HUB_Payment_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            TaxpayerID, DeclarationID, AnnualDeclarationID, PaymentAmount, PaymentDate, PaymentMethod, ReferenceNumber, Status, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.TaxpayerID, src.DeclarationID, src.AnnualDeclarationID, src.PaymentAmount, src.PaymentDate, src.PaymentMethod, src.ReferenceNumber, src.Status, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_Payment sat WHERE sat.HUB_Payment_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── SAT_AnnualDecl ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_SAT_AnnualDecl
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(AnnualDeclarationID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationYear AS NVARCHAR(4)),''),'|',
                    ISNULL(CAST(GrossRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxableRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(PenaltyAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(InterestAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(TotalAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(DeclarationDate AS NVARCHAR(30)),''),'|',ISNULL(CAST(DueDate AS NVARCHAR(30)),''),'|',ISNULL(Status,''),'|',
                    ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_AnnualDeclaration
        )
        UPDATE sat SET sat.SAT_EndDate = GETDATE()
        FROM DV_Bronze.dbo.SAT_AnnualDecl sat INNER JOIN StgHashed src ON sat.HUB_AnnualDecl_HK = src.HK
        WHERE sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff <> src.HD;

        ;WITH StgHashed AS (
            SELECT HASHBYTES('SHA2_256', CAST(AnnualDeclarationID AS NVARCHAR(MAX))) AS HK,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(CAST(TaxpayerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(DeclarationYear AS NVARCHAR(4)),''),'|',
                    ISNULL(CAST(GrossRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxableRevenue AS NVARCHAR(30)),''),'|',ISNULL(CAST(TaxAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(PenaltyAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(InterestAmount AS NVARCHAR(30)),''),'|',ISNULL(CAST(TotalAmount AS NVARCHAR(30)),''),'|',
                    ISNULL(CAST(DeclarationDate AS NVARCHAR(30)),''),'|',ISNULL(CAST(DueDate AS NVARCHAR(30)),''),'|',ISNULL(Status,''),'|',
                    ISNULL(CAST(OfficerID AS NVARCHAR(20)),''),'|',ISNULL(CAST(IsActive AS NVARCHAR(1)),'')
                )) AS HD, *
            FROM DV_Staging.dbo.STG_AnnualDeclaration
        )
        INSERT INTO DV_Bronze.dbo.SAT_AnnualDecl (HUB_AnnualDecl_HK, SAT_LoadDate, SAT_EndDate, SAT_HashDiff, SAT_RecordSource,
            TaxpayerID, DeclarationYear, GrossRevenue, TaxableRevenue, TaxAmount, PenaltyAmount, InterestAmount, TotalAmount,
            DeclarationDate, DueDate, Status, OfficerID, IsActive)
        SELECT src.HK, GETDATE(), NULL, src.HD, src.RecordSource,
            src.TaxpayerID, src.DeclarationYear, src.GrossRevenue, src.TaxableRevenue, src.TaxAmount, src.PenaltyAmount, src.InterestAmount, src.TotalAmount,
            src.DeclarationDate, src.DueDate, src.Status, src.OfficerID, src.IsActive
        FROM StgHashed src WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.SAT_AnnualDecl sat WHERE sat.HUB_AnnualDecl_HK = src.HK AND sat.SAT_EndDate IS NULL AND sat.SAT_HashDiff = src.HD
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

PRINT '=== 9 Satellite procedures created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  LINK STORED PROCEDURES (5)                               ║
-- ║  Pattern: Composite hash + INSERT WHERE NOT EXISTS        ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── LNK_TaxpayerDeclaration (STG_MonthlyDeclaration: TaxpayerID → HUB_Taxpayer, DeclarationID → HUB_Declaration) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_LNK_TaxpayerDeclaration
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.LNK_TaxpayerDeclaration
            (LNK_TaxpayerDeclaration_HK, HUB_Taxpayer_HK, HUB_Declaration_HK, LNK_LoadDate, LNK_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CONCAT(
                CAST(tp.HUB_Taxpayer_HK AS NVARCHAR(100)), '|',
                CAST(HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX))) AS NVARCHAR(100))
            )),
            tp.HUB_Taxpayer_HK,
            HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX))),
            GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_MonthlyDeclaration stg
        INNER JOIN DV_Staging.dbo.STG_Taxpayer stg_tp ON stg.TaxpayerID = stg_tp.TaxpayerID
        INNER JOIN DV_Bronze.dbo.HUB_Taxpayer tp
            ON tp.HUB_Taxpayer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg_tp.TaxID AS NVARCHAR(MAX)))))
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
            WHERE lnk.HUB_Taxpayer_HK = tp.HUB_Taxpayer_HK
              AND lnk.HUB_Declaration_HK = HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── LNK_DeclarationPayment ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_LNK_DeclarationPayment
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.LNK_DeclarationPayment
            (LNK_DeclarationPayment_HK, HUB_Declaration_HK, HUB_Payment_HK, LNK_LoadDate, LNK_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CONCAT(
                CAST(HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX))) AS NVARCHAR(100)), '|',
                CAST(HASHBYTES('SHA2_256', CAST(stg.PaymentID AS NVARCHAR(MAX))) AS NVARCHAR(100))
            )),
            HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX))),
            HASHBYTES('SHA2_256', CAST(stg.PaymentID AS NVARCHAR(MAX))),
            GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Payment stg
        WHERE stg.DeclarationID IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.LNK_DeclarationPayment lnk
            WHERE lnk.HUB_Declaration_HK = HASHBYTES('SHA2_256', CAST(stg.DeclarationID AS NVARCHAR(MAX)))
              AND lnk.HUB_Payment_HK = HASHBYTES('SHA2_256', CAST(stg.PaymentID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── LNK_TaxpayerOfficer (from STG_MonthlyDeclaration: TaxpayerID + OfficerID) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_LNK_TaxpayerOfficer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.LNK_TaxpayerOfficer
            (LNK_TaxpayerOfficer_HK, HUB_Taxpayer_HK, HUB_Officer_HK, LNK_LoadDate, LNK_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CONCAT(
                CAST(tp.HUB_Taxpayer_HK AS NVARCHAR(100)), '|',
                CAST(ofc.HUB_Officer_HK AS NVARCHAR(100))
            )),
            tp.HUB_Taxpayer_HK, ofc.HUB_Officer_HK,
            GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_MonthlyDeclaration stg
        INNER JOIN DV_Staging.dbo.STG_Taxpayer stg_tp ON stg.TaxpayerID = stg_tp.TaxpayerID
        INNER JOIN DV_Staging.dbo.STG_Officer stg_ofc ON stg.OfficerID = stg_ofc.OfficerID
        INNER JOIN DV_Bronze.dbo.HUB_Taxpayer tp
            ON tp.HUB_Taxpayer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg_tp.TaxID AS NVARCHAR(MAX)))))
        INNER JOIN DV_Bronze.dbo.HUB_Officer ofc
            ON ofc.HUB_Officer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg_ofc.OfficerCode AS NVARCHAR(MAX)))))
        WHERE stg.OfficerID IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.LNK_TaxpayerOfficer lnk
            WHERE lnk.HUB_Taxpayer_HK = tp.HUB_Taxpayer_HK AND lnk.HUB_Officer_HK = ofc.HUB_Officer_HK
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── LNK_TaxpayerOwner ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_LNK_TaxpayerOwner
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.LNK_TaxpayerOwner
            (LNK_TaxpayerOwner_HK, HUB_Taxpayer_HK, HUB_Owner_HK, LNK_LoadDate, LNK_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CONCAT(
                CAST(tp.HUB_Taxpayer_HK AS NVARCHAR(100)), '|',
                CAST(HASHBYTES('SHA2_256', CAST(stg.OwnerID AS NVARCHAR(MAX))) AS NVARCHAR(100))
            )),
            tp.HUB_Taxpayer_HK,
            HASHBYTES('SHA2_256', CAST(stg.OwnerID AS NVARCHAR(MAX))),
            GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_Owner stg
        INNER JOIN DV_Staging.dbo.STG_Taxpayer stg_tp ON stg.TaxpayerID = stg_tp.TaxpayerID
        INNER JOIN DV_Bronze.dbo.HUB_Taxpayer tp
            ON tp.HUB_Taxpayer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg_tp.TaxID AS NVARCHAR(MAX)))))
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.LNK_TaxpayerOwner lnk
            WHERE lnk.HUB_Taxpayer_HK = tp.HUB_Taxpayer_HK
              AND lnk.HUB_Owner_HK = HASHBYTES('SHA2_256', CAST(stg.OwnerID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

-- ─── LNK_TaxpayerAnnualDecl ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_LNK_TaxpayerAnnualDecl
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO DV_Bronze.dbo.LNK_TaxpayerAnnualDecl
            (LNK_TaxpayerAnnualDecl_HK, HUB_Taxpayer_HK, HUB_AnnualDecl_HK, LNK_LoadDate, LNK_RecordSource)
        SELECT DISTINCT
            HASHBYTES('SHA2_256', CONCAT(
                CAST(tp.HUB_Taxpayer_HK AS NVARCHAR(100)), '|',
                CAST(HASHBYTES('SHA2_256', CAST(stg.AnnualDeclarationID AS NVARCHAR(MAX))) AS NVARCHAR(100))
            )),
            tp.HUB_Taxpayer_HK,
            HASHBYTES('SHA2_256', CAST(stg.AnnualDeclarationID AS NVARCHAR(MAX))),
            GETDATE(), stg.RecordSource
        FROM DV_Staging.dbo.STG_AnnualDeclaration stg
        INNER JOIN DV_Staging.dbo.STG_Taxpayer stg_tp ON stg.TaxpayerID = stg_tp.TaxpayerID
        INNER JOIN DV_Bronze.dbo.HUB_Taxpayer tp
            ON tp.HUB_Taxpayer_HK = HASHBYTES('SHA2_256', UPPER(TRIM(CAST(stg_tp.TaxID AS NVARCHAR(MAX)))))
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Bronze.dbo.LNK_TaxpayerAnnualDecl lnk
            WHERE lnk.HUB_Taxpayer_HK = tp.HUB_Taxpayer_HK
              AND lnk.HUB_AnnualDecl_HK = HASHBYTES('SHA2_256', CAST(stg.AnnualDeclarationID AS NVARCHAR(MAX)))
        );
        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1); END CATCH
END;
GO

PRINT '=== 5 Link procedures created ===';
PRINT '=== DV_Bronze: Total 23 procedures (9 Hub + 9 Satellite + 5 Link) ===';
GO
