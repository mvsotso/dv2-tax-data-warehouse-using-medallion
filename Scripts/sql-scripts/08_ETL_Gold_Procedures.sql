-- =============================================
-- 08_ETL_Gold_Procedures.sql
-- Gold Layer (Star Schema) Stored Procedures
-- Aligned with DV2_Complete_Architecture_Guide.docx
-- 5 SCD1 Dimensions + 2 SCD2 Dimensions + 4 Facts = 11 total
-- Dimensions: @BatchID, @RowCount OUTPUT (no @SnapshotDate)
-- Facts: @BatchID, @SnapshotDate, @RowCount OUTPUT
-- =============================================

USE DV_Gold;
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  SCD TYPE 1 DIMENSION PROCEDURES (5)                      ║
-- ║  Pattern: MERGE — update changed + insert new             ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── DIM_Category (SCD1) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Category
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE DV_Gold.dbo.DIM_Category AS tgt
        USING (
            SELECT hub.HUB_Category_HK, hub.CategoryID,
                   sat.CategoryName, sat.CategoryDescription, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Category hub
            INNER JOIN DV_Bronze.dbo.SAT_Category sat
                ON hub.HUB_Category_HK = sat.HUB_Category_HK AND sat.SAT_EndDate IS NULL
        ) AS src ON tgt.HUB_Category_HK = src.HUB_Category_HK

        WHEN MATCHED AND (
            ISNULL(tgt.CategoryName,'') <> ISNULL(src.CategoryName,'')
            OR ISNULL(tgt.CategoryDescription,'') <> ISNULL(src.CategoryDescription,'')
            OR ISNULL(tgt.IsActive, 0) <> ISNULL(src.IsActive, 0)
        ) THEN UPDATE SET
            tgt.CategoryName = src.CategoryName,
            tgt.CategoryDescription = src.CategoryDescription,
            tgt.IsActive = src.IsActive,
            tgt.GLD_LoadDate = GETDATE()

        WHEN NOT MATCHED THEN INSERT
            (CategoryID, HUB_Category_HK, CategoryName, CategoryDescription, IsActive, GLD_LoadDate)
        VALUES
            (src.CategoryID, src.HUB_Category_HK, src.CategoryName, src.CategoryDescription, src.IsActive, GETDATE());

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── DIM_Structure (SCD1) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Structure
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE DV_Gold.dbo.DIM_Structure AS tgt
        USING (
            SELECT hub.HUB_Structure_HK, hub.StructureID,
                   sat.StructureName, sat.StructureDescription, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Structure hub
            INNER JOIN DV_Bronze.dbo.SAT_Structure sat
                ON hub.HUB_Structure_HK = sat.HUB_Structure_HK AND sat.SAT_EndDate IS NULL
        ) AS src ON tgt.HUB_Structure_HK = src.HUB_Structure_HK

        WHEN MATCHED AND (
            ISNULL(tgt.StructureName,'') <> ISNULL(src.StructureName,'')
            OR ISNULL(tgt.StructureDescription,'') <> ISNULL(src.StructureDescription,'')
            OR ISNULL(tgt.IsActive, 0) <> ISNULL(src.IsActive, 0)
        ) THEN UPDATE SET
            tgt.StructureName = src.StructureName,
            tgt.StructureDescription = src.StructureDescription,
            tgt.IsActive = src.IsActive,
            tgt.GLD_LoadDate = GETDATE()

        WHEN NOT MATCHED THEN INSERT
            (StructureID, HUB_Structure_HK, StructureName, StructureDescription, IsActive, GLD_LoadDate)
        VALUES
            (src.StructureID, src.HUB_Structure_HK, src.StructureName, src.StructureDescription, src.IsActive, GETDATE());

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── DIM_Activity (SCD1) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Activity
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE DV_Gold.dbo.DIM_Activity AS tgt
        USING (
            SELECT hub.HUB_Activity_HK, hub.ActivityID,
                   sat.ActivityName, sat.ActivityDescription, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Activity hub
            INNER JOIN DV_Bronze.dbo.SAT_Activity sat
                ON hub.HUB_Activity_HK = sat.HUB_Activity_HK AND sat.SAT_EndDate IS NULL
        ) AS src ON tgt.HUB_Activity_HK = src.HUB_Activity_HK

        WHEN MATCHED AND (
            ISNULL(tgt.ActivityName,'') <> ISNULL(src.ActivityName,'')
            OR ISNULL(tgt.ActivityDescription,'') <> ISNULL(src.ActivityDescription,'')
            OR ISNULL(tgt.IsActive, 0) <> ISNULL(src.IsActive, 0)
        ) THEN UPDATE SET
            tgt.ActivityName = src.ActivityName,
            tgt.ActivityDescription = src.ActivityDescription,
            tgt.IsActive = src.IsActive,
            tgt.GLD_LoadDate = GETDATE()

        WHEN NOT MATCHED THEN INSERT
            (ActivityID, HUB_Activity_HK, ActivityName, ActivityDescription, IsActive, GLD_LoadDate)
        VALUES
            (src.ActivityID, src.HUB_Activity_HK, src.ActivityName, src.ActivityDescription, src.IsActive, GETDATE());

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── DIM_PaymentMethod (SCD1) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_PaymentMethod
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        MERGE DV_Gold.dbo.DIM_PaymentMethod AS tgt
        USING (
            SELECT DISTINCT sp.PaymentMethod
            FROM DV_Bronze.dbo.SAT_Payment sp
            WHERE sp.SAT_EndDate IS NULL
              AND sp.PaymentMethod IS NOT NULL
        ) AS src ON tgt.PaymentMethod = src.PaymentMethod

        WHEN NOT MATCHED THEN INSERT (PaymentMethod, GLD_LoadDate)
        VALUES (src.PaymentMethod, GETDATE());

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── DIM_Status (SCD1) ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Status
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Collect all distinct status values across declaration and payment SATs
        ;WITH AllStatuses AS (
            SELECT DISTINCT Status AS StatusCode FROM DV_Bronze.dbo.SAT_MonthlyDecl WHERE SAT_EndDate IS NULL AND Status IS NOT NULL
            UNION
            SELECT DISTINCT Status FROM DV_Bronze.dbo.SAT_Payment WHERE SAT_EndDate IS NULL AND Status IS NOT NULL
            UNION
            SELECT DISTINCT Status FROM DV_Bronze.dbo.SAT_AnnualDecl WHERE SAT_EndDate IS NULL AND Status IS NOT NULL
        )
        MERGE DV_Gold.dbo.DIM_Status AS tgt
        USING AllStatuses AS src ON tgt.StatusCode = src.StatusCode

        WHEN NOT MATCHED THEN INSERT (StatusCode, GLD_LoadDate)
        VALUES (src.StatusCode, GETDATE());

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 5 SCD Type 1 dimension procedures created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  SCD TYPE 2 DIMENSION PROCEDURES (2)                      ║
-- ║  Pattern: UPDATE (close old) → INSERT (new version)       ║
-- ║  Tracked attributes trigger new rows with surrogate key   ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── DIM_Taxpayer (SCD2) ───
-- Tracked: LegalBusinessName, TradingName, EstimatedAnnualRevenue, IsActive
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Taxpayer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- CTE: current Bronze data (Hub + current Satellite)
        ;WITH SourceData AS (
            SELECT
                hub.HUB_Taxpayer_HK, hub.TaxID,
                sat.LegalBusinessName, sat.TradingName,
                sat.RegistrationDate, sat.EstimatedAnnualRevenue, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Taxpayer hub
            INNER JOIN DV_Bronze.dbo.SAT_Taxpayer sat
                ON hub.HUB_Taxpayer_HK = sat.HUB_Taxpayer_HK
                AND sat.SAT_EndDate IS NULL
        )

        -- 1. Close expired records (attributes changed)
        UPDATE dim
        SET dim.ExpiryDate = GETDATE(),
            dim.IsCurrent = 0
        FROM DV_Gold.dbo.DIM_Taxpayer dim
        INNER JOIN SourceData src
            ON dim.HUB_Taxpayer_HK = src.HUB_Taxpayer_HK
        WHERE dim.IsCurrent = 1
          AND (
              ISNULL(dim.LegalBusinessName,'') <> ISNULL(src.LegalBusinessName,'')
              OR ISNULL(dim.TradingName,'') <> ISNULL(src.TradingName,'')
              OR ISNULL(dim.EstimatedAnnualRevenue, 0) <> ISNULL(src.EstimatedAnnualRevenue, 0)
              OR ISNULL(dim.IsActive, 0) <> ISNULL(src.IsActive, 0)
          );

        -- 2. Insert new versions for changed + brand-new records
        ;WITH SourceData AS (
            SELECT
                hub.HUB_Taxpayer_HK, hub.TaxID,
                sat.LegalBusinessName, sat.TradingName,
                sat.RegistrationDate, sat.EstimatedAnnualRevenue, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Taxpayer hub
            INNER JOIN DV_Bronze.dbo.SAT_Taxpayer sat
                ON hub.HUB_Taxpayer_HK = sat.HUB_Taxpayer_HK
                AND sat.SAT_EndDate IS NULL
        )
        INSERT INTO DV_Gold.dbo.DIM_Taxpayer
            (TaxID, HUB_Taxpayer_HK, LegalBusinessName, TradingName,
             RegistrationDate, EstimatedAnnualRevenue, IsActive,
             EffectiveDate, ExpiryDate, IsCurrent, GLD_LoadDate, GLD_BatchID)
        SELECT
            src.TaxID, src.HUB_Taxpayer_HK,
            src.LegalBusinessName, src.TradingName,
            src.RegistrationDate, src.EstimatedAnnualRevenue, src.IsActive,
            GETDATE(), NULL, 1, GETDATE(), @BatchID
        FROM SourceData src
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Gold.dbo.DIM_Taxpayer dim
            WHERE dim.HUB_Taxpayer_HK = src.HUB_Taxpayer_HK AND dim.IsCurrent = 1
        );

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── DIM_Officer (SCD2) ───
-- Tracked: FirstName, LastName, Department, IsActive
CREATE OR ALTER PROCEDURE dbo.usp_Load_DIM_Officer
    @BatchID   INT,
    @RowCount  INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        ;WITH SourceData AS (
            SELECT
                hub.HUB_Officer_HK, hub.OfficerCode,
                sat.FirstName, sat.LastName, sat.Department, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Officer hub
            INNER JOIN DV_Bronze.dbo.SAT_Officer sat
                ON hub.HUB_Officer_HK = sat.HUB_Officer_HK
                AND sat.SAT_EndDate IS NULL
        )
        UPDATE dim
        SET dim.ExpiryDate = GETDATE(), dim.IsCurrent = 0
        FROM DV_Gold.dbo.DIM_Officer dim
        INNER JOIN SourceData src ON dim.HUB_Officer_HK = src.HUB_Officer_HK
        WHERE dim.IsCurrent = 1
          AND (
              ISNULL(dim.FirstName,'') <> ISNULL(src.FirstName,'')
              OR ISNULL(dim.LastName,'') <> ISNULL(src.LastName,'')
              OR ISNULL(dim.Department,'') <> ISNULL(src.Department,'')
              OR ISNULL(dim.IsActive, 0) <> ISNULL(src.IsActive, 0)
          );

        ;WITH SourceData AS (
            SELECT
                hub.HUB_Officer_HK, hub.OfficerCode,
                sat.FirstName, sat.LastName, sat.Department, sat.IsActive
            FROM DV_Bronze.dbo.HUB_Officer hub
            INNER JOIN DV_Bronze.dbo.SAT_Officer sat
                ON hub.HUB_Officer_HK = sat.HUB_Officer_HK
                AND sat.SAT_EndDate IS NULL
        )
        INSERT INTO DV_Gold.dbo.DIM_Officer
            (OfficerCode, HUB_Officer_HK, FirstName, LastName, Department, IsActive,
             EffectiveDate, ExpiryDate, IsCurrent, GLD_LoadDate, GLD_BatchID)
        SELECT
            src.OfficerCode, src.HUB_Officer_HK,
            src.FirstName, src.LastName, src.Department, src.IsActive,
            GETDATE(), NULL, 1, GETDATE(), @BatchID
        FROM SourceData src
        WHERE NOT EXISTS (
            SELECT 1 FROM DV_Gold.dbo.DIM_Officer dim
            WHERE dim.HUB_Officer_HK = src.HUB_Officer_HK AND dim.IsCurrent = 1
        );

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 2 SCD Type 2 dimension procedures created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  FACT STORED PROCEDURES (4)                               ║
-- ║  Pattern: DELETE snapshot → INSERT with DIM SK lookups     ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── FACT_MonthlyDeclaration ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_FACT_MonthlyDeclaration
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Gold.dbo.FACT_MonthlyDeclaration
        WHERE GLD_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Gold.dbo.FACT_MonthlyDeclaration
            (DIM_Taxpayer_SK, DIM_Category_SK, DIM_Status_SK,
             DeclarationID, DeclarationMonth, DeclarationYear,
             GrossRevenue, TaxableRevenue, TaxAmount,
             PenaltyAmount, InterestAmount, TotalAmount,
             GLD_LoadDate, GLD_BatchID, GLD_SnapshotDate)
        SELECT
            dt.DIM_Taxpayer_SK,
            dc.DIM_Category_SK,
            ds.DIM_Status_SK,
            hd.DeclarationID,
            sd.DeclarationMonth, sd.DeclarationYear,
            sd.GrossRevenue, sd.TaxableRevenue, sd.TaxAmount,
            sd.PenaltyAmount, sd.InterestAmount, sd.TotalAmount,
            GETDATE(), @BatchID, @SnapshotDate
        FROM DV_Bronze.dbo.SAT_MonthlyDecl sd
        INNER JOIN DV_Bronze.dbo.HUB_Declaration hd
            ON sd.HUB_Declaration_HK = hd.HUB_Declaration_HK
        -- Resolve Taxpayer via Link
        INNER JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
            ON hd.HUB_Declaration_HK = lnk.HUB_Declaration_HK
        -- DIM SK lookups
        INNER JOIN DV_Gold.dbo.DIM_Taxpayer dt
            ON lnk.HUB_Taxpayer_HK = dt.HUB_Taxpayer_HK AND dt.IsCurrent = 1
        -- Category via SAT_Taxpayer
        LEFT JOIN DV_Bronze.dbo.SAT_Taxpayer sat_tp
            ON lnk.HUB_Taxpayer_HK = sat_tp.HUB_Taxpayer_HK AND sat_tp.SAT_EndDate IS NULL
        LEFT JOIN DV_Gold.dbo.DIM_Category dc
            ON dc.CategoryID = sat_tp.CategoryID
        LEFT JOIN DV_Gold.dbo.DIM_Status ds
            ON ds.StatusCode = sd.Status
        WHERE sd.SAT_EndDate IS NULL;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── FACT_Payment ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_FACT_Payment
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Gold.dbo.FACT_Payment
        WHERE GLD_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Gold.dbo.FACT_Payment
            (DIM_Taxpayer_SK, DIM_PaymentMethod_SK, DIM_Status_SK,
             PaymentID, DeclarationID, AnnualDeclarationID,
             PaymentAmount, PaymentDate,
             GLD_LoadDate, GLD_BatchID, GLD_SnapshotDate)
        SELECT
            dt.DIM_Taxpayer_SK,
            dpm.DIM_PaymentMethod_SK,
            ds.DIM_Status_SK,
            hp.PaymentID,
            sp.DeclarationID, sp.AnnualDeclarationID,
            sp.PaymentAmount, sp.PaymentDate,
            GETDATE(), @BatchID, @SnapshotDate
        FROM DV_Bronze.dbo.SAT_Payment sp
        INNER JOIN DV_Bronze.dbo.HUB_Payment hp
            ON sp.HUB_Payment_HK = hp.HUB_Payment_HK
        -- Resolve Taxpayer: Payment.TaxpayerID → STG_Taxpayer.TaxID → HUB_Taxpayer
        LEFT JOIN DV_Bronze.dbo.SAT_Taxpayer sat_tp
            ON HASHBYTES('SHA2_256', UPPER(TRIM(CAST(
                (SELECT TOP 1 t.TaxID FROM DV_Staging.dbo.STG_Taxpayer t WHERE t.TaxpayerID = sp.TaxpayerID) AS NVARCHAR(MAX)
            )))) = sat_tp.HUB_Taxpayer_HK AND sat_tp.SAT_EndDate IS NULL
        LEFT JOIN DV_Gold.dbo.DIM_Taxpayer dt
            ON dt.HUB_Taxpayer_HK = sat_tp.HUB_Taxpayer_HK AND dt.IsCurrent = 1
        LEFT JOIN DV_Gold.dbo.DIM_PaymentMethod dpm
            ON dpm.PaymentMethod = sp.PaymentMethod
        LEFT JOIN DV_Gold.dbo.DIM_Status ds
            ON ds.StatusCode = sp.Status
        WHERE sp.SAT_EndDate IS NULL;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── FACT_MonthlySnapshot ───
-- Sources from BUS_MonthlyMetrics (Silver layer)
CREATE OR ALTER PROCEDURE dbo.usp_Load_FACT_MonthlySnapshot
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Gold.dbo.FACT_MonthlySnapshot
        WHERE GLD_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Gold.dbo.FACT_MonthlySnapshot
            (DIM_Taxpayer_SK, MetricMonth, MetricYear,
             TotalDeclarations, TotalGrossRevenue, TotalTaxAmount,
             TotalPayments, TotalPaymentAmount, PaymentCoverageRate,
             GLD_LoadDate, GLD_BatchID, GLD_SnapshotDate)
        SELECT
            dt.DIM_Taxpayer_SK,
            bm.MetricMonth, bm.MetricYear,
            bm.TotalDeclarations, bm.TotalGrossRevenue, bm.TotalTaxAmount,
            bm.TotalPayments, bm.TotalPaymentAmount, bm.PaymentCoverageRate,
            GETDATE(), @BatchID, @SnapshotDate
        FROM DV_Silver.dbo.BUS_MonthlyMetrics bm
        INNER JOIN DV_Gold.dbo.DIM_Taxpayer dt
            ON bm.HUB_Taxpayer_HK = dt.HUB_Taxpayer_HK AND dt.IsCurrent = 1
        WHERE bm.SnapshotDate = @SnapshotDate;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── FACT_DeclarationLifecycle ───
-- Tracks status change events from SAT_MonthlyDecl history
CREATE OR ALTER PROCEDURE dbo.usp_Load_FACT_DeclarationLifecycle
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Gold.dbo.FACT_DeclarationLifecycle
        WHERE GLD_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Gold.dbo.FACT_DeclarationLifecycle
            (DIM_Taxpayer_SK, DIM_Status_SK, DeclarationID,
             LifecycleEvent, EventDate,
             GLD_LoadDate, GLD_BatchID, GLD_SnapshotDate)
        SELECT
            dt.DIM_Taxpayer_SK,
            ds.DIM_Status_SK,
            hd.DeclarationID,
            sd.Status               AS LifecycleEvent,
            sd.SAT_LoadDate         AS EventDate,
            GETDATE(), @BatchID, @SnapshotDate
        FROM DV_Bronze.dbo.SAT_MonthlyDecl sd
        INNER JOIN DV_Bronze.dbo.HUB_Declaration hd
            ON sd.HUB_Declaration_HK = hd.HUB_Declaration_HK
        INNER JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
            ON hd.HUB_Declaration_HK = lnk.HUB_Declaration_HK
        INNER JOIN DV_Gold.dbo.DIM_Taxpayer dt
            ON lnk.HUB_Taxpayer_HK = dt.HUB_Taxpayer_HK AND dt.IsCurrent = 1
        LEFT JOIN DV_Gold.dbo.DIM_Status ds
            ON ds.StatusCode = sd.Status;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE(); RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 4 Fact procedures created ===';
PRINT '=== DV_Gold: Total 11 procedures (5 SCD1 + 2 SCD2 + 4 Fact) ===';
GO
