-- =============================================
-- 07_ETL_Silver_Procedures.sql
-- Silver Layer (Business Vault) Stored Procedures
-- Aligned with DV2_Complete_Architecture_Guide.docx
-- 3 PIT + 1 Bridge + 2 Business Vault = 6 total
-- All SPs: DELETE + INSERT pattern with @SnapshotDate
-- =============================================

USE DV_Silver;
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  PIT (Point-In-Time) STORED PROCEDURES (3)                ║
-- ║  Pattern: DELETE snapshot → INSERT via CROSS APPLY         ║
-- ║  Stores SAT_LoadDate pointer, not full attributes          ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── PIT_Taxpayer ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_PIT_Taxpayer
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- DELETE existing snapshot for idempotency
        DELETE FROM DV_Silver.dbo.PIT_Taxpayer
        WHERE PIT_SnapshotDate = @SnapshotDate;

        -- INSERT: find latest SAT_LoadDate <= @SnapshotDate for each Hub record
        INSERT INTO DV_Silver.dbo.PIT_Taxpayer
            (PIT_Taxpayer_HK, PIT_SnapshotDate, SAT_Taxpayer_LoadDate, PIT_LoadDate, PIT_BatchID)
        SELECT
            hub.HUB_Taxpayer_HK,
            @SnapshotDate,
            sat_latest.SAT_LoadDate,
            GETDATE(),
            @BatchID
        FROM DV_Bronze.dbo.HUB_Taxpayer hub
        CROSS APPLY (
            SELECT TOP 1 s.SAT_LoadDate
            FROM DV_Bronze.dbo.SAT_Taxpayer s
            WHERE s.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
              AND s.SAT_LoadDate <= @SnapshotDate
            ORDER BY s.SAT_LoadDate DESC
        ) sat_latest;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── PIT_Declaration ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_PIT_Declaration
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Silver.dbo.PIT_Declaration
        WHERE PIT_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Silver.dbo.PIT_Declaration
            (PIT_Declaration_HK, PIT_SnapshotDate, SAT_MonthlyDecl_LoadDate, PIT_LoadDate, PIT_BatchID)
        SELECT
            hub.HUB_Declaration_HK,
            @SnapshotDate,
            sat_latest.SAT_LoadDate,
            GETDATE(),
            @BatchID
        FROM DV_Bronze.dbo.HUB_Declaration hub
        CROSS APPLY (
            SELECT TOP 1 s.SAT_LoadDate
            FROM DV_Bronze.dbo.SAT_MonthlyDecl s
            WHERE s.HUB_Declaration_HK = hub.HUB_Declaration_HK
              AND s.SAT_LoadDate <= @SnapshotDate
            ORDER BY s.SAT_LoadDate DESC
        ) sat_latest;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── PIT_Payment ───
CREATE OR ALTER PROCEDURE dbo.usp_Load_PIT_Payment
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Silver.dbo.PIT_Payment
        WHERE PIT_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Silver.dbo.PIT_Payment
            (PIT_Payment_HK, PIT_SnapshotDate, SAT_Payment_LoadDate, PIT_LoadDate, PIT_BatchID)
        SELECT
            hub.HUB_Payment_HK,
            @SnapshotDate,
            sat_latest.SAT_LoadDate,
            GETDATE(),
            @BatchID
        FROM DV_Bronze.dbo.HUB_Payment hub
        CROSS APPLY (
            SELECT TOP 1 s.SAT_LoadDate
            FROM DV_Bronze.dbo.SAT_Payment s
            WHERE s.HUB_Payment_HK = hub.HUB_Payment_HK
              AND s.SAT_LoadDate <= @SnapshotDate
            ORDER BY s.SAT_LoadDate DESC
        ) sat_latest;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 3 PIT procedures created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  BRIDGE STORED PROCEDURE (1)                              ║
-- ║  Pattern: DELETE snapshot → INSERT via Link + Hub + SAT    ║
-- ║  Pre-joins with denormalized attributes                    ║
-- ╚═══════════════════════════════════════════════════════════╝

CREATE OR ALTER PROCEDURE dbo.usp_Load_BRG_Taxpayer_Owner
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Silver.dbo.BRG_Taxpayer_Owner
        WHERE BRG_SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Silver.dbo.BRG_Taxpayer_Owner
            (HUB_Taxpayer_HK, HUB_Owner_HK, BRG_SnapshotDate,
             BRG_LoadDate, BRG_BatchID, TaxID, OwnerName)
        SELECT
            lnk.HUB_Taxpayer_HK,
            lnk.HUB_Owner_HK,
            @SnapshotDate,
            GETDATE(),
            @BatchID,
            hub_tp.TaxID,
            sat_own.OwnerName
        FROM DV_Bronze.dbo.LNK_TaxpayerOwner lnk
        -- Hub lookups for business keys
        INNER JOIN DV_Bronze.dbo.HUB_Taxpayer hub_tp
            ON lnk.HUB_Taxpayer_HK = hub_tp.HUB_Taxpayer_HK
        INNER JOIN DV_Bronze.dbo.HUB_Owner hub_own
            ON lnk.HUB_Owner_HK = hub_own.HUB_Owner_HK
        -- CROSS APPLY: latest SAT as-of @SnapshotDate with EndDate check
        CROSS APPLY (
            SELECT TOP 1 s.OwnerName
            FROM DV_Bronze.dbo.SAT_Owner s
            WHERE s.HUB_Owner_HK = lnk.HUB_Owner_HK
              AND s.SAT_LoadDate <= @SnapshotDate
              AND (s.SAT_EndDate IS NULL OR s.SAT_EndDate > @SnapshotDate)
            ORDER BY s.SAT_LoadDate DESC
        ) sat_own;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 1 Bridge procedure created ===';
GO

-- ╔═══════════════════════════════════════════════════════════╗
-- ║  BUSINESS VAULT STORED PROCEDURES (2)                     ║
-- ║  Pattern: DELETE snapshot → INSERT via OUTER APPLY         ║
-- ║  Contains computed/derived business metrics                ║
-- ╚═══════════════════════════════════════════════════════════╝

-- ─── BUS_ComplianceScore ───
-- ComplianceScore = 50% filing on-time rate + 50% payment on-time rate
CREATE OR ALTER PROCEDURE dbo.usp_Load_BUS_ComplianceScore
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Silver.dbo.BUS_ComplianceScore
        WHERE SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Silver.dbo.BUS_ComplianceScore
            (HUB_Taxpayer_HK, SnapshotDate, ComplianceScore,
             FilingOnTimeRate, PaymentOnTimeRate, PenaltyCount,
             BUS_LoadDate, BUS_BatchID)
        SELECT
            hub.HUB_Taxpayer_HK,
            @SnapshotDate,
            -- Weighted score: 50% filing + 50% payment
            ISNULL(filing.OnTimeRate, 0) * 0.50 + ISNULL(payment.OnTimeRate, 0) * 0.50,
            filing.OnTimeRate,
            payment.OnTimeRate,
            penalties.PenaltyCount,
            GETDATE(),
            @BatchID
        FROM DV_Bronze.dbo.HUB_Taxpayer hub
        -- Filing on-time rate
        OUTER APPLY (
            SELECT
                CASE WHEN COUNT(*) = 0 THEN NULL
                     ELSE CAST(SUM(CASE WHEN sd.DeclarationDate <= sd.DueDate THEN 1 ELSE 0 END) AS DECIMAL(5,2))
                          / CAST(COUNT(*) AS DECIMAL(5,2)) * 100
                END AS OnTimeRate
            FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
            INNER JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
                ON lnk.HUB_Declaration_HK = sd.HUB_Declaration_HK
                AND sd.SAT_EndDate IS NULL
            WHERE lnk.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
        ) filing
        -- Payment on-time rate
        OUTER APPLY (
            SELECT
                CASE WHEN COUNT(*) = 0 THEN NULL
                     ELSE CAST(SUM(CASE WHEN sp.Status = 'Completed' THEN 1 ELSE 0 END) AS DECIMAL(5,2))
                          / CAST(COUNT(*) AS DECIMAL(5,2)) * 100
                END AS OnTimeRate
            FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk_td
            INNER JOIN DV_Bronze.dbo.LNK_DeclarationPayment lnk_dp
                ON lnk_td.HUB_Declaration_HK = lnk_dp.HUB_Declaration_HK
            INNER JOIN DV_Bronze.dbo.SAT_Payment sp
                ON lnk_dp.HUB_Payment_HK = sp.HUB_Payment_HK
                AND sp.SAT_EndDate IS NULL
            WHERE lnk_td.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
        ) payment
        -- Penalty count
        OUTER APPLY (
            SELECT COUNT(*) AS PenaltyCount
            FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
            INNER JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
                ON lnk.HUB_Declaration_HK = sd.HUB_Declaration_HK
                AND sd.SAT_EndDate IS NULL
            WHERE lnk.HUB_Taxpayer_HK = hub.HUB_Taxpayer_HK
              AND sd.PenaltyAmount > 0
        ) penalties;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

-- ─── BUS_MonthlyMetrics ───
-- Aggregates monthly declaration and payment data per taxpayer
CREATE OR ALTER PROCEDURE dbo.usp_Load_BUS_MonthlyMetrics
    @BatchID       INT,
    @SnapshotDate  DATETIME,
    @RowCount      INT = 0 OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        DELETE FROM DV_Silver.dbo.BUS_MonthlyMetrics
        WHERE SnapshotDate = @SnapshotDate;

        INSERT INTO DV_Silver.dbo.BUS_MonthlyMetrics
            (HUB_Taxpayer_HK, MetricMonth, MetricYear, SnapshotDate,
             TotalDeclarations, TotalGrossRevenue, TotalTaxAmount,
             TotalPayments, TotalPaymentAmount, PaymentCoverageRate,
             BUS_LoadDate, BUS_BatchID)
        SELECT
            lnk.HUB_Taxpayer_HK,
            sd.DeclarationMonth,
            sd.DeclarationYear,
            @SnapshotDate,
            COUNT(DISTINCT sd.HUB_Declaration_HK)          AS TotalDeclarations,
            SUM(sd.GrossRevenue)                            AS TotalGrossRevenue,
            SUM(sd.TaxAmount)                               AS TotalTaxAmount,
            pay_agg.TotalPayments,
            pay_agg.TotalPaymentAmount,
            CASE WHEN SUM(sd.TotalAmount) = 0 THEN 0
                 ELSE ISNULL(pay_agg.TotalPaymentAmount, 0) / SUM(sd.TotalAmount) * 100
            END                                             AS PaymentCoverageRate,
            GETDATE(),
            @BatchID
        FROM DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk
        INNER JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd
            ON lnk.HUB_Declaration_HK = sd.HUB_Declaration_HK
            AND sd.SAT_EndDate IS NULL
        -- Payment aggregation per taxpayer/month
        OUTER APPLY (
            SELECT
                COUNT(DISTINCT sp.HUB_Payment_HK) AS TotalPayments,
                SUM(sp.PaymentAmount)              AS TotalPaymentAmount
            FROM DV_Bronze.dbo.LNK_DeclarationPayment lnk_dp
            INNER JOIN DV_Bronze.dbo.SAT_Payment sp
                ON lnk_dp.HUB_Payment_HK = sp.HUB_Payment_HK
                AND sp.SAT_EndDate IS NULL
            INNER JOIN DV_Bronze.dbo.SAT_MonthlyDecl sd2
                ON lnk_dp.HUB_Declaration_HK = sd2.HUB_Declaration_HK
                AND sd2.SAT_EndDate IS NULL
                AND sd2.DeclarationMonth = sd.DeclarationMonth
                AND sd2.DeclarationYear = sd.DeclarationYear
            INNER JOIN DV_Bronze.dbo.LNK_TaxpayerDeclaration lnk_td2
                ON lnk_dp.HUB_Declaration_HK = lnk_td2.HUB_Declaration_HK
                AND lnk_td2.HUB_Taxpayer_HK = lnk.HUB_Taxpayer_HK
        ) pay_agg
        GROUP BY lnk.HUB_Taxpayer_HK, sd.DeclarationMonth, sd.DeclarationYear,
                 pay_agg.TotalPayments, pay_agg.TotalPaymentAmount;

        SET @RowCount = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@Err, 16, 1);
    END CATCH
END;
GO

PRINT '=== 2 Business Vault procedures created ===';
PRINT '=== DV_Silver: Total 6 procedures (3 PIT + 1 Bridge + 2 Business Vault) ===';
GO
