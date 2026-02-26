-- =============================================
-- 09_ETL_Master_Orchestration.sql
-- SQL-Based Master ETL Orchestration
-- Aligned with DV2_Complete_Architecture_Guide.docx
--
-- IMPORTANT: Execute scripts in order:
--   01 → 02 → 03 → 04 → 05 → 06 → 07 → 08 → 09
--
-- NOTE: In production, orchestration is handled by SSIS packages:
--   Master_Complete_Pipeline.dtsx → Master_Full_Load / Master_Incremental_Load
-- This SQL script provides an equivalent SQL-only orchestration
-- for testing, debugging, and environments without SSIS.
--
-- SP Inventory Called:
--   ETL_Control : usp_StartBatch, usp_EndBatch, usp_StartStep, usp_EndStep, usp_LogError
--   DV_Staging  : usp_Truncate_STG_Table (helper only — DFTs handle data movement)
--   DV_Bronze   : 9 usp_Load_HUB_* + 9 usp_Load_SAT_* + 5 usp_Load_LNK_* = 23 SPs
--   DV_Silver   : 3 usp_Load_PIT_* + 1 usp_Load_BRG_* + 2 usp_Load_BUS_* = 6 SPs
--   DV_Gold     : 7 usp_Load_DIM_* + 4 usp_Load_FACT_* = 11 SPs
-- =============================================

USE ETL_Control;
GO

-- =============================================
-- usp_MasterETL_FullLoad
-- Orchestrates all 4 layers: Staging → Bronze → Silver → Gold
-- Mirrors: Master_Complete_Pipeline.dtsx → Master_Full_Load.dtsx
-- =============================================
CREATE OR ALTER PROCEDURE dbo.usp_MasterETL_FullLoad
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchID       INT;
    DECLARE @StepID        INT;
    DECLARE @RowCount      INT;
    DECLARE @ErrorCount    INT = 0;
    DECLARE @ErrorMessage  NVARCHAR(4000);
    DECLARE @StartTime     DATETIME = GETDATE();
    DECLARE @SnapshotDate  DATETIME = CAST(GETDATE() AS DATETIME);

    BEGIN TRY
        -- ═══════════════════════════════════════
        -- Start Batch
        -- ═══════════════════════════════════════
        EXEC dbo.usp_StartBatch
            @ProcessName = 'Master_ETL',
            @BatchID     = @BatchID OUTPUT;

        PRINT '════════════════════════════════════════════════';
        PRINT 'MASTER ETL FULL LOAD — ALL 4 LAYERS';
        PRINT 'Batch ID     : ' + CAST(@BatchID AS VARCHAR(10));
        PRINT 'SnapshotDate : ' + CONVERT(VARCHAR(20), @SnapshotDate, 120);
        PRINT 'Start Time   : ' + CONVERT(VARCHAR(20), @StartTime, 120);
        PRINT '════════════════════════════════════════════════';
        PRINT '';

        -- ═══════════════════════════════════════════════════
        -- LAYER 1: STAGING (9 Tables via DFT — SQL helper only)
        -- In SSIS: STG_Load_All.dtsx calls 9 child packages,
        --          each with SQL_TruncateStaging + DFT_Load_*
        -- Here: We truncate tables. Actual data loading requires
        --        SSIS DFTs or manual INSERT from source.
        -- ═══════════════════════════════════════════════════
        PRINT '--- LAYER 1: STAGING (Truncate for Full Load) ---';

        DECLARE @STG_Tables TABLE (TableName VARCHAR(100));
        INSERT INTO @STG_Tables VALUES
            ('STG_Category'), ('STG_Structure'), ('STG_Activity'),
            ('STG_Taxpayer'), ('STG_Owner'), ('STG_Officer'),
            ('STG_MonthlyDeclaration'), ('STG_AnnualDeclaration'), ('STG_Payment');

        DECLARE @tbl VARCHAR(100);
        DECLARE cur_stg CURSOR LOCAL FAST_FORWARD FOR
            SELECT TableName FROM @STG_Tables;
        OPEN cur_stg;
        FETCH NEXT FROM cur_stg INTO @tbl;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                EXEC DV_Staging.dbo.usp_Truncate_STG_Table @TableName = @tbl;
                PRINT '  Truncated: ' + @tbl;
            END TRY
            BEGIN CATCH
                PRINT '  FAILED truncate ' + @tbl + ': ' + ERROR_MESSAGE();
                SET @ErrorCount += 1;
            END CATCH;
            FETCH NEXT FROM cur_stg INTO @tbl;
        END;
        CLOSE cur_stg; DEALLOCATE cur_stg;

        PRINT '';
        PRINT '  NOTE: Staging data movement requires SSIS Data Flow Tasks.';
        PRINT '        In SQL-only mode, INSERT data into STG tables manually';
        PRINT '        before proceeding to Bronze layer.';
        PRINT '';

        -- ═══════════════════════════════════════════════════
        -- LAYER 2: BRONZE — DATA VAULT
        -- In SSIS: BRZ_Load_All.dtsx calls 23 child packages
        -- 9 Hubs + 9 Satellites + 5 Links
        -- ═══════════════════════════════════════════════════
        PRINT '--- LAYER 2: BRONZE — DATA VAULT ---';

        -- ── Hubs (9) ──
        PRINT '  Loading Hubs (9)...';

        -- Lookup Hubs (3)
        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Category @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Category: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Category: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Structure @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Structure: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Structure: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Activity @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Activity: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Activity: ' + ERROR_MESSAGE(); END CATCH;

        -- Core Hubs (6)
        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Taxpayer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Owner @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Owner: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Owner: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Officer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Officer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Officer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Declaration @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Declaration: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Declaration: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_Payment @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_Payment: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Payment: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_HUB_AnnualDecl @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    HUB_AnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_AnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        -- ── Satellites (9) ──
        PRINT '  Loading Satellites (9)...';

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Category @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Category: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Category: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Structure @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Structure: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Structure: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Activity @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Activity: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Activity: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Taxpayer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Owner @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Owner: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Owner: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Officer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Officer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Officer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_MonthlyDecl @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_MonthlyDecl: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_MonthlyDecl: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_Payment @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_Payment: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Payment: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_SAT_AnnualDecl @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    SAT_AnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_AnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        -- ── Links (5) ──
        PRINT '  Loading Links (5)...';

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerDeclaration @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    LNK_TaxpayerDeclaration: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerDeclaration: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_LNK_DeclarationPayment @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    LNK_DeclarationPayment: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_DeclarationPayment: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerOfficer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    LNK_TaxpayerOfficer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerOfficer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerOwner @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    LNK_TaxpayerOwner: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerOwner: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerAnnualDecl @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    LNK_TaxpayerAnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerAnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '  Bronze Complete: 9 Hubs, 9 Satellites, 5 Links';
        PRINT '';

        -- ═══════════════════════════════════════════════════
        -- LAYER 3: SILVER — PIT + Bridge + Business Vault
        -- In SSIS: SLV_Load_All.dtsx calls 6 child packages
        -- 3 PITs + 1 Bridge + 2 Business Vault
        -- ═══════════════════════════════════════════════════
        PRINT '--- LAYER 3: SILVER ---';

        -- PIT Tables (3)
        PRINT '  Loading PIT Tables (3)...';

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_PIT_Taxpayer
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    PIT_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_PIT_Declaration
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    PIT_Declaration: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Declaration: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_PIT_Payment
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    PIT_Payment: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Payment: ' + ERROR_MESSAGE(); END CATCH;

        -- Bridge Table (1)
        PRINT '  Loading Bridge Tables (1)...';

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_BRG_Taxpayer_Owner
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    BRG_Taxpayer_Owner: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BRG_Taxpayer_Owner: ' + ERROR_MESSAGE(); END CATCH;

        -- Business Vault (2)
        PRINT '  Loading Business Vault (2)...';

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_BUS_ComplianceScore
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    BUS_ComplianceScore: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BUS_ComplianceScore: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Silver.dbo.usp_Load_BUS_MonthlyMetrics
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    BUS_MonthlyMetrics: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BUS_MonthlyMetrics: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '  Silver Complete: 3 PITs, 1 Bridge, 2 Business Vault';
        PRINT '';

        -- ═══════════════════════════════════════════════════
        -- LAYER 4: GOLD — DIMENSIONAL MODEL
        -- In SSIS: GLD_Load_All_Dimensions.dtsx (7 dims)
        --          GLD_Load_All_Facts.dtsx     (4 facts)
        -- Dimensions MUST complete before Facts (FK dependency)
        -- ═══════════════════════════════════════════════════
        PRINT '--- LAYER 4: GOLD — DIMENSIONAL MODEL ---';

        -- SCD Type 1 Dimensions (5) — load first (simpler, faster)
        PRINT '  Loading SCD1 Dimensions (5)...';

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Category @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Category (SCD1): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Category: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Structure @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Structure (SCD1): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Structure: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Activity @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Activity (SCD1): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Activity: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_PaymentMethod @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_PaymentMethod (SCD1): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_PaymentMethod: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Status @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Status (SCD1): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Status: ' + ERROR_MESSAGE(); END CATCH;

        -- SCD Type 2 Dimensions (2) — load after SCD1
        PRINT '  Loading SCD2 Dimensions (2)...';

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Taxpayer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Taxpayer (SCD2): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_DIM_Officer @BatchID = @BatchID, @RowCount = @RowCount OUTPUT;
            PRINT '    DIM_Officer (SCD2): ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Officer: ' + ERROR_MESSAGE(); END CATCH;

        -- Fact Tables (4) — load after ALL dimensions
        PRINT '  Loading Fact Tables (4)...';

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_FACT_MonthlyDeclaration
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    FACT_MonthlyDeclaration: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_MonthlyDeclaration: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_FACT_Payment
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    FACT_Payment: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_Payment: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_FACT_MonthlySnapshot
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    FACT_MonthlySnapshot: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_MonthlySnapshot: ' + ERROR_MESSAGE(); END CATCH;

        BEGIN TRY
            EXEC DV_Gold.dbo.usp_Load_FACT_DeclarationLifecycle
                @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT;
            PRINT '    FACT_DeclarationLifecycle: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows';
        END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_DeclarationLifecycle: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '  Gold Complete: 7 Dimensions (5 SCD1 + 2 SCD2), 4 Facts';
        PRINT '';

        -- ═══════════════════════════════════════
        -- End Batch
        -- ═══════════════════════════════════════
        DECLARE @FinalStatus VARCHAR(20) = CASE WHEN @ErrorCount = 0 THEN 'Success' ELSE 'CompletedWithErrors' END;

        EXEC dbo.usp_EndBatch
            @BatchID          = @BatchID,
            @Status           = @FinalStatus,
            @RecordsProcessed = NULL;  -- Auto-calculated from step logs

        DECLARE @EndTime  DATETIME = GETDATE();
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, @EndTime);

        PRINT '════════════════════════════════════════════════';
        PRINT 'MASTER ETL FULL LOAD — COMPLETED';
        PRINT '';
        PRINT '  Status   : ' + @FinalStatus;
        PRINT '  Batch ID : ' + CAST(@BatchID AS VARCHAR(10));
        PRINT '  Errors   : ' + CAST(@ErrorCount AS VARCHAR(10));
        PRINT '  Duration : ' + CAST(@Duration AS VARCHAR(10)) + ' seconds';
        PRINT '';
        PRINT '  Layer 1 (Staging) : 9 tables truncated (DFT data load required)';
        PRINT '  Layer 2 (Bronze)  : 9 Hubs + 9 Satellites + 5 Links = 23 SPs';
        PRINT '  Layer 3 (Silver)  : 3 PITs + 1 Bridge + 2 Business Vault = 6 SPs';
        PRINT '  Layer 4 (Gold)    : 7 Dimensions + 4 Facts = 11 SPs';
        PRINT '  Total SPs Called  : 40 (excl. ETL_Control + Staging helpers)';
        PRINT '════════════════════════════════════════════════';

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        PRINT '';
        PRINT '════════════════════════════════════════════════';
        PRINT 'FATAL ERROR IN PIPELINE: ' + @ErrorMessage;
        PRINT '════════════════════════════════════════════════';

        IF @BatchID IS NOT NULL
            EXEC dbo.usp_EndBatch
                @BatchID          = @BatchID,
                @Status           = 'Failed',
                @RecordsProcessed = NULL;

        THROW;
    END CATCH;
END;
GO


-- =============================================
-- usp_MasterETL_IncrementalLoad
-- Mirrors: Master_Complete_Pipeline.dtsx → Master_Incremental_Load.dtsx
-- Same as Full Load except:
--   - No TRUNCATE on staging tables (incremental DFTs use watermark)
--   - Bronze SPs only process new/changed records (INSERT WHERE NOT EXISTS)
--   - Silver/Gold use same logic (DELETE + INSERT by SnapshotDate)
-- =============================================
CREATE OR ALTER PROCEDURE dbo.usp_MasterETL_IncrementalLoad
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchID       INT;
    DECLARE @RowCount      INT;
    DECLARE @ErrorCount    INT = 0;
    DECLARE @ErrorMessage  NVARCHAR(4000);
    DECLARE @StartTime     DATETIME = GETDATE();
    DECLARE @SnapshotDate  DATETIME = CAST(GETDATE() AS DATETIME);

    BEGIN TRY
        EXEC dbo.usp_StartBatch
            @ProcessName = 'Master_ETL',
            @BatchID     = @BatchID OUTPUT;

        PRINT '════════════════════════════════════════════════';
        PRINT 'MASTER ETL INCREMENTAL LOAD';
        PRINT 'Batch ID     : ' + CAST(@BatchID AS VARCHAR(10));
        PRINT 'SnapshotDate : ' + CONVERT(VARCHAR(20), @SnapshotDate, 120);
        PRINT '════════════════════════════════════════════════';
        PRINT '';

        -- LAYER 1: STAGING (Incremental — no truncation)
        -- SSIS DFTs use watermark-based OLE DB Source queries
        PRINT '--- LAYER 1: STAGING (Incremental — skip truncation) ---';
        PRINT '  Staging data loaded via SSIS DFTs with watermark-based queries.';
        PRINT '';

        -- LAYER 2: BRONZE (same SPs — INSERT WHERE NOT EXISTS handles idempotency)
        PRINT '--- LAYER 2: BRONZE ---';

        -- Hubs (9)
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Category     @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Category: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Category: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Structure     @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Structure: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Structure: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Activity      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Activity: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Activity: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Taxpayer      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Owner         @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Owner: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Owner: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Officer       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Officer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Officer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Declaration   @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Declaration: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Declaration: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_Payment       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_Payment: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_Payment: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_HUB_AnnualDecl    @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    HUB_AnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED HUB_AnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        -- Satellites (9)
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Category      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Category: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Category: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Structure      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Structure: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Structure: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Activity       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Activity: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Activity: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Taxpayer       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Owner          @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Owner: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Owner: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Officer        @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Officer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Officer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_MonthlyDecl    @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_MonthlyDecl: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_MonthlyDecl: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_Payment        @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_Payment: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_Payment: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_SAT_AnnualDecl     @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    SAT_AnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED SAT_AnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        -- Links (5)
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerDeclaration @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    LNK_TaxpayerDeclaration: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerDeclaration: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_LNK_DeclarationPayment  @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    LNK_DeclarationPayment: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_DeclarationPayment: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerOfficer     @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    LNK_TaxpayerOfficer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerOfficer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerOwner       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    LNK_TaxpayerOwner: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerOwner: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Bronze.dbo.usp_Load_LNK_TaxpayerAnnualDecl  @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    LNK_TaxpayerAnnualDecl: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED LNK_TaxpayerAnnualDecl: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '';

        -- LAYER 3: SILVER
        PRINT '--- LAYER 3: SILVER ---';

        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_PIT_Taxpayer         @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    PIT_Taxpayer: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_PIT_Declaration      @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    PIT_Declaration: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Declaration: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_PIT_Payment          @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    PIT_Payment: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED PIT_Payment: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_BRG_Taxpayer_Owner   @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    BRG_Taxpayer_Owner: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BRG_Taxpayer_Owner: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_BUS_ComplianceScore  @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    BUS_ComplianceScore: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BUS_ComplianceScore: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Silver.dbo.usp_Load_BUS_MonthlyMetrics   @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    BUS_MonthlyMetrics: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED BUS_MonthlyMetrics: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '';

        -- LAYER 4: GOLD
        PRINT '--- LAYER 4: GOLD ---';

        -- Dimensions (7) — SCD1 first, then SCD2
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Category      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Category: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Category: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Structure      @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Structure: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Structure: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Activity       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Activity: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Activity: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_PaymentMethod  @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_PaymentMethod: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_PaymentMethod: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Status         @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Status: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Status: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Taxpayer       @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Taxpayer (SCD2): ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Taxpayer: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_DIM_Officer        @BatchID = @BatchID, @RowCount = @RowCount OUTPUT; PRINT '    DIM_Officer (SCD2): ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED DIM_Officer: ' + ERROR_MESSAGE(); END CATCH;

        -- Facts (4)
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_FACT_MonthlyDeclaration    @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    FACT_MonthlyDeclaration: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_MonthlyDeclaration: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_FACT_Payment               @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    FACT_Payment: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_Payment: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_FACT_MonthlySnapshot       @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    FACT_MonthlySnapshot: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_MonthlySnapshot: ' + ERROR_MESSAGE(); END CATCH;
        BEGIN TRY EXEC DV_Gold.dbo.usp_Load_FACT_DeclarationLifecycle  @BatchID = @BatchID, @SnapshotDate = @SnapshotDate, @RowCount = @RowCount OUTPUT; PRINT '    FACT_DeclarationLifecycle: ' + CAST(@RowCount AS VARCHAR(10)); END TRY BEGIN CATCH SET @ErrorCount += 1; PRINT '    FAILED FACT_DeclarationLifecycle: ' + ERROR_MESSAGE(); END CATCH;

        PRINT '';

        -- End Batch
        DECLARE @FinalStatus VARCHAR(20) = CASE WHEN @ErrorCount = 0 THEN 'Success' ELSE 'CompletedWithErrors' END;
        EXEC dbo.usp_EndBatch @BatchID = @BatchID, @Status = @FinalStatus, @RecordsProcessed = NULL;

        DECLARE @EndTime  DATETIME = GETDATE();
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, @EndTime);

        PRINT '════════════════════════════════════════════════';
        PRINT 'INCREMENTAL LOAD COMPLETED: ' + @FinalStatus;
        PRINT '  Errors: ' + CAST(@ErrorCount AS VARCHAR(10)) + '  Duration: ' + CAST(@Duration AS VARCHAR(10)) + 's';
        PRINT '════════════════════════════════════════════════';

    END TRY
    BEGIN CATCH
        SET @ErrorMessage = ERROR_MESSAGE();
        PRINT 'FATAL ERROR: ' + @ErrorMessage;
        IF @BatchID IS NOT NULL
            EXEC dbo.usp_EndBatch @BatchID = @BatchID, @Status = 'Failed', @RecordsProcessed = NULL;
        THROW;
    END CATCH;
END;
GO


-- =============================================
-- Quick Execution Commands
-- =============================================
PRINT '';
PRINT '════════════════════════════════════════════════';
PRINT '09_ETL_Master_Orchestration.sql — Created';
PRINT '';
PRINT 'Procedures:';
PRINT '  usp_MasterETL_FullLoad        — Full pipeline (Truncate + Load all 4 layers)';
PRINT '  usp_MasterETL_IncrementalLoad — Incremental pipeline (Watermark-based, no truncate)';
PRINT '';
PRINT 'Usage:';
PRINT '  EXEC ETL_Control.dbo.usp_MasterETL_FullLoad;';
PRINT '  EXEC ETL_Control.dbo.usp_MasterETL_IncrementalLoad;';
PRINT '';
PRINT 'SP Call Summary:';
PRINT '  ETL_Control : usp_StartBatch, usp_EndBatch';
PRINT '  DV_Staging  : usp_Truncate_STG_Table (Full Load only)';
PRINT '  DV_Bronze   : 9 HUB + 9 SAT + 5 LNK = 23 SPs';
PRINT '  DV_Silver   : 3 PIT + 1 BRG + 2 BUS  = 6 SPs';
PRINT '  DV_Gold     : 7 DIM + 4 FACT          = 11 SPs';
PRINT '  Total       : 42 SP calls per full run';
PRINT '════════════════════════════════════════════════';
GO