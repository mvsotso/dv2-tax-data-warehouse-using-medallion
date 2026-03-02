-- =============================================
-- 04_DDL_Architecture_DW.sql
-- Data Vault 2.0 - Data Warehouse Architecture DDL
-- Aligned with DV2_Complete_Architecture_Guide.docx
-- Databases: DV_Staging, DV_Bronze, DV_Silver, DV_Gold
-- =============================================

USE master;
GO

-- =============================================
-- 1. STAGING DATABASE (DV_Staging)
-- =============================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DV_Staging')
    CREATE DATABASE DV_Staging;
GO

USE DV_Staging;
GO

-- Drop existing tables if recreating
IF OBJECT_ID('dbo.STG_Category', 'U') IS NOT NULL DROP TABLE dbo.STG_Category;
IF OBJECT_ID('dbo.STG_Structure', 'U') IS NOT NULL DROP TABLE dbo.STG_Structure;
IF OBJECT_ID('dbo.STG_Activity', 'U') IS NOT NULL DROP TABLE dbo.STG_Activity;
IF OBJECT_ID('dbo.STG_Taxpayer', 'U') IS NOT NULL DROP TABLE dbo.STG_Taxpayer;
IF OBJECT_ID('dbo.STG_Owner', 'U') IS NOT NULL DROP TABLE dbo.STG_Owner;
IF OBJECT_ID('dbo.STG_Officer', 'U') IS NOT NULL DROP TABLE dbo.STG_Officer;
IF OBJECT_ID('dbo.STG_MonthlyDeclaration', 'U') IS NOT NULL DROP TABLE dbo.STG_MonthlyDeclaration;
IF OBJECT_ID('dbo.STG_AnnualDeclaration', 'U') IS NOT NULL DROP TABLE dbo.STG_AnnualDeclaration;
IF OBJECT_ID('dbo.STG_Payment', 'U') IS NOT NULL DROP TABLE dbo.STG_Payment;
GO

-- ─── Group A: No SourceSystem in source (DRV adds SourceSystem + RecordSource + LoadDateTime) ───

CREATE TABLE STG_Category (
    CategoryID          INT,
    CategoryName        VARCHAR(100),
    CategoryDescription VARCHAR(500),
    IsActive            BIT,
    CreatedDate         DATETIME,
    UpdatedDate         DATETIME,
    -- ETL Metadata (added by DRV_AddMetadata in DFT)
    SourceSystem        NVARCHAR(50)     NULL,
    RecordSource        NVARCHAR(50)     NULL,
    LoadDateTime        DATETIME        NULL
);

CREATE TABLE STG_Structure (
    StructureID          INT,
    StructureName        VARCHAR(100),
    StructureDescription VARCHAR(500),
    IsActive             BIT,
    CreatedDate          DATETIME,
    UpdatedDate          DATETIME,
    SourceSystem         NVARCHAR(50)     NULL,
    RecordSource         NVARCHAR(50)     NULL,
    LoadDateTime         DATETIME        NULL
);

CREATE TABLE STG_Activity (
    ActivityID          INT,
    ActivityName        VARCHAR(100),
    ActivityDescription VARCHAR(500),
    IsActive            BIT,
    CreatedDate         DATETIME,
    UpdatedDate         DATETIME,
    SourceSystem        NVARCHAR(50)     NULL,
    RecordSource        NVARCHAR(50)     NULL,
    LoadDateTime        DATETIME        NULL
);

-- ─── Group B: SourceSystem exists in source (DRV adds RecordSource + LoadDateTime only) ───

CREATE TABLE STG_Taxpayer (
    TaxpayerID              INT,
    TaxID                   VARCHAR(20),
    LegalBusinessName       VARCHAR(300),
    TradingName             VARCHAR(300),
    CategoryID              INT,
    StructureID             INT,
    RegistrationDate        DATE,
    EstimatedAnnualRevenue  DECIMAL(18,2),
    IsActive                BIT,
    CreatedDate             DATETIME,
    UpdatedDate             DATETIME,
    SourceSystem            NVARCHAR(50)     NULL,
    RecordSource            NVARCHAR(50)     NULL,
    LoadDateTime            DATETIME        NULL
);

CREATE TABLE STG_Owner (
    OwnerID                 INT,
    TaxpayerID              INT,
    OwnerName               VARCHAR(200),
    OwnerType               VARCHAR(50),
    OwnershipPercentage     DECIMAL(5,2),
    IsActive                BIT,
    CreatedDate             DATETIME,
    UpdatedDate             DATETIME,
    SourceSystem            NVARCHAR(50)     NULL,
    RecordSource            NVARCHAR(50)     NULL,
    LoadDateTime            DATETIME        NULL
);

CREATE TABLE STG_Officer (
    OfficerID       INT,
    OfficerCode     VARCHAR(20),
    FirstName       VARCHAR(100),
    LastName        VARCHAR(100),
    Department      VARCHAR(100),
    IsActive        BIT,
    CreatedDate     DATETIME,
    UpdatedDate     DATETIME,
    SourceSystem    NVARCHAR(50)     NULL,
    RecordSource    NVARCHAR(50)     NULL,
    LoadDateTime    DATETIME        NULL
);

CREATE TABLE STG_MonthlyDeclaration (
    DeclarationID   INT,
    TaxpayerID      INT,
    DeclarationMonth INT,
    DeclarationYear INT,
    GrossRevenue    DECIMAL(18,2),
    TaxableRevenue  DECIMAL(18,2),
    TaxAmount       DECIMAL(18,2),
    PenaltyAmount   DECIMAL(18,2),
    InterestAmount  DECIMAL(18,2),
    TotalAmount     DECIMAL(18,2),
    DeclarationDate DATE,
    DueDate         DATE,
    Status          VARCHAR(50),
    OfficerID       INT,
    IsActive        BIT,
    CreatedDate     DATETIME,
    UpdatedDate     DATETIME,
    SourceSystem    NVARCHAR(50)     NULL,
    RecordSource    NVARCHAR(50)     NULL,
    LoadDateTime    DATETIME        NULL
);

CREATE TABLE STG_AnnualDeclaration (
    AnnualDeclarationID INT,
    TaxpayerID          INT,
    DeclarationYear     INT,
    GrossRevenue        DECIMAL(18,2),
    TaxableRevenue      DECIMAL(18,2),
    TaxAmount           DECIMAL(18,2),
    PenaltyAmount       DECIMAL(18,2),
    InterestAmount      DECIMAL(18,2),
    TotalAmount         DECIMAL(18,2),
    DeclarationDate     DATE,
    DueDate             DATE,
    Status              VARCHAR(50),
    OfficerID           INT,
    IsActive            BIT,
    CreatedDate         DATETIME,
    UpdatedDate         DATETIME,
    SourceSystem        NVARCHAR(50)     NULL,
    RecordSource        NVARCHAR(50)     NULL,
    LoadDateTime        DATETIME        NULL
);

CREATE TABLE STG_Payment (
    PaymentID           INT,
    TaxpayerID          INT,
    DeclarationID       INT,
    AnnualDeclarationID INT,
    PaymentAmount       DECIMAL(18,2),
    PaymentDate         DATE,
    PaymentMethod       VARCHAR(50),
    ReferenceNumber     VARCHAR(100),
    Status              VARCHAR(50),
    IsActive            BIT,
    CreatedDate         DATETIME,
    UpdatedDate         DATETIME,
    SourceSystem        NVARCHAR(50)     NULL,
    RecordSource        NVARCHAR(50)     NULL,
    LoadDateTime        DATETIME        NULL
);
GO

PRINT '=== DV_Staging: 9 staging tables created ===';
GO

-- =============================================
-- 2. BRONZE DATABASE (DV_Bronze) — Raw Data Vault
-- Hash keys: VARBINARY(32) via HASHBYTES('SHA2_256', ...)
-- No hash functions — computed on-the-fly in SPs
-- =============================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DV_Bronze')
    CREATE DATABASE DV_Bronze;
GO

USE DV_Bronze;
GO

-- ─── HUB Tables (9) ───

CREATE TABLE HUB_Category (
    HUB_Category_HK     VARBINARY(32)   NOT NULL,
    CategoryID           INT             NOT NULL,
    HUB_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource     NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Category PRIMARY KEY (HUB_Category_HK)
);

CREATE TABLE HUB_Structure (
    HUB_Structure_HK     VARBINARY(32)   NOT NULL,
    StructureID           INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Structure PRIMARY KEY (HUB_Structure_HK)
);

CREATE TABLE HUB_Activity (
    HUB_Activity_HK      VARBINARY(32)   NOT NULL,
    ActivityID            INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Activity PRIMARY KEY (HUB_Activity_HK)
);

CREATE TABLE HUB_Taxpayer (
    HUB_Taxpayer_HK      VARBINARY(32)   NOT NULL,
    TaxID                 VARCHAR(20)     NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Taxpayer PRIMARY KEY (HUB_Taxpayer_HK)
);

CREATE TABLE HUB_Owner (
    HUB_Owner_HK         VARBINARY(32)   NOT NULL,
    OwnerID               INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Owner PRIMARY KEY (HUB_Owner_HK)
);

CREATE TABLE HUB_Officer (
    HUB_Officer_HK       VARBINARY(32)   NOT NULL,
    OfficerCode           VARCHAR(20)     NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Officer PRIMARY KEY (HUB_Officer_HK)
);

CREATE TABLE HUB_Declaration (
    HUB_Declaration_HK   VARBINARY(32)   NOT NULL,
    DeclarationID         INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Declaration PRIMARY KEY (HUB_Declaration_HK)
);

CREATE TABLE HUB_Payment (
    HUB_Payment_HK       VARBINARY(32)   NOT NULL,
    PaymentID             INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_Payment PRIMARY KEY (HUB_Payment_HK)
);

CREATE TABLE HUB_AnnualDecl (
    HUB_AnnualDecl_HK    VARBINARY(32)   NOT NULL,
    AnnualDeclarationID   INT             NOT NULL,
    HUB_LoadDate          DATETIME        NOT NULL DEFAULT GETDATE(),
    HUB_RecordSource      NVARCHAR(50)     NOT NULL,
    CONSTRAINT PK_HUB_AnnualDecl PRIMARY KEY (HUB_AnnualDecl_HK)
);
GO

PRINT '=== DV_Bronze: 9 Hub tables created ===';
GO

-- ─── SAT Tables (9) ───

CREATE TABLE SAT_Category (
    HUB_Category_HK     VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    CategoryName         VARCHAR(100)    NULL,
    CategoryDescription  VARCHAR(500)    NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_Category PRIMARY KEY (HUB_Category_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Category_HUB FOREIGN KEY (HUB_Category_HK) REFERENCES HUB_Category(HUB_Category_HK)
);

CREATE TABLE SAT_Structure (
    HUB_Structure_HK    VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    StructureName        VARCHAR(100)    NULL,
    StructureDescription VARCHAR(500)    NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_Structure PRIMARY KEY (HUB_Structure_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Structure_HUB FOREIGN KEY (HUB_Structure_HK) REFERENCES HUB_Structure(HUB_Structure_HK)
);

CREATE TABLE SAT_Activity (
    HUB_Activity_HK     VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    ActivityName         VARCHAR(100)    NULL,
    ActivityDescription  VARCHAR(500)    NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_Activity PRIMARY KEY (HUB_Activity_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Activity_HUB FOREIGN KEY (HUB_Activity_HK) REFERENCES HUB_Activity(HUB_Activity_HK)
);

CREATE TABLE SAT_Taxpayer (
    HUB_Taxpayer_HK          VARBINARY(32)   NOT NULL,
    SAT_LoadDate              DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate               DATETIME        NULL,
    SAT_HashDiff              VARBINARY(32)   NOT NULL,
    SAT_RecordSource          NVARCHAR(50)     NOT NULL,
    LegalBusinessName         VARCHAR(300)    NULL,
    TradingName               VARCHAR(300)    NULL,
    CategoryID                INT             NULL,
    StructureID               INT             NULL,
    RegistrationDate          DATE            NULL,
    EstimatedAnnualRevenue    DECIMAL(18,2)   NULL,
    IsActive                  BIT             NULL,
    CONSTRAINT PK_SAT_Taxpayer PRIMARY KEY (HUB_Taxpayer_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Taxpayer_HUB FOREIGN KEY (HUB_Taxpayer_HK) REFERENCES HUB_Taxpayer(HUB_Taxpayer_HK)
);

CREATE TABLE SAT_Owner (
    HUB_Owner_HK            VARBINARY(32)   NOT NULL,
    SAT_LoadDate             DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate              DATETIME        NULL,
    SAT_HashDiff             VARBINARY(32)   NOT NULL,
    SAT_RecordSource         NVARCHAR(50)     NOT NULL,
    TaxpayerID               INT             NULL,
    OwnerName                VARCHAR(200)    NULL,
    OwnerType                VARCHAR(50)     NULL,
    OwnershipPercentage      DECIMAL(5,2)    NULL,
    IsActive                 BIT             NULL,
    CONSTRAINT PK_SAT_Owner PRIMARY KEY (HUB_Owner_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Owner_HUB FOREIGN KEY (HUB_Owner_HK) REFERENCES HUB_Owner(HUB_Owner_HK)
);

CREATE TABLE SAT_Officer (
    HUB_Officer_HK      VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    OfficerID            INT             NULL,
    FirstName            VARCHAR(100)    NULL,
    LastName             VARCHAR(100)    NULL,
    Department           VARCHAR(100)    NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_Officer PRIMARY KEY (HUB_Officer_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Officer_HUB FOREIGN KEY (HUB_Officer_HK) REFERENCES HUB_Officer(HUB_Officer_HK)
);

CREATE TABLE SAT_MonthlyDecl (
    HUB_Declaration_HK  VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    TaxpayerID           INT             NULL,
    DeclarationMonth     INT             NULL,
    DeclarationYear      INT             NULL,
    GrossRevenue         DECIMAL(18,2)   NULL,
    TaxableRevenue       DECIMAL(18,2)   NULL,
    TaxAmount            DECIMAL(18,2)   NULL,
    PenaltyAmount        DECIMAL(18,2)   NULL,
    InterestAmount       DECIMAL(18,2)   NULL,
    TotalAmount          DECIMAL(18,2)   NULL,
    DeclarationDate      DATE            NULL,
    DueDate              DATE            NULL,
    Status               VARCHAR(50)     NULL,
    OfficerID            INT             NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_MonthlyDecl PRIMARY KEY (HUB_Declaration_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_MonthlyDecl_HUB FOREIGN KEY (HUB_Declaration_HK) REFERENCES HUB_Declaration(HUB_Declaration_HK)
);

CREATE TABLE SAT_Payment (
    HUB_Payment_HK      VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    TaxpayerID           INT             NULL,
    DeclarationID        INT             NULL,
    AnnualDeclarationID  INT             NULL,
    PaymentAmount        DECIMAL(18,2)   NULL,
    PaymentDate          DATE            NULL,
    PaymentMethod        VARCHAR(50)     NULL,
    ReferenceNumber      VARCHAR(100)    NULL,
    Status               VARCHAR(50)     NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_Payment PRIMARY KEY (HUB_Payment_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_Payment_HUB FOREIGN KEY (HUB_Payment_HK) REFERENCES HUB_Payment(HUB_Payment_HK)
);

CREATE TABLE SAT_AnnualDecl (
    HUB_AnnualDecl_HK   VARBINARY(32)   NOT NULL,
    SAT_LoadDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    SAT_EndDate          DATETIME        NULL,
    SAT_HashDiff         VARBINARY(32)   NOT NULL,
    SAT_RecordSource     NVARCHAR(50)     NOT NULL,
    TaxpayerID           INT             NULL,
    DeclarationYear      INT             NULL,
    GrossRevenue         DECIMAL(18,2)   NULL,
    TaxableRevenue       DECIMAL(18,2)   NULL,
    TaxAmount            DECIMAL(18,2)   NULL,
    PenaltyAmount        DECIMAL(18,2)   NULL,
    InterestAmount       DECIMAL(18,2)   NULL,
    TotalAmount          DECIMAL(18,2)   NULL,
    DeclarationDate      DATE            NULL,
    DueDate              DATE            NULL,
    Status               VARCHAR(50)     NULL,
    OfficerID            INT             NULL,
    IsActive             BIT             NULL,
    CONSTRAINT PK_SAT_AnnualDecl PRIMARY KEY (HUB_AnnualDecl_HK, SAT_LoadDate),
    CONSTRAINT FK_SAT_AnnualDecl_HUB FOREIGN KEY (HUB_AnnualDecl_HK) REFERENCES HUB_AnnualDecl(HUB_AnnualDecl_HK)
);
GO

PRINT '=== DV_Bronze: 9 Satellite tables created ===';
GO

-- ─── LNK (Link) Tables (5) ───

CREATE TABLE LNK_TaxpayerDeclaration (
    LNK_TaxpayerDeclaration_HK  VARBINARY(32)  NOT NULL,
    HUB_Taxpayer_HK              VARBINARY(32)  NOT NULL,
    HUB_Declaration_HK           VARBINARY(32)  NOT NULL,
    LNK_LoadDate                 DATETIME       NOT NULL DEFAULT GETDATE(),
    LNK_RecordSource             NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_LNK_TaxpayerDeclaration PRIMARY KEY (LNK_TaxpayerDeclaration_HK),
    CONSTRAINT FK_LNK_TD_Taxpayer FOREIGN KEY (HUB_Taxpayer_HK) REFERENCES HUB_Taxpayer(HUB_Taxpayer_HK),
    CONSTRAINT FK_LNK_TD_Declaration FOREIGN KEY (HUB_Declaration_HK) REFERENCES HUB_Declaration(HUB_Declaration_HK)
);

CREATE TABLE LNK_DeclarationPayment (
    LNK_DeclarationPayment_HK   VARBINARY(32)  NOT NULL,
    HUB_Declaration_HK           VARBINARY(32)  NOT NULL,
    HUB_Payment_HK               VARBINARY(32)  NOT NULL,
    LNK_LoadDate                  DATETIME       NOT NULL DEFAULT GETDATE(),
    LNK_RecordSource              NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_LNK_DeclarationPayment PRIMARY KEY (LNK_DeclarationPayment_HK),
    CONSTRAINT FK_LNK_DP_Declaration FOREIGN KEY (HUB_Declaration_HK) REFERENCES HUB_Declaration(HUB_Declaration_HK),
    CONSTRAINT FK_LNK_DP_Payment FOREIGN KEY (HUB_Payment_HK) REFERENCES HUB_Payment(HUB_Payment_HK)
);

CREATE TABLE LNK_TaxpayerOfficer (
    LNK_TaxpayerOfficer_HK      VARBINARY(32)  NOT NULL,
    HUB_Taxpayer_HK              VARBINARY(32)  NOT NULL,
    HUB_Officer_HK               VARBINARY(32)  NOT NULL,
    LNK_LoadDate                 DATETIME       NOT NULL DEFAULT GETDATE(),
    LNK_RecordSource             NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_LNK_TaxpayerOfficer PRIMARY KEY (LNK_TaxpayerOfficer_HK),
    CONSTRAINT FK_LNK_TO_Taxpayer FOREIGN KEY (HUB_Taxpayer_HK) REFERENCES HUB_Taxpayer(HUB_Taxpayer_HK),
    CONSTRAINT FK_LNK_TO_Officer FOREIGN KEY (HUB_Officer_HK) REFERENCES HUB_Officer(HUB_Officer_HK)
);

CREATE TABLE LNK_TaxpayerOwner (
    LNK_TaxpayerOwner_HK        VARBINARY(32)  NOT NULL,
    HUB_Taxpayer_HK              VARBINARY(32)  NOT NULL,
    HUB_Owner_HK                 VARBINARY(32)  NOT NULL,
    LNK_LoadDate                 DATETIME       NOT NULL DEFAULT GETDATE(),
    LNK_RecordSource             NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_LNK_TaxpayerOwner PRIMARY KEY (LNK_TaxpayerOwner_HK),
    CONSTRAINT FK_LNK_TOw_Taxpayer FOREIGN KEY (HUB_Taxpayer_HK) REFERENCES HUB_Taxpayer(HUB_Taxpayer_HK),
    CONSTRAINT FK_LNK_TOw_Owner FOREIGN KEY (HUB_Owner_HK) REFERENCES HUB_Owner(HUB_Owner_HK)
);

CREATE TABLE LNK_TaxpayerAnnualDecl (
    LNK_TaxpayerAnnualDecl_HK   VARBINARY(32)  NOT NULL,
    HUB_Taxpayer_HK              VARBINARY(32)  NOT NULL,
    HUB_AnnualDecl_HK            VARBINARY(32)  NOT NULL,
    LNK_LoadDate                 DATETIME       NOT NULL DEFAULT GETDATE(),
    LNK_RecordSource             NVARCHAR(50)    NOT NULL,
    CONSTRAINT PK_LNK_TaxpayerAnnualDecl PRIMARY KEY (LNK_TaxpayerAnnualDecl_HK),
    CONSTRAINT FK_LNK_TAD_Taxpayer FOREIGN KEY (HUB_Taxpayer_HK) REFERENCES HUB_Taxpayer(HUB_Taxpayer_HK),
    CONSTRAINT FK_LNK_TAD_AnnualDecl FOREIGN KEY (HUB_AnnualDecl_HK) REFERENCES HUB_AnnualDecl(HUB_AnnualDecl_HK)
);
GO

PRINT '=== DV_Bronze: 5 Link tables created ===';
GO

-- ─── Bronze Indexes ───

CREATE INDEX IX_SAT_Category_Current ON SAT_Category(HUB_Category_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Structure_Current ON SAT_Structure(HUB_Structure_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Activity_Current ON SAT_Activity(HUB_Activity_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Taxpayer_Current ON SAT_Taxpayer(HUB_Taxpayer_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Owner_Current ON SAT_Owner(HUB_Owner_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Officer_Current ON SAT_Officer(HUB_Officer_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_MonthlyDecl_Current ON SAT_MonthlyDecl(HUB_Declaration_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_Payment_Current ON SAT_Payment(HUB_Payment_HK) WHERE SAT_EndDate IS NULL;
CREATE INDEX IX_SAT_AnnualDecl_Current ON SAT_AnnualDecl(HUB_AnnualDecl_HK) WHERE SAT_EndDate IS NULL;

CREATE INDEX IX_LNK_TD_Taxpayer ON LNK_TaxpayerDeclaration(HUB_Taxpayer_HK);
CREATE INDEX IX_LNK_TD_Declaration ON LNK_TaxpayerDeclaration(HUB_Declaration_HK);
CREATE INDEX IX_LNK_DP_Declaration ON LNK_DeclarationPayment(HUB_Declaration_HK);
CREATE INDEX IX_LNK_DP_Payment ON LNK_DeclarationPayment(HUB_Payment_HK);
CREATE INDEX IX_LNK_TO_Taxpayer ON LNK_TaxpayerOfficer(HUB_Taxpayer_HK);
CREATE INDEX IX_LNK_TOw_Taxpayer ON LNK_TaxpayerOwner(HUB_Taxpayer_HK);
CREATE INDEX IX_LNK_TOw_Owner ON LNK_TaxpayerOwner(HUB_Owner_HK);
CREATE INDEX IX_LNK_TAD_Taxpayer ON LNK_TaxpayerAnnualDecl(HUB_Taxpayer_HK);
GO

PRINT '=== DV_Bronze: Indexes created ===';
GO

-- =============================================
-- 3. SILVER DATABASE (DV_Silver) — Business Vault
-- =============================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DV_Silver')
    CREATE DATABASE DV_Silver;
GO

USE DV_Silver;
GO

-- PIT Tables (3)
CREATE TABLE PIT_Taxpayer (
    PIT_Taxpayer_HK         VARBINARY(32)  NOT NULL,
    PIT_SnapshotDate        DATETIME       NOT NULL,
    SAT_Taxpayer_LoadDate   DATETIME       NULL,
    PIT_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    PIT_BatchID             INT            NOT NULL,
    CONSTRAINT PK_PIT_Taxpayer PRIMARY KEY (PIT_Taxpayer_HK, PIT_SnapshotDate)
);

CREATE TABLE PIT_Declaration (
    PIT_Declaration_HK      VARBINARY(32)  NOT NULL,
    PIT_SnapshotDate        DATETIME       NOT NULL,
    SAT_MonthlyDecl_LoadDate DATETIME      NULL,
    PIT_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    PIT_BatchID             INT            NOT NULL,
    CONSTRAINT PK_PIT_Declaration PRIMARY KEY (PIT_Declaration_HK, PIT_SnapshotDate)
);

CREATE TABLE PIT_Payment (
    PIT_Payment_HK          VARBINARY(32)  NOT NULL,
    PIT_SnapshotDate        DATETIME       NOT NULL,
    SAT_Payment_LoadDate    DATETIME       NULL,
    PIT_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    PIT_BatchID             INT            NOT NULL,
    CONSTRAINT PK_PIT_Payment PRIMARY KEY (PIT_Payment_HK, PIT_SnapshotDate)
);

-- Bridge Table (1)
CREATE TABLE BRG_Taxpayer_Owner (
    HUB_Taxpayer_HK         VARBINARY(32)  NOT NULL,
    HUB_Owner_HK            VARBINARY(32)  NOT NULL,
    BRG_SnapshotDate        DATETIME       NOT NULL,
    BRG_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    BRG_BatchID             INT            NOT NULL,
    TaxID                   VARCHAR(20)    NULL,
    OwnerName               VARCHAR(200)   NULL,
    CONSTRAINT PK_BRG_Taxpayer_Owner PRIMARY KEY (HUB_Taxpayer_HK, HUB_Owner_HK, BRG_SnapshotDate)
);

-- Business Vault Tables (2)
CREATE TABLE BUS_ComplianceScore (
    HUB_Taxpayer_HK         VARBINARY(32)  NOT NULL,
    SnapshotDate            DATETIME       NOT NULL,
    ComplianceScore         DECIMAL(5,2)   NULL,
    FilingOnTimeRate        DECIMAL(5,2)   NULL,
    PaymentOnTimeRate       DECIMAL(5,2)   NULL,
    PenaltyCount            INT            NULL,
    BUS_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    BUS_BatchID             INT            NOT NULL,
    CONSTRAINT PK_BUS_ComplianceScore PRIMARY KEY (HUB_Taxpayer_HK, SnapshotDate)
);

CREATE TABLE BUS_MonthlyMetrics (
    HUB_Taxpayer_HK         VARBINARY(32)  NOT NULL,
    MetricMonth             INT            NOT NULL,
    MetricYear              INT            NOT NULL,
    SnapshotDate            DATETIME       NOT NULL,
    TotalDeclarations       INT            NULL,
    TotalGrossRevenue       DECIMAL(18,2)  NULL,
    TotalTaxAmount          DECIMAL(18,2)  NULL,
    TotalPayments           INT            NULL,
    TotalPaymentAmount      DECIMAL(18,2)  NULL,
    PaymentCoverageRate     DECIMAL(5,2)   NULL,
    BUS_LoadDate            DATETIME       NOT NULL DEFAULT GETDATE(),
    BUS_BatchID             INT            NOT NULL,
    CONSTRAINT PK_BUS_MonthlyMetrics PRIMARY KEY (HUB_Taxpayer_HK, MetricMonth, MetricYear, SnapshotDate)
);
GO

PRINT '=== DV_Silver: 3 PIT + 1 Bridge + 2 Business Vault tables created ===';
GO

-- =============================================
-- 4. GOLD DATABASE (DV_Gold) — Star Schema
-- =============================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DV_Gold')
    CREATE DATABASE DV_Gold;
GO

USE DV_Gold;
GO

-- ─── Dimension Tables (7) ───

-- SCD Type 2
CREATE TABLE DIM_Taxpayer (
    DIM_Taxpayer_SK         INT IDENTITY(1,1) NOT NULL,
    TaxID                   VARCHAR(20)     NOT NULL,
    HUB_Taxpayer_HK         VARBINARY(32)   NOT NULL,
    LegalBusinessName       VARCHAR(300)    NULL,
    TradingName             VARCHAR(300)    NULL,
    RegistrationDate        DATE            NULL,
    EstimatedAnnualRevenue  DECIMAL(18,2)   NULL,
    IsActive                BIT             NULL,
    EffectiveDate           DATETIME        NOT NULL,
    ExpiryDate              DATETIME        NULL,
    IsCurrent               BIT             NOT NULL DEFAULT 1,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Taxpayer PRIMARY KEY (DIM_Taxpayer_SK)
);
CREATE INDEX IX_DIM_Taxpayer_HK ON DIM_Taxpayer(HUB_Taxpayer_HK, IsCurrent);
CREATE INDEX IX_DIM_Taxpayer_TaxID ON DIM_Taxpayer(TaxID, IsCurrent);

-- SCD Type 2
CREATE TABLE DIM_Officer (
    DIM_Officer_SK          INT IDENTITY(1,1) NOT NULL,
    OfficerCode             VARCHAR(20)     NOT NULL,
    HUB_Officer_HK          VARBINARY(32)   NOT NULL,
    FirstName               VARCHAR(100)    NULL,
    LastName                VARCHAR(100)    NULL,
    Department              VARCHAR(100)    NULL,
    IsActive                BIT             NULL,
    EffectiveDate           DATETIME        NOT NULL,
    ExpiryDate              DATETIME        NULL,
    IsCurrent               BIT             NOT NULL DEFAULT 1,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Officer PRIMARY KEY (DIM_Officer_SK)
);
CREATE INDEX IX_DIM_Officer_HK ON DIM_Officer(HUB_Officer_HK, IsCurrent);

-- SCD Type 1
CREATE TABLE DIM_Category (
    DIM_Category_SK         INT IDENTITY(1,1) NOT NULL,
    CategoryID              INT             NOT NULL,
    HUB_Category_HK         VARBINARY(32)   NOT NULL,
    CategoryName            VARCHAR(100)    NULL,
    CategoryDescription     VARCHAR(500)    NULL,
    IsActive                BIT             NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Category PRIMARY KEY (DIM_Category_SK),
    CONSTRAINT UK_DIM_Category_HK UNIQUE (HUB_Category_HK)
);

-- SCD Type 1
CREATE TABLE DIM_Structure (
    DIM_Structure_SK        INT IDENTITY(1,1) NOT NULL,
    StructureID             INT             NOT NULL,
    HUB_Structure_HK        VARBINARY(32)   NOT NULL,
    StructureName           VARCHAR(100)    NULL,
    StructureDescription    VARCHAR(500)    NULL,
    IsActive                BIT             NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Structure PRIMARY KEY (DIM_Structure_SK),
    CONSTRAINT UK_DIM_Structure_HK UNIQUE (HUB_Structure_HK)
);

-- SCD Type 1
CREATE TABLE DIM_Activity (
    DIM_Activity_SK         INT IDENTITY(1,1) NOT NULL,
    ActivityID              INT             NOT NULL,
    HUB_Activity_HK         VARBINARY(32)   NOT NULL,
    ActivityName            VARCHAR(100)    NULL,
    ActivityDescription     VARCHAR(500)    NULL,
    IsActive                BIT             NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Activity PRIMARY KEY (DIM_Activity_SK),
    CONSTRAINT UK_DIM_Activity_HK UNIQUE (HUB_Activity_HK)
);

-- SCD Type 1
CREATE TABLE DIM_PaymentMethod (
    DIM_PaymentMethod_SK    INT IDENTITY(1,1) NOT NULL,
    PaymentMethod           VARCHAR(50)     NOT NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_PaymentMethod PRIMARY KEY (DIM_PaymentMethod_SK),
    CONSTRAINT UK_DIM_PaymentMethod UNIQUE (PaymentMethod)
);

-- SCD Type 1
CREATE TABLE DIM_Status (
    DIM_Status_SK           INT IDENTITY(1,1) NOT NULL,
    StatusCode              VARCHAR(50)     NOT NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    CONSTRAINT PK_DIM_Status PRIMARY KEY (DIM_Status_SK),
    CONSTRAINT UK_DIM_Status UNIQUE (StatusCode)
);
GO

-- ─── Fact Tables (4) ───

CREATE TABLE FACT_MonthlyDeclaration (
    FACT_MonthlyDeclaration_SK  INT IDENTITY(1,1) NOT NULL,
    DIM_Taxpayer_SK         INT             NOT NULL,
    DIM_Category_SK         INT             NULL,
    DIM_Status_SK           INT             NULL,
    DeclarationID           INT             NOT NULL,
    DeclarationMonth        INT             NOT NULL,
    DeclarationYear         INT             NOT NULL,
    GrossRevenue            DECIMAL(18,2)   NULL,
    TaxableRevenue          DECIMAL(18,2)   NULL,
    TaxAmount               DECIMAL(18,2)   NULL,
    PenaltyAmount           DECIMAL(18,2)   NULL,
    InterestAmount          DECIMAL(18,2)   NULL,
    TotalAmount             DECIMAL(18,2)   NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    GLD_SnapshotDate        DATETIME        NOT NULL,
    CONSTRAINT PK_FACT_MonthlyDeclaration PRIMARY KEY (FACT_MonthlyDeclaration_SK),
    CONSTRAINT FK_FACT_MDecl_Taxpayer FOREIGN KEY (DIM_Taxpayer_SK) REFERENCES DIM_Taxpayer(DIM_Taxpayer_SK)
);

CREATE TABLE FACT_Payment (
    FACT_Payment_SK         INT IDENTITY(1,1) NOT NULL,
    DIM_Taxpayer_SK         INT             NULL,
    DIM_PaymentMethod_SK    INT             NULL,
    DIM_Status_SK           INT             NULL,
    PaymentID               INT             NOT NULL,
    DeclarationID           INT             NULL,
    AnnualDeclarationID     INT             NULL,
    PaymentAmount           DECIMAL(18,2)   NULL,
    PaymentDate             DATE            NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    GLD_SnapshotDate        DATETIME        NOT NULL,
    CONSTRAINT PK_FACT_Payment PRIMARY KEY (FACT_Payment_SK),
    CONSTRAINT FK_FACT_Pay_Taxpayer FOREIGN KEY (DIM_Taxpayer_SK) REFERENCES DIM_Taxpayer(DIM_Taxpayer_SK)
);

CREATE TABLE FACT_MonthlySnapshot (
    FACT_MonthlySnapshot_SK INT IDENTITY(1,1) NOT NULL,
    DIM_Taxpayer_SK         INT             NOT NULL,
    MetricMonth             INT             NOT NULL,
    MetricYear              INT             NOT NULL,
    TotalDeclarations       INT             NULL,
    TotalGrossRevenue       DECIMAL(18,2)  NULL,
    TotalTaxAmount          DECIMAL(18,2)  NULL,
    TotalPayments           INT             NULL,
    TotalPaymentAmount      DECIMAL(18,2)  NULL,
    PaymentCoverageRate     DECIMAL(5,2)   NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    GLD_SnapshotDate        DATETIME        NOT NULL,
    CONSTRAINT PK_FACT_MonthlySnapshot PRIMARY KEY (FACT_MonthlySnapshot_SK)
);

CREATE TABLE FACT_DeclarationLifecycle (
    FACT_DeclarationLifecycle_SK INT IDENTITY(1,1) NOT NULL,
    DIM_Taxpayer_SK         INT             NOT NULL,
    DIM_Status_SK           INT             NULL,
    DeclarationID           INT             NOT NULL,
    LifecycleEvent          VARCHAR(50)     NULL,
    EventDate               DATETIME        NULL,
    GLD_LoadDate            DATETIME        NOT NULL DEFAULT GETDATE(),
    GLD_BatchID             INT             NOT NULL,
    GLD_SnapshotDate        DATETIME        NOT NULL,
    CONSTRAINT PK_FACT_DeclarationLifecycle PRIMARY KEY (FACT_DeclarationLifecycle_SK)
);
GO

-- Fact indexes
CREATE INDEX IX_FACT_MDecl_Taxpayer ON FACT_MonthlyDeclaration(DIM_Taxpayer_SK);
CREATE INDEX IX_FACT_MDecl_Snapshot ON FACT_MonthlyDeclaration(GLD_SnapshotDate);
CREATE INDEX IX_FACT_Pay_Taxpayer ON FACT_Payment(DIM_Taxpayer_SK);
CREATE INDEX IX_FACT_Pay_Snapshot ON FACT_Payment(GLD_SnapshotDate);
CREATE INDEX IX_FACT_MSnap_Taxpayer ON FACT_MonthlySnapshot(DIM_Taxpayer_SK);
CREATE INDEX IX_FACT_MSnap_Snapshot ON FACT_MonthlySnapshot(GLD_SnapshotDate);
GO

PRINT '=== DV_Gold: 7 Dimension + 4 Fact tables created ===';
PRINT '=== All 4 databases created successfully ===';
GO