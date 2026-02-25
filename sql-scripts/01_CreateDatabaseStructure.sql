-- =============================================
-- Data Vault 2.0 Implementation Guide
-- Tax System Data Warehouse - Source Database
-- UPDATED VERSION - Dynamic Paths & Best Practices
-- =============================================

-- Create Source Database with Dynamic Path
USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'TaxSystemDB')
    DROP DATABASE TaxSystemDB;
GO

-- =============================================
-- ENHANCEMENT: Use SQL Server Default Data Directory
-- No hardcoded paths - uses SQL Server instance default
-- =============================================
CREATE DATABASE TaxSystemDB;
GO

PRINT 'Database TaxSystemDB created in default SQL Server directory';
PRINT 'Data Path: ' + CONVERT(VARCHAR(500), SERVERPROPERTY('InstanceDefaultDataPath'));
PRINT 'Log Path: ' + CONVERT(VARCHAR(500), SERVERPROPERTY('InstanceDefaultLogPath'));
GO

USE TaxSystemDB;
GO

-- =============================================
-- Create Lookup Tables
-- =============================================

-- Category Lookup
CREATE TABLE Category (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName VARCHAR(100) NOT NULL,
    CategoryDescription VARCHAR(500),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- Structure Lookup
CREATE TABLE Structure (
    StructureID INT IDENTITY(1,1) PRIMARY KEY,
    StructureName VARCHAR(100) NOT NULL,
    StructureDescription VARCHAR(500),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- Activity Lookup
CREATE TABLE Activity (
    ActivityID INT IDENTITY(1,1) PRIMARY KEY,
    ActivityName VARCHAR(100) NOT NULL,
    ActivityDescription VARCHAR(500),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
);

-- =============================================
-- Create Core Business Tables
-- =============================================

-- Taxpayer Table
CREATE TABLE Taxpayer (
    TaxpayerID INT IDENTITY(1,1) PRIMARY KEY,
    TaxID VARCHAR(20) NOT NULL UNIQUE,
    LegalBusinessName VARCHAR(300) NOT NULL,
    TradingName VARCHAR(300),
    CategoryID INT NOT NULL,
    StructureID INT NOT NULL,
    RegistrationDate DATE NOT NULL,
    EstimatedAnnualRevenue DECIMAL(18,2),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem',
    FOREIGN KEY (CategoryID) REFERENCES Category(CategoryID),
    FOREIGN KEY (StructureID) REFERENCES Structure(StructureID)
);

-- Owner Table
CREATE TABLE Owner (
    OwnerID INT IDENTITY(1,1) PRIMARY KEY,
    TaxpayerID INT NOT NULL,
    OwnerName VARCHAR(200) NOT NULL,
    OwnerType VARCHAR(50) NOT NULL, -- Individual, Company, Trust
    OwnershipPercentage DECIMAL(5,2),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem',
    FOREIGN KEY (TaxpayerID) REFERENCES Taxpayer(TaxpayerID)
);

-- Officer Table
CREATE TABLE Officer (
    OfficerID INT IDENTITY(1,1) PRIMARY KEY,
    OfficerCode VARCHAR(20) NOT NULL UNIQUE,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    Department VARCHAR(100),
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem'
);

-- Monthly Declaration Table
CREATE TABLE MonthlyDeclaration (
    DeclarationID INT IDENTITY(1,1) PRIMARY KEY,
    TaxpayerID INT NOT NULL,
    DeclarationMonth INT NOT NULL CHECK (DeclarationMonth BETWEEN 1 AND 12), -- ENHANCEMENT: Added constraint
    DeclarationYear INT NOT NULL CHECK (DeclarationYear BETWEEN 1900 AND 2100), -- ENHANCEMENT: Added constraint
    GrossRevenue DECIMAL(18,2) NOT NULL CHECK (GrossRevenue >= 0), -- ENHANCEMENT: Added constraint
    TaxableRevenue DECIMAL(18,2) NOT NULL CHECK (TaxableRevenue >= 0),
    TaxAmount DECIMAL(18,2) NOT NULL CHECK (TaxAmount >= 0),
    PenaltyAmount DECIMAL(18,2) DEFAULT 0 CHECK (PenaltyAmount >= 0),
    InterestAmount DECIMAL(18,2) DEFAULT 0 CHECK (InterestAmount >= 0),
    TotalAmount DECIMAL(18,2) NOT NULL CHECK (TotalAmount >= 0),
    DeclarationDate DATE NOT NULL,
    DueDate DATE NOT NULL,
    Status VARCHAR(50) DEFAULT 'Pending', -- Pending, Submitted, Approved, Rejected
    OfficerID INT,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem',
    FOREIGN KEY (TaxpayerID) REFERENCES Taxpayer(TaxpayerID),
    FOREIGN KEY (OfficerID) REFERENCES Officer(OfficerID),
    -- ENHANCEMENT: Added constraint to ensure TotalAmount is consistent
    CONSTRAINT CHK_MonthlyDeclaration_TotalAmount 
        CHECK (TotalAmount = TaxAmount + PenaltyAmount + InterestAmount)
);

-- Annual Declaration Table
CREATE TABLE AnnualDeclaration (
    AnnualDeclarationID INT IDENTITY(1,1) PRIMARY KEY,
    TaxpayerID INT NOT NULL,
    DeclarationYear INT NOT NULL CHECK (DeclarationYear BETWEEN 1900 AND 2100),
    GrossRevenue DECIMAL(18,2) NOT NULL CHECK (GrossRevenue >= 0),
    TaxableRevenue DECIMAL(18,2) NOT NULL CHECK (TaxableRevenue >= 0),
    TaxAmount DECIMAL(18,2) NOT NULL CHECK (TaxAmount >= 0),
    PenaltyAmount DECIMAL(18,2) DEFAULT 0 CHECK (PenaltyAmount >= 0),
    InterestAmount DECIMAL(18,2) DEFAULT 0 CHECK (InterestAmount >= 0),
    TotalAmount DECIMAL(18,2) NOT NULL CHECK (TotalAmount >= 0),
    DeclarationDate DATE NOT NULL,
    DueDate DATE NOT NULL,
    Status VARCHAR(50) DEFAULT 'Pending',
    OfficerID INT,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem',
    FOREIGN KEY (TaxpayerID) REFERENCES Taxpayer(TaxpayerID),
    FOREIGN KEY (OfficerID) REFERENCES Officer(OfficerID),
    CONSTRAINT CHK_AnnualDeclaration_TotalAmount 
        CHECK (TotalAmount = TaxAmount + PenaltyAmount + InterestAmount)
);

-- Payment Table
CREATE TABLE Payment (
    PaymentID INT IDENTITY(1,1) PRIMARY KEY,
    TaxpayerID INT NOT NULL,
    DeclarationID INT, -- Can be NULL for advance payments
    AnnualDeclarationID INT, -- Can be NULL for monthly payments
    PaymentAmount DECIMAL(18,2) NOT NULL CHECK (PaymentAmount > 0), -- ENHANCEMENT: Added constraint
    PaymentDate DATE NOT NULL,
    PaymentMethod VARCHAR(50) NOT NULL, -- Cash, Bank Transfer, Check
    ReferenceNumber VARCHAR(100),
    Status VARCHAR(50) DEFAULT 'Completed', -- Completed, Pending, Failed
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    SourceSystem VARCHAR(50) DEFAULT 'TaxSystem',
    FOREIGN KEY (TaxpayerID) REFERENCES Taxpayer(TaxpayerID),
    FOREIGN KEY (DeclarationID) REFERENCES MonthlyDeclaration(DeclarationID),
    FOREIGN KEY (AnnualDeclarationID) REFERENCES AnnualDeclaration(AnnualDeclarationID),
    -- ENHANCEMENT: Added constraint to ensure either monthly or annual declaration
    CONSTRAINT CHK_Payment_DeclarationType 
        CHECK ((DeclarationID IS NOT NULL AND AnnualDeclarationID IS NULL) OR 
               (DeclarationID IS NULL AND AnnualDeclarationID IS NOT NULL) OR
               (DeclarationID IS NULL AND AnnualDeclarationID IS NULL))
);

-- =============================================
-- Create Indexes for Performance
-- ENHANCED: Added more indexes based on analysis
-- =============================================

-- Taxpayer indexes
CREATE INDEX IX_Taxpayer_TaxID ON Taxpayer(TaxID);
CREATE INDEX IX_Taxpayer_CategoryID ON Taxpayer(CategoryID);
CREATE INDEX IX_Taxpayer_UpdatedDate ON Taxpayer(UpdatedDate);
CREATE INDEX IX_Taxpayer_StructureID ON Taxpayer(StructureID); -- ENHANCEMENT: Added
CREATE INDEX IX_Taxpayer_IsActive ON Taxpayer(IsActive) WHERE IsActive = 1; -- ENHANCEMENT: Filtered index

-- Monthly Declaration indexes
CREATE INDEX IX_MonthlyDeclaration_TaxpayerID ON MonthlyDeclaration(TaxpayerID);
CREATE INDEX IX_MonthlyDeclaration_YearMonth ON MonthlyDeclaration(DeclarationYear, DeclarationMonth);
CREATE INDEX IX_MonthlyDeclaration_UpdatedDate ON MonthlyDeclaration(UpdatedDate);
CREATE INDEX IX_MonthlyDeclaration_Status ON MonthlyDeclaration(Status); -- ENHANCEMENT: Added
CREATE INDEX IX_MonthlyDeclaration_OfficerID ON MonthlyDeclaration(OfficerID); -- ENHANCEMENT: Added

-- Annual Declaration indexes
CREATE INDEX IX_AnnualDeclaration_TaxpayerID ON AnnualDeclaration(TaxpayerID);
CREATE INDEX IX_AnnualDeclaration_Year ON AnnualDeclaration(DeclarationYear);
CREATE INDEX IX_AnnualDeclaration_UpdatedDate ON AnnualDeclaration(UpdatedDate);
CREATE INDEX IX_AnnualDeclaration_Status ON AnnualDeclaration(Status); -- ENHANCEMENT: Added

-- Payment indexes
CREATE INDEX IX_Payment_TaxpayerID ON Payment(TaxpayerID);
CREATE INDEX IX_Payment_DeclarationID ON Payment(DeclarationID);
CREATE INDEX IX_Payment_AnnualDeclarationID ON Payment(AnnualDeclarationID); -- ENHANCEMENT: Added
CREATE INDEX IX_Payment_PaymentDate ON Payment(PaymentDate);
CREATE INDEX IX_Payment_UpdatedDate ON Payment(UpdatedDate);
CREATE INDEX IX_Payment_Status ON Payment(Status); -- ENHANCEMENT: Added

-- Owner indexes
CREATE INDEX IX_Owner_TaxpayerID ON Owner(TaxpayerID);
CREATE INDEX IX_Owner_UpdatedDate ON Owner(UpdatedDate);
CREATE INDEX IX_Owner_OwnerType ON Owner(OwnerType); -- ENHANCEMENT: Added

-- Officer indexes
CREATE INDEX IX_Officer_OfficerCode ON Officer(OfficerCode);
CREATE INDEX IX_Officer_UpdatedDate ON Officer(UpdatedDate);
CREATE INDEX IX_Officer_Department ON Officer(Department); -- ENHANCEMENT: Added

-- =============================================
-- ENHANCEMENT: Create Data Quality Views
-- =============================================

GO
CREATE VIEW vw_DataQuality_Overview
AS
SELECT 
    'Taxpayers' AS EntityType,
    COUNT(*) AS TotalRecords,
    SUM(CASE WHEN IsActive = 1 THEN 1 ELSE 0 END) AS ActiveRecords,
    SUM(CASE WHEN UpdatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END) AS RecentUpdates
FROM Taxpayer
UNION ALL
SELECT 'Monthly Declarations', COUNT(*), 
    SUM(CASE WHEN Status IN ('Approved', 'Submitted') THEN 1 ELSE 0 END),
    SUM(CASE WHEN UpdatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END)
FROM MonthlyDeclaration
UNION ALL
SELECT 'Annual Declarations', COUNT(*),
    SUM(CASE WHEN Status IN ('Approved', 'Submitted') THEN 1 ELSE 0 END),
    SUM(CASE WHEN UpdatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END)
FROM AnnualDeclaration
UNION ALL
SELECT 'Payments', COUNT(*),
    SUM(CASE WHEN Status = 'Completed' THEN 1 ELSE 0 END),
    SUM(CASE WHEN UpdatedDate >= DATEADD(DAY, -7, GETDATE()) THEN 1 ELSE 0 END)
FROM Payment;
GO

-- =============================================
-- ENHANCEMENT: Statistics Update Procedure
-- =============================================

CREATE PROCEDURE usp_UpdateStatistics
AS
BEGIN
    SET NOCOUNT ON;
    
    UPDATE STATISTICS Taxpayer WITH FULLSCAN;
    UPDATE STATISTICS Owner WITH FULLSCAN;
    UPDATE STATISTICS Officer WITH FULLSCAN;
    UPDATE STATISTICS MonthlyDeclaration WITH FULLSCAN;
    UPDATE STATISTICS AnnualDeclaration WITH FULLSCAN;
    UPDATE STATISTICS Payment WITH FULLSCAN;
    UPDATE STATISTICS Category WITH FULLSCAN;
    UPDATE STATISTICS Structure WITH FULLSCAN;
    UPDATE STATISTICS Activity WITH FULLSCAN;
    
    PRINT 'Statistics updated successfully!';
END;
GO