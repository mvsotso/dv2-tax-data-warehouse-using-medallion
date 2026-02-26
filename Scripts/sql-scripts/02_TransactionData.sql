-- =============================================
-- Data Vault 2.0 Implementation Guide
-- Tax System Data Warehouse - Sample Data
-- =============================================

USE TaxSystemDB;
GO

-- =============================================
-- Insert Lookup Data
-- =============================================

-- Insert Categories
INSERT INTO Category (CategoryName, CategoryDescription) VALUES
('Individual', 'Individual taxpayers'),
('Small Business', 'Small business entities'),
('Corporation', 'Large corporate entities'),
('Partnership', 'Partnership entities'),
('Trust', 'Trust entities');

-- Insert Structures
INSERT INTO Structure (StructureName, StructureDescription) VALUES
('Sole Proprietorship', 'Single owner business'),
('Partnership', 'Multiple owner business'),
('Corporation', 'Corporate structure'),
('LLC', 'Limited Liability Company'),
('Trust', 'Trust structure');

-- Insert Activities
INSERT INTO Activity (ActivityName, ActivityDescription) VALUES
('Retail', 'Retail business activities'),
('Manufacturing', 'Manufacturing activities'),
('Services', 'Service-based activities'),
('Construction', 'Construction activities'),
('Technology', 'Technology services');

-- Insert Officers
INSERT INTO Officer (OfficerCode, FirstName, LastName, Department) VALUES
('OFF001', 'John', 'Smith', 'Tax Collection'),
('OFF002', 'Sarah', 'Johnson', 'Tax Collection'),
('OFF003', 'Michael', 'Brown', 'Tax Collection'),
('OFF004', 'Emily', 'Davis', 'Tax Collection'),
('OFF005', 'David', 'Wilson', 'Tax Collection'),
('OFF006', 'Lisa', 'Anderson', 'Tax Collection'),
('OFF007', 'Robert', 'Taylor', 'Tax Collection'),
('OFF008', 'Jennifer', 'Thomas', 'Tax Collection'),
('OFF009', 'William', 'Jackson', 'Tax Collection'),
('OFF010', 'Mary', 'White', 'Tax Collection');

-- =============================================
-- Generate Sample Taxpayers
-- =============================================

DECLARE @Counter INT = 1;
DECLARE @MaxTaxpayers INT = 1000;
DECLARE @CategoryCount INT = (SELECT COUNT(*) FROM Category);
DECLARE @StructureCount INT = (SELECT COUNT(*) FROM Structure);

WHILE @Counter <= @MaxTaxpayers
BEGIN
    INSERT INTO Taxpayer (
        TaxID,
        LegalBusinessName,
        TradingName,
        CategoryID,
        StructureID,
        RegistrationDate,
        EstimatedAnnualRevenue,
        UpdatedDate
    )
    VALUES (
        'TAX' + RIGHT('000000' + CAST(@Counter AS VARCHAR(6)), 6),
        'Business ' + CAST(@Counter AS VARCHAR(10)),
        'Trading Name ' + CAST(@Counter AS VARCHAR(10)),
        (@Counter % @CategoryCount) + 1,
        (@Counter % @StructureCount) + 1,
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 3650, GETDATE()), -- Random date within last 10 years
        ABS(CHECKSUM(NEWID())) % 1000000 + 10000, -- Random revenue between 10K and 1M
        DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE()) -- Random update within last 30 days
    );
    
    SET @Counter = @Counter + 1;
END;

-- =============================================
-- Generate Sample Owners
-- =============================================

DECLARE @TaxpayerCount INT = (SELECT COUNT(*) FROM Taxpayer);
DECLARE @OwnerCounter INT = 1;

WHILE @OwnerCounter <= @TaxpayerCount
BEGIN
    DECLARE @OwnerCount INT = (ABS(CHECKSUM(NEWID())) % 3) + 1; -- 1-3 owners per taxpayer
    DECLARE @OwnerIndex INT = 1;
    
    WHILE @OwnerIndex <= @OwnerCount
    BEGIN
        INSERT INTO Owner (
            TaxpayerID,
            OwnerName,
            OwnerType,
            OwnershipPercentage,
            UpdatedDate
        )
        VALUES (
            @OwnerCounter,
            'Owner ' + CAST(@OwnerIndex AS VARCHAR(10)) + ' of Taxpayer ' + CAST(@OwnerCounter AS VARCHAR(10)),
            CASE (@OwnerIndex % 3)
                WHEN 0 THEN 'Individual'
                WHEN 1 THEN 'Company'
                ELSE 'Trust'
            END,
            CASE @OwnerCount
                WHEN 1 THEN 100.00
                WHEN 2 THEN 50.00
                ELSE 33.33
            END,
            DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE())
        );
        
        SET @OwnerIndex = @OwnerIndex + 1;
    END;
    
    SET @OwnerCounter = @OwnerCounter + 1;
END;

-- =============================================
-- Generate Sample Monthly Declarations
-- =============================================

DECLARE @TaxpayerCounter INT = 1;
DECLARE @OfficerCount INT = (SELECT COUNT(*) FROM Officer);

WHILE @TaxpayerCounter <= @TaxpayerCount
BEGIN
    DECLARE @Year INT = 2020;
    
    -- Generate declarations for last 3 years
    WHILE @Year <= 2023
    BEGIN
        DECLARE @Month INT = 1;
        
        WHILE @Month <= 12
        BEGIN
            -- 80% chance of having a declaration for each month
            IF (ABS(CHECKSUM(NEWID())) % 100) < 80
            BEGIN
                DECLARE @Revenue DECIMAL(18,2) = ABS(CHECKSUM(NEWID())) % 100000 + 1000;
                DECLARE @TaxRate DECIMAL(5,2) = 15.0 + (ABS(CHECKSUM(NEWID())) % 10); -- 15-25% tax rate
                DECLARE @TaxAmount DECIMAL(18,2) = @Revenue * (@TaxRate / 100);
                DECLARE @Penalty DECIMAL(18,2) = CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 5 THEN ABS(CHECKSUM(NEWID())) % 1000 ELSE 0 END;
                DECLARE @Interest DECIMAL(18,2) = CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 3 THEN ABS(CHECKSUM(NEWID())) % 500 ELSE 0 END;
                
                INSERT INTO MonthlyDeclaration (
                    TaxpayerID,
                    DeclarationMonth,
                    DeclarationYear,
                    GrossRevenue,
                    TaxableRevenue,
                    TaxAmount,
                    PenaltyAmount,
                    InterestAmount,
                    TotalAmount,
                    DeclarationDate,
                    DueDate,
                    Status,
                    OfficerID,
                    UpdatedDate
                )
                VALUES (
                    @TaxpayerCounter,
                    @Month,
                    @Year,
                    @Revenue,
                    @Revenue * 0.9, -- 90% taxable
                    @TaxAmount,
                    @Penalty,
                    @Interest,
                    @TaxAmount + @Penalty + @Interest,
                    DATEFROMPARTS(@Year, @Month, 15), -- 15th of each month
                    DATEFROMPARTS(@Year, @Month, 25), -- Due 25th of each month
                    CASE (ABS(CHECKSUM(NEWID())) % 4)
                        WHEN 0 THEN 'Pending'
                        WHEN 1 THEN 'Submitted'
                        WHEN 2 THEN 'Approved'
                        ELSE 'Rejected'
                    END,
                    (ABS(CHECKSUM(NEWID())) % @OfficerCount) + 1,
                    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE())
                );
            END;
            
            SET @Month = @Month + 1;
        END;
        
        SET @Year = @Year + 1;
    END;
    
    SET @TaxpayerCounter = @TaxpayerCounter + 1;
END;

-- =============================================
-- Generate Sample Annual Declarations
-- =============================================

SET @TaxpayerCounter = 1;

WHILE @TaxpayerCounter <= @TaxpayerCount
BEGIN
    SET @Year = 2020;
    
    -- Generate annual declarations for last 3 years
    WHILE @Year <= 2023
    BEGIN
        -- 90% chance of having an annual declaration
        IF (ABS(CHECKSUM(NEWID())) % 100) < 90
        BEGIN
            DECLARE @AnnualRevenue DECIMAL(18,2) = ABS(CHECKSUM(NEWID())) % 2000000 + 50000;
            DECLARE @AnnualTaxRate DECIMAL(5,2) = 20.0 + (ABS(CHECKSUM(NEWID())) % 15); -- 20-35% tax rate
            DECLARE @AnnualTaxAmount DECIMAL(18,2) = @AnnualRevenue * (@AnnualTaxRate / 100);
            DECLARE @AnnualPenalty DECIMAL(18,2) = CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 8 THEN ABS(CHECKSUM(NEWID())) % 5000 ELSE 0 END;
            DECLARE @AnnualInterest DECIMAL(18,2) = CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 5 THEN ABS(CHECKSUM(NEWID())) % 2000 ELSE 0 END;
            
            INSERT INTO AnnualDeclaration (
                TaxpayerID,
                DeclarationYear,
                GrossRevenue,
                TaxableRevenue,
                TaxAmount,
                PenaltyAmount,
                InterestAmount,
                TotalAmount,
                DeclarationDate,
                DueDate,
                Status,
                OfficerID,
                UpdatedDate
            )
            VALUES (
                @TaxpayerCounter,
                @Year,
                @AnnualRevenue,
                @AnnualRevenue * 0.85, -- 85% taxable
                @AnnualTaxAmount,
                @AnnualPenalty,
                @AnnualInterest,
                @AnnualTaxAmount + @AnnualPenalty + @AnnualInterest,
                DATEFROMPARTS(@Year, 12, 31), -- End of year
                DATEFROMPARTS(@Year + 1, 3, 31), -- Due March 31st next year
                CASE (ABS(CHECKSUM(NEWID())) % 4)
                    WHEN 0 THEN 'Pending'
                    WHEN 1 THEN 'Submitted'
                    WHEN 2 THEN 'Approved'
                    ELSE 'Rejected'
                END,
                (ABS(CHECKSUM(NEWID())) % @OfficerCount) + 1,
                DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE())
            );
        END;
        
        SET @Year = @Year + 1;
    END;
    
    SET @TaxpayerCounter = @TaxpayerCounter + 1;
END;

-- =============================================
-- Generate Sample Payments
-- =============================================

-- Generate payments for monthly declarations
INSERT INTO Payment (
    TaxpayerID,
    DeclarationID,
    PaymentAmount,
    PaymentDate,
    PaymentMethod,
    ReferenceNumber,
    Status,
    UpdatedDate
)
SELECT 
    md.TaxpayerID,
    md.DeclarationID,
    md.TotalAmount,
    DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 10, md.DueDate), -- Payment within 10 days of due date
    CASE (ABS(CHECKSUM(NEWID())) % 3)
        WHEN 0 THEN 'Bank Transfer'
        WHEN 1 THEN 'Cash'
        ELSE 'Check'
    END,
    'PAY' + RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY md.DeclarationID) AS VARCHAR(10)), 10),
    CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 95 THEN 'Completed' ELSE 'Failed' END,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE())
FROM MonthlyDeclaration md
WHERE md.Status IN ('Approved', 'Submitted')
AND (ABS(CHECKSUM(NEWID())) % 100) < 70; -- 70% of declarations have payments

-- Generate payments for annual declarations
INSERT INTO Payment (
    TaxpayerID,
    AnnualDeclarationID,
    PaymentAmount,
    PaymentDate,
    PaymentMethod,
    ReferenceNumber,
    Status,
    UpdatedDate
)
SELECT 
    ad.TaxpayerID,
    ad.AnnualDeclarationID,
    ad.TotalAmount,
    DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 30, ad.DueDate), -- Payment within 30 days of due date
    CASE (ABS(CHECKSUM(NEWID())) % 3)
        WHEN 0 THEN 'Bank Transfer'
        WHEN 1 THEN 'Cash'
        ELSE 'Check'
    END,
    'PAY' + RIGHT('000000' + CAST(ROW_NUMBER() OVER (ORDER BY ad.AnnualDeclarationID) + 100000 AS VARCHAR(10)), 10),
    CASE WHEN (ABS(CHECKSUM(NEWID())) % 100) < 90 THEN 'Completed' ELSE 'Failed' END,
    DATEADD(DAY, -ABS(CHECKSUM(NEWID())) % 30, GETDATE())
FROM AnnualDeclaration ad
WHERE ad.Status IN ('Approved', 'Submitted')
AND (ABS(CHECKSUM(NEWID())) % 100) < 80; -- 80% of annual declarations have payments

-- =============================================
-- Verification Queries
-- =============================================

PRINT 'Sample data generation completed!';
PRINT 'Verification:';

SELECT 'Taxpayers' AS Entity, COUNT(*) AS RecordCount FROM Taxpayer
UNION ALL
SELECT 'Owners', COUNT(*) FROM Owner
UNION ALL
SELECT 'Officers', COUNT(*) FROM Officer
UNION ALL
SELECT 'MonthlyDeclarations', COUNT(*) FROM MonthlyDeclaration
UNION ALL
SELECT 'AnnualDeclarations', COUNT(*) FROM AnnualDeclaration
UNION ALL
SELECT 'Payments', COUNT(*) FROM Payment;

PRINT 'Data generation completed successfully!';

