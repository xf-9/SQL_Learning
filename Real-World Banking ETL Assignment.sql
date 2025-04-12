USE master;

CREATE DATABASE ETL_Assignment1
COLLATE Latin1_General_CS_AS;
GO
USE ETL_Assignment1;
-- Verify whether the database is case sensitive
SELECT CASE WHEN 'A' = 'a' THEN 'NOT CASE SENSITIVE' ELSE 'CASE SENSITIVE' END;

-- STEP 1: Create reference tables and populate with sample data

-- 1. Interest Rates (monthly base rates)
CREATE TABLE InterestRates (
    RateID INT IDENTITY(1,1) PRIMARY KEY,
    EffectiveDate DATE NOT NULL,
    BaseInterestRate DECIMAL(5, 2) NOT NULL
);

INSERT INTO InterestRates (EffectiveDate, BaseInterestRate)
VALUES
  ('2025-04-01', 3.50),
  ('2025-05-01', 3.75);

-- 2. Energy Class Margin
CREATE TABLE EnergyClassMargin (
    EnergyClass VARCHAR(20) PRIMARY KEY,
    MarginRate DECIMAL(5, 2) NOT NULL
);

INSERT INTO EnergyClassMargin (EnergyClass, MarginRate)
VALUES
  ('Electric', 0.20),
  ('Hybrid',   0.30),
  ('Gasoline', 0.50),
  ('Diesel',   0.70);

-- 3. Credit Risk Tier Adjustments
CREATE TABLE CreditRiskTier (
    RiskTier VARCHAR(20) PRIMARY KEY,
    RiskAdjustment DECIMAL(5, 2) NOT NULL
);

INSERT INTO CreditRiskTier (RiskTier, RiskAdjustment)
VALUES
  ('Excellent', -0.30),
  ('Good', -0.10),
  ('Average', 0.20),
  ('Poor', 0.50);

-- 4. Depreciation Rates
CREATE TABLE DepreciationRates (
    MinYear INT,
    MaxYear INT,
    DepreciationRate DECIMAL(5,2)
);

INSERT INTO DepreciationRates (MinYear, MaxYear, DepreciationRate)
VALUES
  (0, 1, 0.10),
  (2, 3, 0.20),
  (4, 6, 0.35),
  (7, 99, 0.50);

-- STEP 2: Monthly Vehicle Data (Staging Table)

CREATE TABLE CarInformation_202505 (
    RecordID INT PRIMARY KEY,
    CarModel VARCHAR(100),
    EnergyClass VARCHAR(20),
    ManufactureYear INT,
    BasePrice DECIMAL(10,2),
    FileMonth DATE,
    CustomerRiskTier VARCHAR(20)
);

INSERT INTO CarInformation_202505
    (RecordID, CarModel, EnergyClass, ManufactureYear, BasePrice, FileMonth, CustomerRiskTier)
VALUES
    (1, 'EcoCar X1', 'Electric', 2023, 35000.00, '2025-05-01', 'Excellent'),
    (2, 'Speedster G2', 'Gasoline', 2020, 27000.00, '2025-05-01', 'Good'),
    (3, 'FamilyVan D3', 'Diesel', 2018, 22000.00, '2025-05-01', 'Average'),
    (4, 'Compact EV4', 'Electric', 2024, 32000.00, '2025-05-01', 'Poor'),
    (5, 'Hybrid Cruiser', 'Hybrid', 2021, 31000.00, '2025-05-01', 'Good');

-- STEP 3: Final Fact Table

CREATE TABLE LoanProfitEstimates (
    RecordID INT PRIMARY KEY,
    CarModel VARCHAR(100),
    EnergyClass VARCHAR(20),
    ManufactureYear INT,
    BasePrice DECIMAL(10,2),
    CustomerRiskTier VARCHAR(20),
    FinalInterestRate DECIMAL(5,2),
    EstimatedMonthlyPayment DECIMAL(10,2),
    DepreciatedValue DECIMAL(10,2),
    EstimatedProfit DECIMAL(10,2),
    DataMonth DATE
);

-- STEP 4
INSERT INTO LoanProfitEstimates (
    RecordID,
    CarModel,
    EnergyClass,
    ManufactureYear,
    BasePrice,
    CustomerRiskTier,
    FinalInterestRate,
    EstimatedMonthlyPayment,
    DepreciatedValue,
    EstimatedProfit,
    DataMonth
)
SELECT
    c.RecordID,
    c.CarModel,
    c.EnergyClass,
    c.ManufactureYear,
    c.BasePrice,
    c.CustomerRiskTier,
    -- Calculate Final Interest Rate
    ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment AS FinalInterestRate,
    -- Calculate Estimated Monthly Payment (simple interest-only formula)
    (c.BasePrice * (ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment) / 100) / 12 AS EstimatedMonthlyPayment,
    -- Calculate Depreciated Value based on vehicle age and matching depreciation rate
    c.BasePrice * (1 - dr.DepreciationRate) AS DepreciatedValue,
    -- Calculate Estimated Profit (basic model)
    (
        ((c.BasePrice * (ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment) / 100)) -
        (c.BasePrice - (c.BasePrice * (1 - dr.DepreciationRate)))
    ) AS EstimatedProfit,
    c.FileMonth
FROM CarInformation_202505 c
JOIN InterestRates ir
    ON YEAR(c.FileMonth) = YEAR(ir.EffectiveDate)
   AND MONTH(c.FileMonth) = MONTH(ir.EffectiveDate)
JOIN EnergyClassMargin ecm
    ON c.EnergyClass = ecm.EnergyClass
JOIN CreditRiskTier crt
    ON c.CustomerRiskTier = crt.RiskTier
JOIN DepreciationRates dr
    ON (YEAR(c.FileMonth) - c.ManufactureYear) BETWEEN dr.MinYear AND dr.MaxYear;

SELECT * FROM LoanProfitEstimates ORDER BY RecordID;

------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE CarInformation_202506;
CREATE TABLE CarInformation_202506 (
    RecordID INT PRIMARY KEY,
    CarModel VARCHAR(100),
    EnergyClass VARCHAR(20),
    ManufactureYear INT,
    BasePrice DECIMAL(10,2),
    FileMonth DATE,
    CustomerRiskTier VARCHAR(20)
);

INSERT INTO CarInformation_202506
    (RecordID, CarModel, EnergyClass, ManufactureYear, BasePrice, FileMonth, CustomerRiskTier)
VALUES
    (6, 'VoltRunner X2', 'Electric', 2024, 36000.00, '2025-06-01', 'Excellent'),
    (7, 'CityDrive LX', 'Hybrid', 2022, 29500.00, '2025-06-01', 'Good'),
    (8, 'PowerTruck D9', 'Diesel', 2016, 28000.00, '2025-06-01', 'Average'),
    (9, 'SpeedKing G3', 'Gasoline', 2021, 25000.00, '2025-06-01', 'Poor'),
    (10, 'EcoFlex Mini', 'Electric', 2023, 33000.00, '2025-06-01', 'Good');

INSERT INTO InterestRates (EffectiveDate, BaseInterestRate)
VALUES
  ('2025-06-01', 3.80);
-- A new base interest rate will be inserted monthly, kommer från bussiness side
SELECT * FROM InterestRates;

DELETE FROM InterestRates WHERE RateID = 3;

SELECT * FROM EnergyClassMargin; -- MarginRate can be changed monthly, or unchanged
SELECT * FROM CreditRiskTier; -- Risk Adjustment can be changed monthly, or unchanged
SELECT * FROM DepreciationRates; -- can be changed monthly, Possibly unchanged for few years
SELECT * FROM CarInformation_202505;
SELECT * FROM CarInformation_202506;
SELECT * FROM InterestRates

SELECT * FROM LoanProfitEstimates;


DECLARE @TABLENAME VARCHAR(50)
DECLARE @SQL NVARCHAR(MAX)

DECLARE @MT INT, @YR INT
SET @MT = MONTH(GETDATE())
SET @YR = YEAR(GETDATE())
-- SELECT CONCAT(@YR,  FORMAT(@MT,'00'))

SET @TABLENAME = CONCAT('CarInformation_',CONCAT(@YR,  FORMAT(@MT,'00')))
SET @SQL = 'SELECT * FROM ' + QUOTENAME(@TABLENAME) + ' AS CI LEFT JOIN CreditRiskTier AS CR ON CR.RiskTier = CI.CustomerRiskTier'

EXEC sp_executesql @SQL

DROP TABLE InterestRates
CREATE TABLE InterestRates (
    RateID INT IDENTITY(1,1) PRIMARY KEY,
    EffectiveDate DATE NOT NULL,
    BaseInterestRate DECIMAL(5, 2) NOT NULL
);

INSERT INTO InterestRates (EffectiveDate, BaseInterestRate)
VALUES
  ('2025-03-01', 3.50),
  ('2025-04-01', 3.75);


-- First Part
-- Fetches the correct CarInformation_yyyymm table in the job.
-- Automatically retrieves values from the previous month if the "Base interest rate" in the InterestRates table for the current month is missing.
-- Generates an output as a new table with dynamic name LoanProfitEstimates_yyyymm without errors.


-- Second Part：
-- The code is both efficient and easy to read.

-- If any error is detected, it logs a descriptive message indicating the type of error. For example:
-- "The base interest rate for the current month is missing; the value from the previous month was used."
-- "The CarInformation_yyyymm table for the current month is missing." etc.
-- Sends a confirmation email indicating whether the job was successful, including a detailed description.


SELECT
    c.RecordID,
    c.CarModel,
    c.EnergyClass,
    c.ManufactureYear,
    c.BasePrice,
    c.CustomerRiskTier,
    -- Calculate Final Interest Rate
    ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment AS FinalInterestRate,
    -- Calculate Estimated Monthly Payment (simple interest-only formula)
    (c.BasePrice * (ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment) / 100) / 12 AS EstimatedMonthlyPayment,
    -- Calculate Depreciated Value based on vehicle age and matching depreciation rate
    c.BasePrice * (1 - dr.DepreciationRate) AS DepreciatedValue,
    -- Calculate Estimated Profit (basic model)
    (
        ((c.BasePrice * (ir.BaseInterestRate + ecm.MarginRate + crt.RiskAdjustment) / 100)) -
        (c.BasePrice - (c.BasePrice * (1 - dr.DepreciationRate)))
    ) AS EstimatedProfit,
    c.FileMonth,
	ir.BaseInterestRate
FROM CarInformation_202505 c
JOIN InterestRates ir
    ON YEAR(c.FileMonth) = YEAR(ir.EffectiveDate)
   AND MONTH(c.FileMonth) = MONTH(DATEADD(MONTH,1,ir.EffectiveDate))
JOIN EnergyClassMargin ecm
    ON c.EnergyClass = ecm.EnergyClass
JOIN CreditRiskTier crt
    ON c.CustomerRiskTier = crt.RiskTier
JOIN DepreciationRates dr
    ON (YEAR(c.FileMonth) - c.ManufactureYear) BETWEEN dr.MinYear AND dr.MaxYear;


SELECT * FROM InterestRates
SELECT * FROM CarInformation_202505

SELECT MONTH(DATEADD(MONTH,1,GETDATE()))
SELECT MONTH(GETDATE())