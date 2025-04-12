--  Create the error log table if it doesn't exist
IF OBJECT_ID('ETL_ErrorLog', 'U') IS NULL
BEGIN
    CREATE TABLE ETL_ErrorLog (
        LogID INT IDENTITY(1,1) PRIMARY KEY,       -- Auto-incrementing ID for each log entry
        JobName NVARCHAR(100),                     -- Name of the ETL job
        TargetMonth DATE,                          -- The data month the job processes
        LogTime DATETIME DEFAULT GETDATE(),        -- When the log was created
        Message NVARCHAR(4000),                    -- Details about the log or error
        Status NVARCHAR(50)                        -- Status, e.g., 'Success', 'Failed'
    );
END;

--  Declare variables for job execution
DECLARE @TargetDate DATE;
DECLARE @TargetYYYYMM CHAR(6);
DECLARE @PreviousMonth DATE;
DECLARE @InputTable NVARCHAR(100);
DECLARE @OutputTable NVARCHAR(100);
DECLARE @BaseInterestRate DECIMAL(5, 2);
DECLARE @EffectiveRateDate DATE;
DECLARE @SQL NVARCHAR(MAX);
DECLARE @JobName NVARCHAR(100);
DECLARE @Status NVARCHAR(50);
DECLARE @Message NVARCHAR(MAX);
DECLARE @Recipients NVARCHAR(MAX);
DECLARE @RowCount INT;

-- Need set up SSMS mail profil before it
--  Assign values to the variables
SET @TargetDate = '2025-05-01';
SET @TargetYYYYMM = FORMAT(@TargetDate, 'yyyyMM');
SET @PreviousMonth = DATEADD(MONTH, -1, @TargetDate);
SET @InputTable = 'CarInformation_' + @TargetYYYYMM;
SET @OutputTable = 'LoanProfitEstimates_' + @TargetYYYYMM;
SET @JobName = 'ETL_LoanProfitGeneration';
SET @Recipients = 'Mail@Mail.com'; -- Receiver Account
SET @RowCount = NULL;

--  Main ETL process
BEGIN TRY
    --  Ensure all required reference tables exist
    IF OBJECT_ID('CreditRiskTier', 'U') IS NULL
        THROW 51004, 'Missing dependency table: CreditRiskTier', 1;
    IF OBJECT_ID('EnergyClassMargin', 'U') IS NULL
        THROW 51005, 'Missing dependency table: EnergyClassMargin', 1;
    IF OBJECT_ID('DepreciationRates', 'U') IS NULL
        THROW 51006, 'Missing dependency table: DepreciationRates', 1;

    --  Check if the source data table exists
    IF OBJECT_ID(@InputTable, 'U') IS NULL
    BEGIN
        SET @Status = 'Failed';
        SET @Message = 'The CarInformation_' + @TargetYYYYMM + ' table for the current month is missing.';
        THROW 51000, @Message, 1;
    END;

    --  Try getting the current month's interest rate
    SELECT TOP 1 
        @BaseInterestRate = BaseInterestRate,
        @EffectiveRateDate = EffectiveDate
    FROM InterestRates
    WHERE EffectiveDate = @TargetDate;

    --  Fallback: Use previous month's rate if current is missing
    IF @BaseInterestRate IS NULL
    BEGIN
        SELECT TOP 1 
            @BaseInterestRate = BaseInterestRate,
            @EffectiveRateDate = EffectiveDate
        FROM InterestRates
        WHERE EffectiveDate = @PreviousMonth;

        IF @BaseInterestRate IS NULL
        BEGIN
            SET @Status = 'Failed';
            SET @Message = 'No base interest rate found for both current and previous month.';
            THROW 51001, @Message, 1;
        END
        ELSE
        BEGIN
            SET @Message = 'Base interest rate missing for current month; fallback to previous month (' 
                            + CONVERT(NVARCHAR(10), @PreviousMonth, 120) + ').';
        END
    END
    ELSE
    BEGIN
        SET @Message = 'ETL completed successfully using current month interest rate.';
    END;

    --  Dynamically build SQL to calculate and insert loan profit estimates
    SET @SQL = '
    IF OBJECT_ID(''' + @OutputTable + ''', ''U'') IS NOT NULL
        DROP TABLE ' + @OutputTable + ';

    SELECT 
        c.RecordID,
        c.CarModel,
        c.EnergyClass,
        c.ManufactureYear,
        c.BasePrice,
        c.CustomerRiskTier,
        CAST(' + CAST(@BaseInterestRate AS NVARCHAR) + ' 
            + ISNULL(r.RiskAdjustment, 0)
            + ISNULL(m.MarginRate, 0) AS DECIMAL(5,2)) AS FinalInterestRate,
        CAST((c.BasePrice * (1 + (
            ' + CAST(@BaseInterestRate AS NVARCHAR) + ' 
            + ISNULL(r.RiskAdjustment, 0)
            + ISNULL(m.MarginRate, 0)
        )/100)) / 36 AS DECIMAL(10,2)) AS EstimatedMonthlyPayment,
        CAST(c.BasePrice * ISNULL(d.DepreciationRate, 0) AS DECIMAL(10,2)) AS DepreciatedValue,
        CAST(((c.BasePrice * (1 + (
            ' + CAST(@BaseInterestRate AS NVARCHAR) + ' 
            + ISNULL(r.RiskAdjustment, 0)
            + ISNULL(m.MarginRate, 0)
        )/100)) - c.BasePrice AS DECIMAL(10,2)) AS EstimatedProfit,
        ''' + CONVERT(NVARCHAR(10), @TargetDate, 120) + ''' AS DataMonth,
        ' + CAST(@BaseInterestRate AS NVARCHAR) + ' AS BaseInterestRate,
        ''' + CONVERT(NVARCHAR(10), @EffectiveRateDate, 120) + ''' AS EffectiveRateDate
    INTO ' + @OutputTable + '
    FROM ' + @InputTable + ' c
    LEFT JOIN CreditRiskTier r ON c.CustomerRiskTier = r.RiskTier
    LEFT JOIN EnergyClassMargin m ON c.EnergyClass = m.EnergyClass
    LEFT JOIN DepreciationRates d 
        ON DATEDIFF(YEAR, c.ManufactureYear, YEAR(c.FileMonth)) BETWEEN d.MinYear AND d.MaxYear;
    ';

    EXEC sp_executesql @SQL;

    --  Confirm table creation and content
    IF OBJECT_ID(@OutputTable, 'U') IS NULL
    BEGIN
        SET @Status = 'Failed';
        SET @Message = 'Target table [' + @OutputTable + '] was not created.';
        THROW 51002, @Message, 1;
    END;

    SET @SQL = 'SELECT @cnt_out = COUNT(*) FROM ' + QUOTENAME(@OutputTable);
    EXEC sp_executesql @SQL, N'@cnt_out INT OUTPUT', @cnt_out = @RowCount OUTPUT;

    IF @RowCount = 0
    BEGIN
        SET @Status = 'Failed';
        SET @Message = 'Target table [' + @OutputTable + '] was created but contains 0 rows.';
        THROW 51003, @Message, 1;
    END;

    --  Mark as success
    SET @Status = 'Success';

END TRY
BEGIN CATCH
    --  Handle any runtime errors
    SET @Status = 'Failed';
    SET @Message = ISNULL(@Message + ' | ', '') + ERROR_MESSAGE();
END CATCH;

--  Log the result in ETL_ErrorLog table
INSERT INTO ETL_ErrorLog (JobName, TargetMonth, Message, Status)
VALUES (@JobName, @TargetDate, @Message, @Status);

--  Send email notification with job status and details
BEGIN TRY
    DECLARE @Subject NVARCHAR(200) = 
        'ETL Job Result: ' + @Status + ' | ' + FORMAT(@TargetDate, 'yyyy-MM');

    DECLARE @Body NVARCHAR(MAX) = 
        '<h3>SQL Server ETL Notification</h3>' +
        '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">' +
        '<tr><td><b>Job Name</b></td><td>' + @JobName + '</td></tr>' +
        '<tr><td><b>Target Month</b></td><td>' + FORMAT(@TargetDate, 'yyyy-MM') + '</td></tr>' +
        '<tr><td><b>Status</b></td><td>' + @Status + '</td></tr>' +
        '<tr><td><b>Execution Time</b></td><td>' + CONVERT(NVARCHAR, GETDATE(), 120) + '</td></tr>' +
        '<tr><td><b>Row Count</b></td><td>' + ISNULL(CAST(@RowCount AS NVARCHAR), 'N/A') + '</td></tr>' +
        '<tr><td><b>Details</b></td><td>' + @Message + '</td></tr>' +
        '</table>';

    EXEC msdb.dbo.sp_send_dbmail
        @profile_name = 'MyMailProfile',
        @recipients = @Recipients,
        @subject = @Subject,
        @body = @Body,
        @body_format = 'HTML';
END TRY
BEGIN CATCH
    PRINT 'Email sending failed: ' + ERROR_MESSAGE();
    INSERT INTO ETL_ErrorLog (JobName, TargetMonth, Message, Status)
    VALUES (@JobName, @TargetDate, 'Email failed to send: ' + ERROR_MESSAGE(), 'EmailFailed');
END CATCH;

--  Optional validation queries
SELECT * FROM [dbo].[LoanProfitEstimates_202505];
-- SELECT * FROM [dbo].[CarInformation_202505];
-- SELECT * FROM InterestRates;
-- SELECT * FROM [dbo].[DepreciationRates];
