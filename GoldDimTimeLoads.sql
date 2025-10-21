USE SpotifyDataBase;
GO

ALTER   PROCEDURE [dbo].[usp_silver_to_gold_dim_time_load]
    @yearsAhead INT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @lastDate DATE; 
    DECLARE @targetEndDate DATE;

    SELECT @lastDate = MAX(FullDate)
    FROM gold.dim_time;

    IF @lastDate IS NULL
        SET @lastDate = '1950-01-01';

    SET @targetEndDate = DATEADD(YEAR, @yearsAhead,@lastDate);

    IF @targetEndDate >= CAST(GETDATE() AS DATE)
        BEGIN
            PRINT 'No new dates to add to dim_time.';
            RETURN;
        END;
	ELSE
		BEGIN
			PRINT CONCAT('Generating time dimension since ', 
						 CONVERT(VARCHAR(10), DATEADD(DAY, 1, @lastDate), 120),
						 ' to ', CONVERT(VARCHAR(10), @targetEndDate, 120));

			;WITH DateSeries AS (
				SELECT DATEADD(DAY, 1, @lastDate) AS FullDate
				UNION ALL
				SELECT DATEADD(DAY, 1, FullDate)
				FROM DateSeries
				WHERE FullDate <= @targetEndDate
			)
			INSERT INTO gold.dim_time (
				DateID,
				FullDate,
				Year,
				MONTH,
				MonthName,
				Day,
				DayOfWeek,
				DayName
			)
			SELECT
				CAST(CONVERT(CHAR(8), FullDate, 112) AS INT) AS DateID,
				FullDate,
				YEAR(FullDate) AS Year,
				MONTH(FullDate) AS Month,
				DATENAME(MONTH, FullDate) AS MonthName,
				DAY(FullDate) AS Day,
				DATEPART(WEEKDAY, FullDate) AS DayOfWeek,
				DATENAME(WEEKDAY, FullDate) AS DayName
			FROM DateSeries
			OPTION (MAXRECURSION 32767);
		END
END;
    SELECT
        CAST(CONVERT(CHAR(8), FullDate, 112) AS INT) AS DateID,
        FullDate,
        YEAR(FullDate) AS Year,
        MONTH(FullDate) AS Month,
        DATENAME(MONTH, FullDate) AS MonthName,
        DAY(FullDate) AS Day,
        DATEPART(WEEKDAY, FullDate) AS DayOfWeek,
        DATENAME(WEEKDAY, FullDate) AS DayName
    FROM DateSeries
    OPTION (MAXRECURSION 32767);
END;
GO