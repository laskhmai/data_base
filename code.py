INSERT INTO dbo.Daily_RunTables (SchemaName, TableName, DateColumn, ExpectedTime, GraceMinutes)
VALUES ('MongoDB', 'process', 'updated_date', '15:00', 30);

-- ============================================================
-- Step 1: Add MongoDB.process to the config table
-- (adjust DateColumn, ExpectedTime, GraceMinutes as needed)
-- ============================================================
INSERT INTO dbo.Daily_RunTables (SchemaName, TableName, DateColumn, ExpectedTime, GraceMinutes)
VALUES ('MongoDB', 'process', 'updated_date', '05:00', 30);
-- ^ Replace 'updated_date' with actual date column name in MongoDB.process
-- ^ Adjust ExpectedTime and GraceMinutes to match your pipeline schedule
GO

-- ============================================================
-- Step 2: Corrected stored procedure
-- ============================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROC [dbo].[usp_Check_Daily_Table_Updates] AS
BEGIN
    SET NOCOUNT ON;

    -- Capture "now" once so the whole run is consistent
    DECLARE @Now   datetime2(0) = SYSDATETIME();
    DECLARE @Today date         = CAST(@Now AS date);

    /*
       Build temp config with all needed columns via CTAS (Synapse-friendly)
       Rolling-boundary logic:
         - If now < (today @ ExpectedTime + Grace) -> cutoff = (yesterday @ ExpectedTime - Grace)
         - Else                                    -> cutoff = (today   @ ExpectedTime - Grace)
    */
    IF OBJECT_ID('tempdb..#cfg') IS NOT NULL DROP TABLE #cfg;

    CREATE TABLE #cfg
    WITH (DISTRIBUTION = ROUND_ROBIN)
    AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL))          AS rn,
        drt.[SchemaName],
        drt.[TableName],
        drt.[DateColumn],
        CAST(drt.[ExpectedTime] AS time(0))                 AS ExpectedTime,
        drt.[GraceMinutes],

        /* NextExpected = today at ExpectedTime */
        DATEADD(
            SECOND,
            DATEDIFF(SECOND,
                CAST('00:00:00' AS time),
                CAST(drt.[ExpectedTime] AS time(0))),
            CAST(@Today AS datetime2(0))
        )                                                   AS NextExpected,

        /* PrevExpected = yesterday at ExpectedTime */
        DATEADD(
            SECOND,
            DATEDIFF(SECOND,
                CAST('00:00:00' AS time),
                CAST(drt.[ExpectedTime] AS time(0))),
            DATEADD(DAY, -1, CAST(@Today AS datetime2(0)))
        )                                                   AS PrevExpected,

        /* Rolling cutoff (ExpectedFrom) */
        CASE
            WHEN @Now < DATEADD(MINUTE, drt.[GraceMinutes],
                            DATEADD(SECOND,
                                DATEDIFF(SECOND,
                                    CAST('00:00:00' AS time),
                                    CAST(drt.[ExpectedTime] AS time(0))),
                                CAST(@Today AS datetime2(0))))
            THEN DATEADD(MINUTE, -drt.[GraceMinutes],
                    DATEADD(SECOND,
                        DATEDIFF(SECOND,
                            CAST('00:00:00' AS time),
                            CAST(drt.[ExpectedTime] AS time(0))),
                        DATEADD(DAY, -1, CAST(@Today AS datetime2(0)))))
            ELSE DATEADD(MINUTE, -drt.[GraceMinutes],
                    DATEADD(SECOND,
                        DATEDIFF(SECOND,
                            CAST('00:00:00' AS time),
                            CAST(drt.[ExpectedTime] AS time(0))),
                        CAST(@Today AS datetime2(0))))
        END                                                 AS ExpectedFrom

    FROM dbo.Daily_RunTables AS drt;

    /* Loop over config rows without CURSOR (Synapse limitation) */
    DECLARE @i int = 1;
    DECLARE @n int = (SELECT COUNT(*) FROM #cfg);

    /* Per-row vars — declared ONCE outside the loop */
    DECLARE
        @SchemaName     sysname,
        @TableName      sysname,
        @DateColumn     sysname,
        @ExpectedFrom   datetime2(0),
        @Sql            nvarchar(max),
        @MaxDt          datetime2(0),
        @Status         varchar(20),
        @Details        varchar(500),
        @CheckedAt      datetime2(0),
        @LastUpd        varchar(19),   -- moved outside loop (avoids Synapse re-declare issue)
        @ErrMsg         varchar(500);  -- moved outside loop

    WHILE @i <= @n
    BEGIN
        SELECT
            @SchemaName   = c.[SchemaName],
            @TableName    = c.[TableName],
            @DateColumn   = c.[DateColumn],
            @ExpectedFrom = c.[ExpectedFrom]
        FROM #cfg AS c
        WHERE c.rn = @i;

        BEGIN TRY
            /* Dynamic scan: MAX(date) from target table/column */
            SET @Sql = N'
                SELECT @MaxDtOUT = MAX(TRY_CAST('
                    + QUOTENAME(@DateColumn) + N' AS datetime2(0)))
                FROM '
                    + QUOTENAME(@SchemaName) + N'.'
                    + QUOTENAME(@TableName)  + N';';

            SET @MaxDt = NULL;

            EXEC sp_executesql
                @Sql,
                N'@MaxDtOUT datetime2(0) OUTPUT',
                @MaxDtOUT = @MaxDt OUTPUT;

            /* -------------------------------------------------------
               STATUS LOGIC
               FIX: For tables that store dates as midnight (00:00:00),
               comparing @MaxDt >= @ExpectedFrom always fails because
               midnight < expected time (e.g. 04:00).
               Fix: compare DATE portions only.
            ------------------------------------------------------- */
            IF @MaxDt IS NULL
            BEGIN
                SET @Status  = 'NO_DATA';
                SET @Details = 'No rows / date null';
            END
            ELSE IF CAST(@MaxDt AS date) >= CAST(@ExpectedFrom AS date)
            -- ^^^ KEY FIX: was (@MaxDt >= @ExpectedFrom)
            -- This now correctly handles date-only columns stored as midnight
            BEGIN
                SET @Status  = 'SUCCESS';
                SET @Details = 'OK';
            END
            ELSE
            BEGIN
                SET @Status  = 'NOT_RUN';
                SET @LastUpd = CONVERT(varchar(19), @MaxDt, 120);
                SET @Details = 'LastUpdate=' + @LastUpd;
            END

            SET @CheckedAt = SYSDATETIME();

            INSERT INTO dbo.daily_update
            (
                CheckedAt, SchemaName, TableName, DateColumn,
                ExpectedFrom, MaxDateValue, Status, Details
            )
            VALUES
            (
                @CheckedAt,
                @SchemaName, @TableName, @DateColumn,
                @ExpectedFrom, @MaxDt, @Status, @Details
            );

        END TRY
        BEGIN CATCH
            /* Capture error in variable first (Synapse rule) */
            SET @ErrMsg    = LEFT(ERROR_MESSAGE(), 500);
            SET @CheckedAt = SYSDATETIME();

            INSERT INTO dbo.daily_update
            (
                CheckedAt, SchemaName, TableName, DateColumn,
                ExpectedFrom, MaxDateValue, Status, Details
            )
            VALUES
            (
                @CheckedAt,
                @SchemaName, @TableName, @DateColumn,
                @ExpectedFrom, NULL, 'ERROR', @ErrMsg
            );
        END CATCH;

        SET @i += 1;
    END

    /* Convenience result: last 2 hours of checks */
    SELECT *
    FROM dbo.daily_update
    WHERE CheckedAt >= DATEADD(HOUR, -2, SYSDATETIME())
    ORDER BY CheckedAt DESC, Status, SchemaName, TableName;

END;
GO

-- Run it
EXEC [dbo].[usp_Check_Daily_Table_Updates];