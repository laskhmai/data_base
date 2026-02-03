CREATE OR ALTER PROCEDURE dbo.usp_Check_Daily_Table_Updates
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(0) = SYSDATETIME();
    DECLARE @Today date = CAST(@Now AS date);

    -- Variables for config rows
    DECLARE
        @SchemaName sysname,
        @TableName  sysname,
        @DateColumn sysname,
        @ExpectedTime time(0),
        @GraceMinutes int;

    -- Variables used per table
    DECLARE
        @ExpectedToday datetime2(0),
        @ExpectedFrom  datetime2(0),
        @Sql nvarchar(max);

    -- Cursor over your config table
    DECLARE cur CURSOR FOR
    SELECT SchemaName, TableName, DateColumn, ExpectedTime, GraceMinutes
    FROM dbo.Daily_RunTables;

    OPEN cur;
    FETCH NEXT FROM cur INTO @SchemaName, @TableName, @DateColumn, @ExpectedTime, @GraceMinutes;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            /* 1) Build the expected cutoff time (ExpectedFrom)
                  - If now is before today's expected time -> compare against yesterday expected time
                  - Else compare against today's expected time
                  - Then subtract GraceMinutes
            */
            SET @ExpectedToday = CAST(@Today AS datetime2(0)) + CAST(@ExpectedTime AS datetime2(0));

            SET @ExpectedFrom =
                CASE
                    WHEN @Now < @ExpectedToday THEN DATEADD(DAY, -1, @ExpectedToday)
                    ELSE @ExpectedToday
                END;

            SET @ExpectedFrom = DATEADD(MINUTE, -@GraceMinutes, @ExpectedFrom);

            /* 2) Dynamic SQL: compute MaxDateValue and insert result */
            SET @Sql = N'
                DECLARE @MaxDt datetime2(0);

                SELECT @MaxDt = MAX(TRY_CONVERT(datetime2(0), ' + QUOTENAME(@DateColumn) + N'))
                FROM ' + QUOTENAME(@SchemaName) + N'.' + QUOTENAME(@TableName) + N';

                INSERT INTO dbo.daily_update
                (
                    CheckedAt, SchemaName, TableName, DateColumn,
                    ExpectedFrom, MaxDateValue, Status, Details
                )
                VALUES
                (
                    SYSDATETIME(),
                    @pSchema, @pTable, @pCol,
                    @pExpectedFrom, @MaxDt,
                    CASE
                        WHEN @MaxDt IS NULL THEN ''NO_DATA''
                        WHEN @MaxDt >= @pExpectedFrom THEN ''SUCCESS''
                        ELSE ''NOT_RUN''
                    END,
                    CASE
                        WHEN @MaxDt IS NULL THEN ''No rows / date null''
                        WHEN @MaxDt >= @pExpectedFrom THEN ''OK''
                        ELSE CONCAT(''LastUpdate='', CONVERT(varchar(19), @MaxDt, 120))
                    END
                );
            ';

            EXEC sp_executesql
                @Sql,
                N'@pSchema sysname, @pTable sysname, @pCol sysname, @pExpectedFrom datetime2(0)',
                @pSchema=@SchemaName, @pTable=@TableName, @pCol=@DateColumn, @pExpectedFrom=@ExpectedFrom;

        END TRY
        BEGIN CATCH
            -- Log error but continue
            INSERT INTO dbo.daily_update
            (
                CheckedAt, SchemaName, TableName, DateColumn,
                ExpectedFrom, MaxDateValue, Status, Details
            )
            VALUES
            (
                SYSDATETIME(),
                @SchemaName, @TableName, @DateColumn,
                @ExpectedFrom, NULL,
                'ERROR', LEFT(ERROR_MESSAGE(), 500)
            );
        END CATCH;

        FETCH NEXT FROM cur INTO @SchemaName, @TableName, @DateColumn, @ExpectedTime, @GraceMinutes;
    END

    CLOSE cur;
    DEALLOCATE cur;

    -- Return latest run results (last 2 hours) for quick view
    SELECT *
    FROM dbo.daily_update
    WHERE CheckedAt >= DATEADD(HOUR, -2, SYSDATETIME())
    ORDER BY CheckedAt DESC, Status, SchemaName, TableName;
END;
GO
