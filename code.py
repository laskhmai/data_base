CREATE TABLE dbo.Table_Freshness_Config
(
    Id            int IDENTITY(1,1) PRIMARY KEY,
    SchemaName    sysname NOT NULL,
    TableName     sysname NOT NULL,
    DateColumn    sysname NOT NULL,

    ExpectedTime  time(0) NOT NULL,      -- e.g. '06:00' or '18:00'
    GraceMinutes  int NOT NULL DEFAULT 60, -- allow delay
    IsActive      bit NOT NULL DEFAULT 1
);
CREATE TABLE dbo.Table_Freshness_Result
(
    RunId        uniqueidentifier NOT NULL,
    CheckedAt    datetime2(0) NOT NULL DEFAULT SYSDATETIME(),
    SchemaName   sysname NOT NULL,
    TableName    sysname NOT NULL,
    DateColumn   sysname NOT NULL,

    ExpectedFrom datetime2(0) NULL,   -- the cutoff time we expect
    MaxDateValue datetime2(0) NULL,   -- actual max date in table

    Status       varchar(20) NOT NULL, -- SUCCESS / NOT_RUN / NO_DATA / ERROR
    Details      varchar(500) NULL
);
CREATE OR ALTER PROCEDURE dbo.usp_Check_Table_Freshness
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunId uniqueidentifier = NEWID();
    DECLARE @Now datetime2(0) = SYSDATETIME();
    DECLARE @Today date = CAST(@Now AS date);

    DECLARE
        @Schema sysname,
        @Table  sysname,
        @Col    sysname,
        @ExpectedTime time(0),
        @Grace int,
        @sql nvarchar(max);

    DECLARE cur CURSOR FAST_FORWARD FOR
    SELECT SchemaName, TableName, DateColumn, ExpectedTime, GraceMinutes
    FROM dbo.Table_Freshness_Config
    WHERE IsActive = 1;

    OPEN cur;
    FETCH NEXT FROM cur INTO @Schema, @Table, @Col, @ExpectedTime, @Grace;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Build expected cutoff datetime:
            -- If current time is before today's expected time => compare against yesterday expected time
            DECLARE @ExpectedToday datetime2(0) =
                CAST(@Today AS datetime2(0)) + CAST(@ExpectedTime AS datetime2(0));

            DECLARE @ExpectedFrom datetime2(0) =
                CASE WHEN @Now < @ExpectedToday
                     THEN DATEADD(DAY, -1, @ExpectedToday)
                     ELSE @ExpectedToday
                END;

            -- Apply grace (allow late run)
            SET @ExpectedFrom = DATEADD(MINUTE, -@Grace, @ExpectedFrom);

            -- Dynamic SQL to get MAX(dateColumn)
            SET @sql = N'
                DECLARE @maxdt datetime2(0);

                SELECT @maxdt = MAX(TRY_CONVERT(datetime2(0), ' + QUOTENAME(@Col) + N'))
                FROM ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table) + N';

                INSERT INTO dbo.Table_Freshness_Result
                (RunId, SchemaName, TableName, DateColumn, ExpectedFrom, MaxDateValue, Status, Details)
                VALUES
                (
                    @RunId, @SchemaName, @TableName, @DateColumn, @ExpectedFrom, @maxdt,
                    CASE
                        WHEN @maxdt IS NULL THEN ''NO_DATA''
                        WHEN @maxdt >= @ExpectedFrom THEN ''SUCCESS''
                        ELSE ''NOT_RUN''
                    END,
                    CASE
                        WHEN @maxdt IS NULL THEN ''No rows / date null''
                        WHEN @maxdt >= @ExpectedFrom THEN ''OK''
                        ELSE CONCAT(''LastUpdate='', CONVERT(varchar(19), @maxdt, 120))
                    END
                );';

            EXEC sp_executesql
                @sql,
                N'@RunId uniqueidentifier, @SchemaName sysname, @TableName sysname, @DateColumn sysname, @ExpectedFrom datetime2(0)',
                @RunId=@RunId, @SchemaName=@Schema, @TableName=@Table, @DateColumn=@Col, @ExpectedFrom=@ExpectedFrom;

        END TRY
        BEGIN CATCH
            INSERT INTO dbo.Table_Freshness_Result
            (RunId, SchemaName, TableName, DateColumn, ExpectedFrom, MaxDateValue, Status, Details)
            VALUES
            (@RunId, @Schema, @Table, @Col, NULL, NULL, 'ERROR', LEFT(ERROR_MESSAGE(), 500));
        END CATCH;

        FETCH NEXT FROM cur INTO @Schema, @Table, @Col, @ExpectedTime, @Grace;
    END

    CLOSE cur;
    DEALLOCATE cur;

    -- Show summary for this run
    SELECT *
    FROM dbo.Table_Freshness_Result
    WHERE RunId = @RunId
    ORDER BY
        CASE Status WHEN 'ERROR' THEN 1 WHEN 'NOT_RUN' THEN 2 WHEN 'NO_DATA' THEN 3 ELSE 4 END,
        SchemaName, TableName;
END
GO
