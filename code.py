ALTER PROC [Silver].[usp_CloudabilityAggregate_DailySpend]
    @UsageDate DATE = NULL  -- Add this line
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Change this:
    -- DECLARE @UsageDate DATE = DATEADD(DAY,-3,CAST(GETDATE() AS DATE))
    
    -- To this:
    IF @UsageDate IS NULL
        SET @UsageDate = 
            DATEADD(DAY,-3,CAST(GETDATE() AS DATE))