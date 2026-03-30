SELECT 
    REPLACE(REPLACE(CAST([properties] AS NVARCHAR(MAX)), CHAR(13), ' '), CHAR(10), ' ') AS properties_clean
FROM [Gold].[AzureActiveResourceOwnerShipNormalized]