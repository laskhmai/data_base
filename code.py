SELECT v.resource_id,
       v.resource_type,
       v.change_category
FROM [Gold].[VTag_Azure_InferredTags] v
WHERE v.subscription_name = 'az3-bpic-npe'
AND v.resource_id NOT IN (
    SELECT LOWER(CAST([ResourceId] AS NVARCHAR(450)))
    FROM [Silver].[AzureResourcesNormalized]
    WHERE [AccountName] = 'az3-bpic-npe'
)