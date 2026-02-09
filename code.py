SELECT
    ResourceId,
    CASE 
        WHEN ResTags LIKE '%parking%' THEN 'ResTags'
        WHEN RgTags LIKE '%parking%' THEN 'RgTags'
        WHEN Properties LIKE '%parking%' THEN 'Properties'
        ELSE 'Not Found'
    END AS ParkingFoundIn,
    ResTags,
    RgTags,
    Properties
FROM silver.AzureResourcesNormalized
WHERE
    ResTags LIKE '%parking%'
 OR RgTags LIKE '%parking%'
 OR Properties LIKE '%parking%';