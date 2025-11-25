SELECT 
    f.resource_id,
    f.final_app_service_id,
    f.billing_owner_name,
    f.billing_owner_email,
    f.ownership_determination_method,
    f.ownership_confidence_score,
    f.orphan_reason,
    s.EapmId,
    s.AppOwner,
    s.AppOwnerEmail
FROM [AZURE].[Final_Gold_Resources] f
LEFT JOIN [Silver].[SnowNormalizedStaging] s
    ON TRY_CONVERT(int, f.final_app_service_id) = s.EapmId
WHERE f.is_orphaned = 1
  AND f.ownership_determination_method = 'APM via EAPM ID';



SELECT 
    f.resource_id,
    f.final_app_service_id,
    f.billing_owner_name,
    f.billing_owner_email,
    f.ownership_determination_method,
    f.ownership_confidence_score,
    f.orphan_reason,
    s.EapmId
FROM [AZURE].[Final_Gold_Resources] f
LEFT JOIN [Silver].[SnowNormalizedStaging] s
    ON TRY_CONVERT(int, f.final_app_service_id) = s.EapmId
WHERE f.is_orphaned = 1
  AND f.ownership_determination_method = 'APM via Naming Pattern'
  AND f.orphan_reason = 'invalid_eapm_id';



SELECT *
FROM [AZURE].[Final_Gold_Resources]
WHERE is_orphaned = 1
  AND ownership_determination_method = 'APM via Resource Tag ID';

