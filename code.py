ALTER PROC [Gold].[usp_VTAG_ResourceOwnerMapping_Upsert]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @UTCNow DATETIME = GETUTCDATE();

    -- Cleanup temp if it exists
    IF OBJECT_ID('tempdb..#Compare_GoldOwner') IS NOT NULL
        DROP TABLE #Compare_GoldOwner;

    BEGIN TRY
        BEGIN TRAN;

        --------------------------------------------------------------------
        -- Only run if staging table has data
        --------------------------------------------------------------------
        IF EXISTS (SELECT 1 FROM [Silver].[VTAG_ResourceOwnerMapping_Staging])
        BEGIN
            PRINT 'Comparing incoming and existing Gold owner records...';

            ----------------------------------------------------------------
            -- 1. Build compare table  (incoming vs existing)
            ----------------------------------------------------------------
            SELECT
                -- business key (adjust to your real key!)
                s.ResourceId,
                -- if key is AccountId instead, replace both places

                -- incoming columns from staging
                s.Environment,
                s.Billing_Owner_AppSvcId,
                s.Support_Owner_AppSvcId,
                s.Billing_Owner_AppId,
                s.Support_Owner_AppId,
                s.Application_Name,
                s.Billing_Owner_Name,
                s.Support_Owner_Name,
                s.Business_Unit,
                s.Department,
                s.Management_Model,
                s.Is_Platform_Managed,
                s.Platform_Team_Name,
                s.Ownership_Determination_Method,
                s.Ownership_Confidence_Score,
                s.Is_Orphaned,
                s.Is_Deleted,
                s.Orphan_Reason,
                s.Hash_Key          AS New_HashKey,

                -- existing columns (may be NULL)
                g.ResourceId        AS Existing_ResourceId,
                g.Hash_Key          AS Existing_HashKey,
                g.Is_Deleted        AS Existing_IsDeleted
            INTO #Compare_GoldOwner
            FROM [Silver].[VTAG_ResourceOwnerMapping_Staging] s
            LEFT JOIN [Gold].[VTAG_ResourceOwnerMapping] g
                ON s.ResourceId = g.ResourceId;      -- <== adjust key here

            ----------------------------------------------------------------
            -- 2. Mark Deleted rows (exist in Gold but now flagged deleted)
            ----------------------------------------------------------------
            PRINT 'Deleted Gold owner records...';

            UPDATE g
            SET
                g.Is_Deleted      = 1,
                g.Is_Current      = 0,
                g.Change_Category = 'Deleted',
                g.Processing_Date = @UTCNow
            FROM [Gold].[VTAG_ResourceOwnerMapping] g
            INNER JOIN #Compare_GoldOwner c
                ON g.ResourceId = c.ResourceId
            WHERE c.Existing_ResourceId IS NOT NULL
                  AND c.New_HashKey IS NULL;  -- not present in staging anymore
            -- (If you prefer "soft delete when Is_Deleted=1 in staging"
            -- change the WHERE to use c.Is_Deleted = 1 instead.)

            ----------------------------------------------------------------
            -- 3. Verified rows (same hash = no change, just seen again)
            ----------------------------------------------------------------
            PRINT 'Verified Gold owner records...';

            UPDATE g
            SET
                g.Last_Verified_Date = @UTCNow,
                g.Processing_Date    = @UTCNow,
                g.Change_Category    = 'Verified'
            FROM [Gold].[VTAG_ResourceOwnerMapping] g
            INNER JOIN #Compare_GoldOwner c
                ON g.ResourceId = c.ResourceId
            WHERE c.Existing_HashKey = c.New_HashKey
                  AND c.New_HashKey IS NOT NULL
                  AND g.Is_Deleted = 0;

            ----------------------------------------------------------------
            -- 4. Changed rows (same key, different hash)
            ----------------------------------------------------------------
            PRINT 'Upsert (update) changed Gold owner records...';

            UPDATE g
            SET
                g.Environment                     = c.Environment,
                g.Billing_Owner_AppSvcId          = c.Billing_Owner_AppSvcId,
                g.Support_Owner_AppSvcId          = c.Support_Owner_AppSvcId,
                g.Billing_Owner_AppId             = c.Billing_Owner_AppId,
                g.Support_Owner_AppId             = c.Support_Owner_AppId,
                g.Application_Name                = c.Application_Name,
                g.Billing_Owner_Name              = c.Billing_Owner_Name,
                g.Support_Owner_Name              = c.Support_Owner_Name,
                g.Business_Unit                   = c.Business_Unit,
                g.Department                      = c.Department,
                g.Management_Model                = c.Management_Model,
                g.Is_Platform_Managed             = c.Is_Platform_Managed,
                g.Platform_Team_Name              = c.Platform_Team_Name,
                g.Ownership_Determination_Method  = c.Ownership_Determination_Method,
                g.Ownership_Confidence_Score      = c.Ownership_Confidence_Score,
                g.Is_Orphaned                     = c.Is_Orphaned,
                g.Is_Deleted                      = c.Is_Deleted,
                g.Orphan_Reason                   = c.Orphan_Reason,
                g.Hash_Key                        = c.New_HashKey,
                g.Change_Category                 = 'Updated',
                g.Last_Modified_Date              = @UTCNow,
                g.Processing_Date                 = @UTCNow,
                g.Is_Current                      = 1
            FROM [Gold].[VTAG_ResourceOwnerMapping] g
            INNER JOIN #Compare_GoldOwner c
                ON g.ResourceId = c.ResourceId
            WHERE c.Existing_HashKey IS NOT NULL
                  AND c.New_HashKey IS NOT NULL
                  AND c.Existing_HashKey <> c.New_HashKey
                  AND g.Is_Deleted = 0;

            ----------------------------------------------------------------
            -- 5. Insert new rows (in staging, not in existing)
            ----------------------------------------------------------------
            PRINT 'Insert new Gold owner records...';

            INSERT INTO [Gold].[VTAG_ResourceOwnerMapping] (
                ResourceId,
                Environment,
                Billing_Owner_AppSvcId,
                Support_Owner_AppSvcId,
                Billing_Owner_AppId,
                Support_Owner_AppId,
                Application_Name,
                Billing_Owner_Name,
                Support_Owner_Name,
                Business_Unit,
                Department,
                Management_Model,
                Is_Platform_Managed,
                Platform_Team_Name,
                Ownership_Determination_Method,
                Ownership_Confidence_Score,
                Is_Orphaned,
                Is_Deleted,
                Orphan_Reason,
                Hash_Key,
                Change_Category,
                Created_Date,
                Last_Verified_Date,
                Last_Modified_Date,
                Processing_Date,
                Is_Current
            )
            SELECT
                c.ResourceId,
                c.Environment,
                c.Billing_Owner_AppSvcId,
                c.Support_Owner_AppSvcId,
                c.Billing_Owner_AppId,
                c.Support_Owner_AppId,
                c.Application_Name,
                c.Billing_Owner_Name,
                c.Support_Owner_Name,
                c.Business_Unit,
                c.Department,
                c.Management_Model,
                c.Is_Platform_Managed,
                c.Platform_Team_Name,
                c.Ownership_Determination_Method,
                c.Ownership_Confidence_Score,
                c.Is_Orphaned,
                c.Is_Deleted,
                c.Orphan_Reason,
                c.New_HashKey,
                'New'   AS Change_Category,
                @UTCNow AS Created_Date,
                @UTCNow AS Last_Verified_Date,
                @UTCNow AS Last_Modified_Date,
                @UTCNow AS Processing_Date,
                1       AS Is_Current
            FROM #Compare_GoldOwner c
            WHERE c.Existing_ResourceId IS NULL;

            ----------------------------------------------------------------
            -- 6. Cleanup staging
            ----------------------------------------------------------------
            TRUNCATE TABLE [Silver].[VTAG_ResourceOwnerMapping_Staging];
        END;  -- IF staging has rows

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        -- optional: log error info here (same style as your other procs)
        DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
        SELECT
            @ErrMsg = ERROR_MESSAGE(),
            @ErrSeverity = ERROR_SEVERITY();
        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH;
END;
GO
