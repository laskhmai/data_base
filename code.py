# Add ownership columns (DO NOT overwrite existing values)
final_df["billing_owner_appsvcid"] = (
    final_df["billing_owner_appsvcid"]
    .combine_first(final_df["final_app_service_id"])
)

final_df["support_owner_appsvcid"] = (
    final_df["support_owner_appsvcid"]
    .combine_first(final_df["final_app_service_id"])
)
# ===== OPTION A: Ensure appsvcid columns always exist =====
for col in ["billing_owner_appsvcid", "support_owner_appsvcid"]:
    if col not in final_df.columns:
        final_df[col] = None
# ✅ Combine both sets (keep ALL columns; do NOT use intersection)
final_df = pd.concat([non_orphaned_join, orphan_join], ignore_index=True, sort=False)

# ✅ Ensure columns exist (in case one side didn't have them)
for col in ["billing_owner_appsvcid", "support_owner_appsvcid", "final_app_service_id"]:
    if col not in final_df.columns:
        final_df[col] = pd.NA
def dbg(df, rid, label):
    x = df[df["resource_id"] == rid].copy() if ("resource_id" in df.columns) else df.iloc[0:0]
    print(f"\n--- {label} --- rows={len(x)}")
    if len(x) == 0:
        return
    cols = [
        "resource_id",
        "OwnerSource",
        "isOrphaned",
        "final_app_service_id",
        "billing_owner_appsvcid",
        "support_owner_appsvcid",
        "AppID", "app_id",
        "AppName", "application_name",
        "appserviceid_key", "appservice_key"
    ]
    cols = [c for c in cols if c in x.columns]
    print(x[cols].head(10).to_string(index=False))
DEBUG_RID = "/subscriptions/....."   # paste your one bad resource_id here
dbg(resources_df, DEBUG_RID, "resources_df (input to transform)")
dbg(non_orphaned_join, DEBUG_RID, "non_orphaned_join (after snow join)")
dbg(orphan_tags, DEBUG_RID, "orphan_tags (after pick_orphaned_appsvc)")
dbg(orphan_join, DEBUG_RID, "orphan_join (after snow join)")
dbg(final_df, DEBUG_RID, "final_df (after concat)")
print("\nKey samples:")
if "final_app_service_id" in orphan_tags.columns:
    print("orphan_tags final_app_service_id sample:", orphan_tags["final_app_service_id"].dropna().astype(str).head(5).tolist())
if "appservice_key" in snow_df.columns:
    print("snow appservice_key sample:", snow_df["appservice_key"].dropna().astype(str).head(5).tolist())
