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
