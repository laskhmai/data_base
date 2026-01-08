# Add ownership columns (DO NOT overwrite existing values)
final_df["billing_owner_appsvcid"] = (
    final_df["billing_owner_appsvcid"]
    .combine_first(final_df["final_app_service_id"])
)

final_df["support_owner_appsvcid"] = (
    final_df["support_owner_appsvcid"]
    .combine_first(final_df["final_app_service_id"])
)
