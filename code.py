def process_subscription(account_name: str,
                         virtual_tags_df: pd.DataFrame,
                         snow_df: pd.DataFrame):
    print(f"\n=== Processing subscription: {account_name} ===")
    
    resources_df = load_sources(account_name)
    if resources_df.empty:
        print("No resources for this subscription Skipping")
        return

    # Filter virtual_tags to this subscription only
    # resources_df → "ResourceId" (CamelCase from Silver table)
    # virtual_tags_df → "resource_id" (lowercase from VTag table)
    # Both lowercased to match — same as transform() does internally
    sub_resource_ids = set(
        resources_df["ResourceId"]
        .astype(str)
        .str.lower()
    )
    vt_filtered = virtual_tags_df[
        virtual_tags_df["resource_id"]
        .astype(str)
        .str.lower()
        .isin(sub_resource_ids)
    ].copy()

    print(f"Filtered virtual_tags: {len(vt_filtered)} rows "
          f"(from {len(virtual_tags_df)} total)")

    prev_hash_df = load_previous_hashes(account_name)
    delta_df = filter_changed_resources(resources_df, prev_hash_df)

    print(f"Transforming {len(delta_df)} resources...")
    if delta_df is None or delta_df.empty:
        print("Delta returned no rows Skipping transformation")
        return

    gold_df = transform(delta_df, vt_filtered, snow_df)