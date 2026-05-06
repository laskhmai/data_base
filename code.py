def process_subscription(account_name: str, 
                         virtual_tags_df: pd.DataFrame, 
                         snow_df: pd.DataFrame):
    print(f"\n=== Processing subscription: {account_name} ===")
    
    # 1) load sources for this subscription
    resources_df = load_sources(account_name)
    
    if resources_df.empty:
        print("No resources for this subscription Skipping")
        return
    
    # ✅ FIX 1: Filter virtual_tags to only this subscription's resources
    # This prevents 810K row merge causing OOM
    sub_resource_ids = set(
        resources_df["ResourceId"].astype(str).str.lower()
    )
    vt_filtered = virtual_tags_df[
        virtual_tags_df["resource_id_key"].isin(sub_resource_ids)
    ].copy()
    print(f"Filtered virtual_tags: {len(vt_filtered)} rows "
          f"(from {len(virtual_tags_df)} total)")
    
    # 2) load previous hashes and filter delta
    prev_hash_df = load_previous_hashes(account_name)
    delta_df = filter_changed_resources(resources_df, prev_hash_df)
    
    # 3) Transform only the delta
    print(f"Transforming {len(delta_df)} resources...")
    if delta_df is None or delta_df.empty:
        print("Delta returned no rows Skipping transformation")
        return
    
    # ✅ Pass filtered vt_filtered instead of full virtual_tags_df
    gold_df = transform(delta_df, vt_filtered, snow_df)