def main():
    subscriptions = get_subscriptions()
    print(f"Found {len(subscriptions)} subscriptions")
    
    virtual_tags_df, snow_df = load_child_tables()
    print("Completed loading records")
    
    # ✅ TEST ONE SUBSCRIPTION ONLY
    # Change this to any subscription
    # you want to test
    test_subscription = "az3-udap-prd"
    
    process_subscription(
        test_subscription,
        virtual_tags_df,
        snow_df
    )

main()