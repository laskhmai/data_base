if __name__ == "__main__":
    # Previous complete month (Postgres pattern)
    # Running June 23rd → uses May 2026 full month
    today           = datetime.now()
    last_month_date = today - relativedelta(months=1)

    months      = [last_month_date.strftime("%Y-%m")]
    start_dates = [last_month_date.replace(day=1)
                   .strftime("%Y-%m-%d")]
    end_dates   = [((last_month_date.replace(day=1)
                   + relativedelta(months=1))
                   - timedelta(days=1)).strftime("%Y-%m-%d")]

    print("months =", months)
    print("StartDate =", start_dates[0])
    print("EndDate =", end_dates[0])