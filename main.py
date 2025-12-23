from emoji_oracle_analytics.app import main


if __name__ == "__main__":
    main()


<<<<<<< Updated upstream
=======
    context = {
        "client": client,
        "log_path": settings.LOG_PATH,
        "data_dir": settings.DATA_DIR,
        'csv_dir': settings.CSV_DIR,
        "dataset": settings.DATASET,
        'start_date': settings.START_DATE,
        'report_path': settings.REPORT_PATH,
        'country': settings.COUNTRY,
        'not_user': settings.NOT_USER,
        'version_filter': settings.VERSION_FILTER
    }
    

    # pull data and run through pipeline
    df = run_pipeline(df=df, context=context)
    logger.info("Data pipeline executed successfully.")
    

    logger.info("Generating dataframes...")
    dfs = create_dataframes(df=df)
    logger.info("Dataframes generated successfully.")

    logger.info("Calculating KPIs...")

    kpis = calculate_kpis(df=df, dict=dfs)
    
#    sliced_data = df[df['user_pseudo_id'] == 'a6bdeeb9060751b4b3a2c29d71b5e049'].copy()

#    sliced_data.to_csv(os.path.join(settings.CSV_DIR, "sliced_data.csv"), index=False)

    df.to_csv(os.path.join(settings.CSV_DIR, "processed_data.csv"), index=False)

    for name, dataframe in dfs.items():
        dataframe.to_csv(os.path.join(settings.CSV_DIR, f"{name}_data.csv"), index=False)
    
    logger.info("Data pipeline complete. Processed data saved.")
    generate_report(df=df, dfs_dict = dfs, kpis = kpis, context = context)


>>>>>>> Stashed changes
