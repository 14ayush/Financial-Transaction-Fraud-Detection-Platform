import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="incremental",
        unique_key="transaction_id",
        submission_method="workflow_job",
        schema="gold",
    )

    # -----------------------------------------------------------------
    # 1. Get the feature rows we need to score
    # -----------------------------------------------------------------
    features = dbt.ref("go_predictive_model")

    if dbt.is_incremental:
        existing_max = session.sql(
            f"select coalesce(max(transaction_date), '1900-01-01') as m from {dbt.this}"
        ).collect()[0]["m"]
        features = features.filter(features.transaction_date > existing_max)

    features_pd = features.toPandas()

    FEATURE_COLS = [
        "amount_zscore", "velocity_1hr", "impossible_travel_flag",
        "new_device_flag", "device_amount_percentile", "off_hours_flag",
        "location_mismatch_flag", "merchant_risk_rate",
    ]

    if len(features_pd) == 0:
        empty = pd.DataFrame(columns=[
            "transaction_id", "user_id", "merchant_id",
            "transaction_date", "fraud_probability", "risk_tier"
        ])
        return session.createDataFrame(empty)

    # -----------------------------------------------------------------
    # 2. Load the "Production"-aliased model from the UC Model Registry
    # -----------------------------------------------------------------
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    model = mlflow.sklearn.load_model(
        "models:/workspace.default.fraud_detection_model@champion"
    )

    # -----------------------------------------------------------------
    # 3. Score
    # -----------------------------------------------------------------
    X = features_pd[FEATURE_COLS]
    fraud_probability = model.predict_proba(X)[:, 1]

    features_pd["fraud_probability"] = fraud_probability
    features_pd["risk_tier"] = pd.cut(
        fraud_probability,
        bins=[-0.01, 0.3, 0.7, 1.0],
        labels=["Low", "Medium", "High"],
    )

    result_pd = features_pd[[
        "transaction_id", "user_id", "merchant_id",
        "transaction_date", "fraud_probability", "risk_tier"
    ]]

    return session.createDataFrame(result_pd)