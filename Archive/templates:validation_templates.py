def validate_schema(df, required_columns):
    """
    Ensures the incoming data has all columns required by the model.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Data missing required columns: {missing}")
    return True