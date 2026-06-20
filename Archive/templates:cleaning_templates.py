def handle_outliers(df, column, threshold=3):
    """
    Removes outliers using Z-score method.
    """
    z_scores = (df[column] - df[column].mean()) / df[column].std()
    return df[abs(z_scores) < threshold]