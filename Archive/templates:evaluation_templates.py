from sklearn.metrics import classification_report, mean_squared_error

def summarize_performance(y_true, y_pred, mode='classification'):
    """
    Standardized performance reporting.
    """
    if mode == 'classification':
        return classification_report(y_true, y_pred)
    else:
        return {"mse": mean_squared_error(y_true, y_pred)}