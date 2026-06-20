from sklearn.preprocessing import StandardScaler, OneHotEncoder
import pandas as pd

def scale_features(df, columns):
    """
    Standardizes numerical features to have mean=0 and variance=1.
    Essential for distance-based models (e.g., SVM, K-Means).
    """
    scaler = StandardScaler()
    df[columns] = scaler.fit_transform(df[columns])
    return df

def encode_categorical(df, columns):
    """
    Converts categorical strings into binary columns.
    Example: 'City' -> 'City_London', 'City_Paris'.
    """
    return pd.get_dummies(df, columns=columns)