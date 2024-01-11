import json

def load_config():
    with open('config.json') as f:
        config = json.load(f)
    return config

def read_csv_with_encoding(url, encodings=['utf-8', 'latin1', 'cp1252']):
    """Read CSV with multiple encodings and return a DataFrame."""
    for encoding in encodings:
        try:
            return pd.read_csv(url, encoding=encoding)
        except UnicodeDecodeError:
            print(f"Error with {encoding} encoding. Trying another.")
