import os
import ssl
import hashlib
import pandas as pd
from sklearn.model_selection import train_test_split

# 1. Programmatically Bypass SSL Certificate Verification (Universal Bypass)
# This prevents "[SSL: CERTIFICATE_VERIFY_FAILED]" errors in local/Windows environments.
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.util.ssl_.create_urllib3_context = lambda *args, **kwargs: ssl._create_unverified_context()
ssl.create_default_context = ssl._create_unverified_context

# Also monkeypatch standard requests session
import requests
orig_send = requests.Session.send
requests.Session.send = lambda self, request, **kwargs: orig_send(self, request, **{**kwargs, 'verify': False})

os.environ['HF_HUB_DISABLE_SSL_VERIFY'] = '1'

def compute_sha256(filepath):
    """Compute SHA-256 hash of a file for lightweight data versioning."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def main():
    print("Downloading dataset 'arize-ai/ecommerce_reviews_with_language_drift' from Hugging Face LFS...")
    
    # URLs for training and validation splits (combining them for our own custom stratified 80/10/10 split)
    train_url = 'https://huggingface.co/datasets/arize-ai/ecommerce_reviews_with_language_drift/resolve/main/training.csv'
    val_url = 'https://huggingface.co/datasets/arize-ai/ecommerce_reviews_with_language_drift/resolve/main/validation.csv'
    
    # 2. Read the raw CSVs into DataFrames
    try:
        train_raw = pd.read_csv(train_url)
        val_raw = pd.read_csv(val_url)
        print(f"Raw splits downloaded successfully: Train={train_raw.shape}, Val={val_raw.shape}")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return
    
    # 3. Combine them to run a clean, custom 80/10/10 stratified split on the full 9,000 labeled reviews
    df_full = pd.concat([train_raw, val_raw], ignore_index=True)
    print(f"Combined total rows: {len(df_full)}")
    
    # Extract only the columns of interest and rename them
    # text -> review_text, label -> sentiment (already contains 'positive', 'negative', 'neutral')
    df_clean = df_full[['text', 'label']].rename(columns={'text': 'review_text', 'label': 'sentiment'})
    
    # Drop rows with null values to avoid training issues
    df_clean = df_clean.dropna().reset_index(drop=True)
    print(f"Cleaned dataset. Non-null rows: {len(df_clean)}")
    print("Class distribution:\n", df_clean['sentiment'].value_counts())
    
    # 4. Stratified train/val/test split (80/10/10)
    # First split: 80% train, 20% temp (which will be split 50/50 into val and test)
    train_df, temp_df = train_test_split(
        df_clean, 
        test_size=0.20, 
        random_state=42, 
        stratify=df_clean['sentiment']
    )
    
    val_df, test_df = train_test_split(
        temp_df, 
        test_size=0.50, 
        random_state=42, 
        stratify=temp_df['sentiment']
    )
    
    print(f"\nStratified Splits Completed:")
    print(f" - Train split: {len(train_df)} rows")
    print(f" - Validation split: {len(val_df)} rows")
    print(f" - Test split: {len(test_df)} rows")
    
    # 5. Save Splits as CSV files
    os.makedirs('data', exist_ok=True)
    
    train_path = 'data/train_v1.csv'
    val_path = 'data/val_v1.csv'
    test_path = 'data/test_v1.csv'
    
    train_df.to_csv(train_path, index=False, encoding='utf-8')
    val_df.to_csv(val_path, index=False, encoding='utf-8')
    test_df.to_csv(test_path, index=False, encoding='utf-8')
    
    print("\nData splits saved successfully to 'data/' directory.")
    
    # 6. Compute and Save SHA-256 Hashes for Data Versioning
    hash_train = compute_sha256(train_path)
    hash_val = compute_sha256(val_path)
    hash_test = compute_sha256(test_path)
    
    hash_filepath = 'data/dataset_hashes.txt'
    with open(hash_filepath, 'w') as h_file:
        h_file.write(f"train_v1.csv: {hash_train}\n")
        h_file.write(f"val_v1.csv: {hash_val}\n")
        h_file.write(f"test_v1.csv: {hash_test}\n")
        
    print(f"Data versioning completed. Hashes written to '{hash_filepath}':")
    print(f" - train_v1.csv: {hash_train}")
    print(f" - val_v1.csv: {hash_val}")
    print(f" - test_v1.csv: {hash_test}")

if __name__ == "__main__":
    main()
