import re

def clean_text(text):
    """
    Consistently preprocesses raw review text for training and serving.
    
    1. Converts text to lowercase.
    2. Strips HTML tags (e.g., <br />, <a>, etc.).
    3. Keeps only alphanumeric characters, standard punctuation, and whitespace.
    4. Trims excessive whitespace.
    """
    if not isinstance(text, str):
        return ""
        
    # Convert to lowercase
    text = text.lower()
    
    # Strip HTML tags
    text = re.sub(r'<[^>]*>', ' ', text)
    
    # Keep only letters, numbers, spaces, and basic punctuation
    text = re.sub(r'[^a-zA-Z0-9\s\!\?\,\.\']', '', text)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text
