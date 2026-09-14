import re
from pyvi import ViTokenizer

def clean_text(text):
    """
    Consistently preprocesses raw review text for training and serving.
    
    1. Converts text to lowercase.
    2. Strips HTML tags (e.g., <br />, <a>, etc.).
    3. Performs Vietnamese compound word segmentation via PyVi.
    4. Keeps only alphanumeric characters, underscores (from word tokens), standard punctuation, and whitespace.
    5. Trims excessive whitespace.
    """
    if not isinstance(text, str):
        return ""
        
    # Convert to lowercase
    text = text.lower()
    
    # Strip HTML tags
    text = re.sub(r'<[^>]*>', ' ', text)
    
    # Vietnamese word segmentation (e.g., "xe điện" -> "xe_điện")
    text = ViTokenizer.tokenize(text)
    
    # Keep unicode letters (including Vietnamese accents), numbers, spaces, and basic punctuation
    text = re.sub(r'[^\w\s\!\?\,\.\']', '', text, flags=re.UNICODE)
    
    # Clean up multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text
