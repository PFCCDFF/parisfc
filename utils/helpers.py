# utils/helpers.py
import unicodedata
import os
from typing import Optional

def normalize_str(s: str) -> str:
    """Normalise une chaîne de caractères pour la comparaison."""
    if s is None:
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split()).lower()
    return s

def find_local_file_by_normalized_name(folder: str, target_name: str) -> Optional[str]:
    """Trouve un fichier local en comparant les noms normalisés."""
    if not os.path.exists(folder):
        return None
    target_norm = normalize_str(target_name)
    for fn in os.listdir(folder):
        if normalize_str(fn) == target_norm:
            return os.path.join(folder, fn)
    return None

def safe_float(x, default=float('nan')):
    """Convertit une valeur en float, avec gestion des erreurs."""
    try:
        if x is None or (isinstance(x, float) and x != x):  # Check for NaN
            return default
        return float(x)
    except (ValueError, TypeError):
        return default
