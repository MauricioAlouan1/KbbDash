"""
Dynamic DATA_ROOT resolution for KaBaby KbbDash data system.

Resolves the external Dropbox data directory at runtime by checking
multiple candidate paths on different machines.
"""

from pathlib import Path
from typing import Optional

# Module-level cache
_data_root: Optional[Path] = None

# Candidate paths (check in order)
CANDIDATE_PATHS = [
    Path("/Users/mauricioalouan/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data"),
    Path("/Users/simon/Library/CloudStorage/Dropbox/KBB MF/AAA/Balancetes/Fechamentos/data"),
]


def get_data_root() -> Path:
    """
    Resolve and return the DATA_ROOT path.
    
    Checks candidate paths in order and returns the first existing one.
    Caches the result after first resolution.
    
    Returns:
        Path: The resolved DATA_ROOT directory
        
    Raises:
        FileNotFoundError: If none of the candidate paths exist
    """
    global _data_root
    
    if _data_root is not None:
        return _data_root
    
    for candidate in CANDIDATE_PATHS:
        if candidate.exists() and candidate.is_dir():
            _data_root = candidate.resolve()
            return _data_root
    
    error_msg = (
        "❌ DATA_ROOT not found. None of the candidate directories exist:\n"
        + "\n".join(f"  - {p}" for p in CANDIDATE_PATHS)
        + "\n\nPlease ensure Dropbox is synced and the data folder is accessible."
    )
    raise FileNotFoundError(error_msg)

