import os
def ensure_directories(directories):
    """Ensure that the specified directories exist."""
    try:
        for d in directories:
            os.makedirs(d, exist_ok=True)
        print("[INFO]    Folder structure validated.")
    except Exception as e:
        print(f"[FAIL]        Error creating directories: {e}")
        exit(1)

