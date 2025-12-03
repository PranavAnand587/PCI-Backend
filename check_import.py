import sys
import os

# Add the directory to sys.path
sys.path.append(os.getcwd())

print("Attempting to import research router...")
try:
    from routers import research
    print("Successfully imported research router")
except Exception as e:
    print(f"Failed to import research router: {e}")
