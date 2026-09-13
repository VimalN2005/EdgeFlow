import os
import sys

# Add project root directory to sys.path so edgeflow can be imported on Vercel
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from edgeflow.main import app

# Export ASGI app instance for Vercel
__all__ = ["app"]
