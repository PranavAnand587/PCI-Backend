# PCI Complaints Analysis API

This is the FastAPI backend for the PCI Complaints Dashboard.

## Setup

1.  **Install Dependencies**:
    Navigate to this directory (`api_dev`) and run:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Database**:
    Ensure `complaints.db` is present in this directory.
    Ensure `india_states.geojson` is present in this directory (required for map visualizations).

## Running the Server

To start the development server with auto-reload:

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`.

## Environment Variables

Currently, no specific environment variables are required for local development.
- **Database**: Uses `complaints.db` in the current directory.
- **CORS**: Configured to allow all origins (`*`) by default.
