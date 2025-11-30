# Reports List Manual Test Checklist

1) Start backend and frontend
- Run `python api/server.py` (or `docker compose up`).
- Run `npm install` and `npm run dev` in `ui` if not already running.

2) Load reports page
- Navigate to `/reports` in the UI.
- Confirm the table shows “Loading...” briefly, then lists available files or “No reports available.”

3) Verify data rendering
- Confirm each row shows name, description, dateGenerated, status, and action buttons.
- Click “View Details” and see an alert with report metadata.

4) Download
- Click “Download” for a listed report; the file should download without errors.

5) Error handling
- Temporarily stop the API and refresh `/reports`; an error message should appear instead of the table rows.
