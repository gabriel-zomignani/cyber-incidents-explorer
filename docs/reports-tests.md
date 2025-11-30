# Reports List Manual Test Checklist

## Endpoints
- `GET /api/reports` — list reports with metadata merged from metadata.json and filesystem
- `GET /api/reports/<id>` — single report metadata
- `GET /api/reports/<id>/download` — download a report file

## Steps
1) Start backend and frontend
- Run `python api/server.py` (or `docker compose up`).
- Run `npm install` and `npm run dev` in `ui` if not already running.

2) Load reports page
- Navigate to `/reports` in the UI.
- Confirm you briefly see "Loading..." then a table of reports.
- If there are no reports, an empty state message should appear instead of an empty table.

3) Verify data rendering
- Each row shows displayName, description, dateGenerated, status, and action buttons.
- Click "View Details" to open the modal and see displayName, description, dateGenerated, status, and fileName.

4) Download
- Click "Download" for a listed report; the file should download without errors.

5) Missing file handling
- Temporarily rename a report file in `reports/` to simulate a missing file, reload `/reports`, and see the status marked as Missing in the list and details.

6) Error handling
- Temporarily stop the API and refresh `/reports`; an error message should appear instead of the table rows.
