import { useEffect, useMemo, useState } from "react";
import Navigation from "./Navigation";
import "./HomePage.css";
import "./ReportsPage.css";

export default function ReportsPage() {
  const [reports, setReports] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [showDetails, setShowDetails] = useState(false);
  const [details, setDetails] = useState(null);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [detailsError, setDetailsError] = useState("");

  const API_BASE = useMemo(
    () => import.meta.env.VITE_API_URL ?? `${window.location.protocol}//${window.location.hostname}:5000`,
    []
  );

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError("");
      try {
        const res = await fetch(`${API_BASE}/api/reports`);
        if (!res.ok) {
          throw new Error("Failed to load reports");
        }
        const data = await res.json();
        const reportsList = Array.isArray(data?.reports) ? data.reports : [];
        // Sort by dateGenerated descending (newest first) as a backup
        reportsList.sort((a, b) => {
          const dateA = a.dateGenerated || '';
          const dateB = b.dateGenerated || '';
          return dateB.localeCompare(dateA);
        });
        setReports(reportsList);
      } catch (err) {
        console.error(err);
        setError("Could not load reports. Please try again.");
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [API_BASE]);

  const handleViewDetails = async (reportId) => {
    setShowDetails(true);
    setDetails(null);
    setDetailsError("");
    setDetailsLoading(true);
    try {
      const res = await fetch(`${API_BASE}/api/reports/${reportId}`);
      if (!res.ok) {
        throw new Error("Failed to load details");
      }
      const data = await res.json();
      setDetails(data?.report ?? null);
    } catch (err) {
      console.error(err);
      setDetailsError("Could not load report details.");
    } finally {
      setDetailsLoading(false);
    }
  };

  return (
    <div className="app-container">
      <Navigation />
      <div className="main-content page-container">
        <div className="reports-container">
          <div className="reports-header">
            <h2>Generated Reports</h2>
          </div>

          <div className="reports-table-container">
            {loading ? (
              <div className="reports-table">
                <div style={{ padding: "1.5rem" }}>Loading...</div>
              </div>
            ) : error ? (
              <div className="reports-table">
                <div style={{ padding: "1.5rem", color: "var(--accent-warning)" }}>{error}</div>
              </div>
            ) : reports.length === 0 ? (
              <div className="reports-empty">
                <h3>No reports available yet</h3>
                <p>Generated reports will appear here once they are created.</p>
              </div>
            ) : (
              <table className="reports-table">
                <thead>
                  <tr>
                    <th>Report Name</th>
                    <th>Description</th>
                    <th>Date Generated</th>
                    <th>Status</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {reports.map((report) => (
                    <tr key={report.id}>
                      <td className="report-name-cell">{report.displayName || report.name}</td>
                      <td className="report-desc-cell">{report.description}</td>
                      <td className="report-date-cell">{report.dateGenerated || "N/A"}</td>
                      <td>
                        <span className={`status-badge ${report.status === 'Completed' ? 'status-completed' : 'status-pending'}`}>
                          {report.status}
                        </span>
                      </td>
                      <td>
                        <button className="action-btn" onClick={() => handleViewDetails(report.id)}>
                          View Details
                        </button>
                        <a
                          className="action-btn"
                          href={`${API_BASE}/api/reports/${encodeURIComponent(report.id)}/download`}
                        >
                          Download
                        </a>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      {showDetails && (
        <div className="reports-modal-backdrop" onClick={() => setShowDetails(false)}>
          <div className="reports-modal" onClick={(e) => e.stopPropagation()}>
            <div className="reports-modal-header">
              <h3>Report Details</h3>
              <button className="action-btn" onClick={() => setShowDetails(false)}>Close</button>
            </div>
            <div className="reports-modal-body">
              {detailsLoading && <div>Loading details...</div>}
              {detailsError && <div className="reports-modal-error">{detailsError}</div>}
              {!detailsLoading && !detailsError && details && (
                <div className="reports-modal-grid">
                  <div><strong>Name:</strong> {details.displayName || details.name}</div>
                  <div><strong>Description:</strong> {details.description}</div>
                  <div><strong>Date Generated:</strong> {details.dateGenerated || "N/A"}</div>
                  <div><strong>Status:</strong> {details.status}</div>
                  <div><strong>File Name:</strong> {details.fileName}</div>
                </div>
              )}
              {!detailsLoading && !detailsError && !details && (
                <div>No details available.</div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
