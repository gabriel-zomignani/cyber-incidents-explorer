import { useEffect, useMemo, useState } from "react";
import Navigation from "./Navigation";
import "./HomePage.css";
import "./ReportsPage.css";

export default function ReportsPage() {
  const [reports, setReports] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

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
        setReports(Array.isArray(data?.reports) ? data.reports : []);
      } catch (err) {
        console.error(err);
        setError("Could not load reports. Please try again.");
      } finally {
        setLoading(false);
      }
    };

    load();
  }, [API_BASE]);

  const handleViewDetails = (report) => {
    alert(
      `Report: ${report.name}\nDescription: ${report.description}\nDate: ${report.dateGenerated ?? "N/A"}\nStatus: ${report.status}`
    );
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
                  {reports.length === 0 ? (
                    <tr>
                      <td colSpan={5} style={{ padding: "1.5rem" }}>No reports available.</td>
                    </tr>
                  ) : (
                    reports.map((report) => (
                      <tr key={report.id}>
                        <td className="report-name-cell">{report.name}</td>
                        <td className="report-desc-cell">{report.description}</td>
                        <td className="report-date-cell">{report.dateGenerated || "N/A"}</td>
                        <td>
                          <span className={`status-badge ${report.status === 'Completed' ? 'status-completed' : 'status-pending'}`}>
                            {report.status}
                          </span>
                        </td>
                        <td>
                          <button className="action-btn" onClick={() => handleViewDetails(report)}>
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
                    ))
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
