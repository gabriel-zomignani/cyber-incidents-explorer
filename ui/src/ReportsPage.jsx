import { useState, useEffect } from "react";
import Navigation from "./Navigation";
import "./HomePage.css";
import "./ReportsPage.css";

export default function ReportsPage() {
  const [reports, setReports] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetch("/data/reports.json")
      .then((res) => {
        if (!res.ok) {
          throw new Error("Failed to fetch reports");
        }
        return res.json();
      })
      .then((data) => {
        setReports(data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Error loading reports:", err);
        setError("Failed to load reports. Please try again later.");
        setLoading(false);
      });
  }, []);

  if (loading) {
    return (
      <div className="app-container">
        <Navigation />
        <div className="main-content">
          <div className="reports-container">
            <div className="reports-header">
              <h2>Generated Reports</h2>
            </div>
            <div className="loading-state">Loading reports...</div>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="app-container">
        <Navigation />
        <div className="main-content">
          <div className="reports-container">
            <div className="reports-header">
              <h2>Generated Reports</h2>
            </div>
            <div className="error-message">{error}</div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="app-container">
      <Navigation />
      <div className="main-content">
        <div className="reports-container">
          <div className="reports-header">
            <h2>Generated Reports</h2>
          </div>

          <div className="reports-table-container">
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
                    <td className="report-name-cell">{report.name}</td>
                    <td className="report-desc-cell">{report.description}</td>
                    <td className="report-date-cell">{report.date}</td>
                    <td>
                      <span className={`status-badge ${report.status === 'Completed' ? 'status-completed' : 'status-pending'}`}>
                        {report.status}
                      </span>
                    </td>
                    <td>
                      <button className="action-btn">View Details</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
