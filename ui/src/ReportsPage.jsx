import Navigation from "./Navigation";
import "./HomePage.css";
import "./ReportsPage.css";

export default function ReportsPage() {
  // Hardcoded dummy data as requested
  const reports = [
    {
      id: 1,
      name: "Q3 Security Audit",
      description: "Comprehensive security audit for Q3 2024 including vulnerability assessment.",
      date: "2024-10-15",
      status: "Completed"
    },
    {
      id: 2,
      name: "Incident #402 Analysis",
      description: "Post-mortem analysis of the phishing attempt detected on Oct 12.",
      date: "2024-10-13",
      status: "Pending"
    },
    {
      id: 3,
      name: "Weekly Threat Summary",
      description: "Summary of blocked IPs and failed login attempts for the week of Oct 7.",
      date: "2024-10-10",
      status: "Completed"
    },
    {
      id: 4,
      name: "User Access Review",
      description: "Quarterly review of privileged user accounts and permissions.",
      date: "2024-09-30",
      status: "Completed"
    },
    {
      id: 5,
      name: "Firewall Configuration Backup",
      description: "Routine backup of firewall rules and configurations.",
      date: "2024-09-28",
      status: "Completed"
    }
  ];

  const handleDownload = async (report) => {
    try {
      const res = await fetch(`/api/reports/${report.id}/download`, {
        method: "GET",
      });

      if (!res.ok) {
        throw new Error("Download failed");
      }

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");

      a.href = url;
      a.download = `${report.name}.pdf`; // change extension if needed
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      alert("Report downloaded successfully");
    } catch (err) {
      console.error(err);
      alert("Could not download report. Please try again.");
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
                      <button
                        className="action-btn"
                        onClick={() => handleDownload(report)}
                      >Download</button>
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
