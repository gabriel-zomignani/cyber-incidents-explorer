import { Link } from "react-router-dom";
import Navigation from "./Navigation";
import "./HomePage.css";

export default function HomePage() {
  return (
    <div className="app-container">
      <Navigation />

      {/* Main Content */}
      <main className="main-content">
        <div className="content-card">
          <h2>Welcome to Cyber Security Events App</h2>
          <p>Explore and analyze global cyber-security events with interactive visualizations and detailed reports.</p>

          <div className="dashboard-grid">
            <Link to="/reports" className="dashboard-widget">
              <h3>Reports</h3>
              <p>View detailed incident reports</p>
            </Link>
            <Link to="/visualization/table" className="dashboard-widget">
              <h3>Data Table</h3>
              <p>Analyze raw data in table format</p>
            </Link>
            <Link to="/visualization/pie-chart" className="dashboard-widget">
              <h3>Visualizations</h3>
              <p>See graphical insights</p>
            </Link>
            <Link to="/ai" className="dashboard-widget">
              <h3>AI Assistant</h3>
              <p>Get AI-powered insights</p>
            </Link>
          </div>
        </div>
      </main>
    </div>
  );
}
