import { Link } from "react-router-dom";

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
          
          
        </div>
      </main>
    </div>
  );
}
