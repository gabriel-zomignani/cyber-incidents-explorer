// ui/src/LandingPage.jsx
import { Link } from "react-router-dom";

export default function LandingPage() {
  return (
    <div className="landing">
      <h1>Cyber Incidents Explorer</h1>

      <div className="landing-buttons">
        <Link to="/login">
          <button>Go to Login / Signup</button>
        </Link>

        <Link to="/table-view">
          <button>Open Database Table</button>
        </Link>

        <Link to="/visualizations">
          <button>Open Visualizations</button>
        </Link>
      </div>
    </div>
  );
}
