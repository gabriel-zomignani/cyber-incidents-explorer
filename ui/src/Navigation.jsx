import { Link, useNavigate } from "react-router-dom";
import { useState, useRef, useEffect } from "react";
import { useAuth } from "./AuthContext";
import "./Navigation.css";

export default function Navigation() {
  const [vizDropdownOpen, setVizDropdownOpen] = useState(false);
  const dropdownRef = useRef(null);
  const closeTimeoutRef = useRef(null);
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const handleMouseEnter = () => {
    if (closeTimeoutRef.current) {
      clearTimeout(closeTimeoutRef.current);
    }
    setVizDropdownOpen(true);
  };

  const handleMouseLeave = () => {
    closeTimeoutRef.current = setTimeout(() => {
      setVizDropdownOpen(false);
    }, 150); // 150ms delay
  };

  const handleLogout = () => {
    logout();
    navigate("/login");
  };

  // User menu dropdown state
  const [userMenuOpen, setUserMenuOpen] = useState(false);
  const userMenuRef = useRef(null);

  // Close user menu on outside click
  useEffect(() => {
    if (!userMenuOpen) return;
    function handleClick(e) {
      if (userMenuRef.current && !userMenuRef.current.contains(e.target)) {
        setUserMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [userMenuOpen]);

  return (
    <nav className="navbar">
      <div className="navbar-content">
        <h1 className="navbar-title">Cyber Security Events App</h1>
        <ul className="navbar-links">
          <li><Link to="/">Home</Link></li>
          <li><Link to="/reports">Reports</Link></li>
          <li
            ref={dropdownRef}
            className="navbar-dropdown"
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
          >
            <span className="navbar-dropdown-label">Visualization</span>
            {vizDropdownOpen && (
              <ul className="navbar-submenu">
                <li><Link to="/visualization/table">Table View</Link></li>
                <li><Link to="/visualization/pie-chart">Pie Chart</Link></li>
              </ul>
            )}
          </li>
          <li><Link to="/ai">AI</Link></li>
          {user?.role === "admin" && (
            <li><Link to="/accounts">User Management</Link></li>
          )}
          {user && (
            <li
              className="navbar-dropdown user-dropdown"
              ref={userMenuRef}
            >
              <button
                className="user-menu-trigger"
                onClick={() => setUserMenuOpen((open) => !open)}
                aria-haspopup="true"
                aria-expanded={userMenuOpen}
              >
                {user.username}
                <span style={{ fontSize: "0.8em", opacity: 0.7 }}>▼</span>
              </button>
              {userMenuOpen && (
                <ul className="navbar-submenu user-menu">
                  <li>
                    <Link to="/change-password" onClick={() => setUserMenuOpen(false)}>
                      Change Password
                    </Link>
                  </li>
                  <li>
                    <button
                      onClick={() => { setUserMenuOpen(false); handleLogout(); }}
                    >
                      Logout
                    </button>
                  </li>
                </ul>
              )}
            </li>
          )}
        </ul>
      </div>
    </nav>
  );
}
