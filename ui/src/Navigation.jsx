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
    }, 150);
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
          {/* public Home (landing) */}
          <li>
            <Link to="/">Home</Link>
          </li>

          <li>
            <Link to="/reports">Reports</Link>
          </li>

          {/* Visualization dropdown */}
          <li
            ref={dropdownRef}
            className="navbar-dropdown"
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
          >
            <span className="navbar-dropdown-label">Visualization</span>
            {vizDropdownOpen && (
              <ul className="navbar-submenu">
                <li>
                  <Link to="/table-view">Table View</Link>
                </li>
                <li>
                  <Link to="/visualizations">Pie Chart</Link>
                </li>
              </ul>
            )}
          </li>

          {/* Admin-only link */}
          {user?.role === "admin" && (
            <li>
              <Link to="/accounts">User Management</Link>
            </li>
          )}

          {/* User menu: change password + logout */}
          {user && (
            <li
              className="navbar-dropdown user-dropdown"
              ref={userMenuRef}
              style={{ position: "relative" }}
            >
              <button
                className="user-menu-trigger"
                style={{
                  background: "none",
                  border: "none",
                  color: "white",
                  fontWeight: 500,
                  fontSize: "1rem",
                  cursor: "pointer",
                  padding: "0.25rem 1.2rem",
                  borderRadius: "4px",
                }}
                onClick={() => setUserMenuOpen((open) => !open)}
                aria-haspopup="true"
                aria-expanded={userMenuOpen}
              >
                {user.username}
                <span style={{ marginLeft: 8, fontSize: "0.9em" }}>▼</span>
              </button>

              {userMenuOpen && (
                <ul
                  className="navbar-submenu user-menu"
                  style={{
                    minWidth: 180,
                    right: 0,
                    left: "auto",
                    marginTop: 8,
                    padding: 0,
                  }}
                >
                  <li style={{ padding: 0, margin: 0 }}>
                    <Link
                      to="/change-password"
                      style={{
                        padding: "0.75rem 1.5rem",
                        display: "block",
                        color: "white",
                        textDecoration: "none",
                        fontWeight: 400,
                        fontSize: "1rem",
                      }}
                      onClick={() => setUserMenuOpen(false)}
                    >
                      Change Password
                    </Link>
                  </li>
                  <li style={{ padding: 0, margin: 0 }}>
                    <button
                      onClick={() => {
                        setUserMenuOpen(false);
                        handleLogout();
                      }}
                      style={{
                        background: "none",
                        border: "none",
                        color: "white",
                        cursor: "pointer",
                        fontSize: "1rem",
                        fontWeight: 400,
                        padding: "0.75rem 1.5rem",
                        width: "100%",
                        textAlign: "left",
                      }}
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
