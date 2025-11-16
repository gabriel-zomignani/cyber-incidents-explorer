// ui/src/App.jsx
import React from "react";
import { Routes, Route } from "react-router-dom";

import Navigation from "./Navigation.jsx";
import LandingPage from "./LandingPage.jsx";
import HomePage from "./HomePage.jsx";
import LoginPage from "./LoginPage.jsx";
import SignupPage from "./SignupPage.jsx";
import ReportsPage from "./ReportsPage.jsx";
import AccountsPage from "./AccountsPage.jsx";
import ChangePasswordPage from "./ChangePasswordPage.jsx";
import TableViewPage from "./TableViewPage.jsx";
import VisualizationView from "./VisualizationView.jsx";

// ProtectedRoute exists but is now a simple passthrough
import ProtectedRoute from "./ProtectedRoute.jsx";

export default function App() {
  return (
    <div className="app-root">
      {/* Single navbar at the top for all pages */}
      <Navigation />

      <Routes>
        {/* Public routes */}
        <Route path="/" element={<LandingPage />} />
        <Route path="/login" element={<LoginPage />} />
        <Route path="/signup" element={<SignupPage />} />

        {/* Everything below is effectively public now because ProtectedRoute just returns children */}

        <Route
          path="/home"
          element={
            <ProtectedRoute>
              <HomePage />
            </ProtectedRoute>
          }
        />

        <Route
          path="/reports"
          element={
            <ProtectedRoute>
              <ReportsPage />
            </ProtectedRoute>
          }
        />

        <Route
          path="/accounts"
          element={
            <ProtectedRoute>
              <AccountsPage />
            </ProtectedRoute>
          }
        />

        <Route
          path="/change-password"
          element={
            <ProtectedRoute>
              <ChangePasswordPage />
            </ProtectedRoute>
          }
        />

        {/* 👉 Your database table view */}
        <Route
          path="/table-view"
          element={
            <ProtectedRoute>
              <TableViewPage />
            </ProtectedRoute>
          }
        />

        {/* 👉 Your pie chart view */}
        <Route
          path="/visualizations"
          element={
            <ProtectedRoute>
              <VisualizationView />
            </ProtectedRoute>
          }
        />
      </Routes>
    </div>
  );
}
