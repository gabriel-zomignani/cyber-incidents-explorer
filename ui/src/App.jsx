import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import { AuthProvider } from "./AuthContext";
import ProtectedRoute from "./ProtectedRoute";
import LoginPage from "./LoginPage";
import SignupPage from "./SignupPage";
import HomePage from "./HomePage";
import ReportsPage from "./ReportsPage";
import TableViewPage from "./TableViewPage";
import PieChartView from "./PieChartView";
import AccountsPage from "./AccountsPage";
import ChangePasswordPage from "./ChangePasswordPage";
import AIPage from "./AIPage";
import "./App.css";

export default function App() {
  return (
    <Router>
      <AuthProvider>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/signup" element={<SignupPage />} />
          <Route path="/" element={<ProtectedRoute element={<HomePage />} allowedRoles={["admin", "analyst"]} />} />
          <Route path="/reports" element={<ProtectedRoute element={<ReportsPage />} allowedRoles={["admin", "analyst"]} />} />
          <Route path="/visualization/table" element={<ProtectedRoute element={<TableViewPage />} allowedRoles={["admin", "analyst"]} />} />
          <Route path="/visualization/pie-chart" element={<ProtectedRoute element={<PieChartView />} allowedRoles={["admin", "analyst"]} />} />
          <Route path="/accounts" element={<ProtectedRoute element={<AccountsPage />} allowedRoles={["admin"]} />} />
          <Route path="/change-password" element={<ProtectedRoute element={<ChangePasswordPage />} allowedRoles={["admin", "analyst"]} />} />
          <Route path="/ai" element={<ProtectedRoute element={<AIPage />} allowedRoles={["admin", "analyst"]} />} />
        </Routes>
      </AuthProvider>
    </Router>
  );
}
