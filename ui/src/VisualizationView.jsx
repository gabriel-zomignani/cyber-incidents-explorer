// ui/src/VisualizationView.jsx
import React from "react";
import PieChartView from "./PieChartView.jsx";
import "./VisualizationView.css";

export default function VisualizationView() {
  return (
    <div className="viz-page">
      <h1 className="viz-title">Visualization</h1>

      <div className="viz-card">
        {/* 👉 This is Pawel's pie chart component */}
        <PieChartView />
      </div>
    </div>
  );
}
