import React, { useState } from "react";
import Navigation from "./Navigation";
import "./AIPage.css";
import { marked } from 'marked';
import html2canvas from 'html2canvas';
import jsPDF from 'jspdf';

export default function AIPage() {
  const [question, setQuestion] = useState("");
  const [result, setResult] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setResult("");
    setError("");
    try {
      const API_BASE = import.meta.env.VITE_API_URL ?? `${window.location.protocol}//${window.location.hostname}:5000`;

      // Submit job (async) to the backend
      const resp = await fetch(`${API_BASE}/api/analyze_async`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question }),
      });
      const json = await resp.json();
      if (!json.success) {
        setError(json.error || 'Failed to start analysis');
        setLoading(false);
        return;
      }

      const jobId = json.job_id;
      // Poll for result
      const pollIntervalMs = 2000; // 2s
      const maxWaitMs = 10 * 60 * 1000; // 10 minutes total
      const start = Date.now();

      const poll = async () => {
        try {
          const sresp = await fetch(`${API_BASE}/api/analyze_status/${jobId}`);
          const sjson = await sresp.json();
          if (!sjson.success) {
            setError(sjson.error || 'Error checking job status');
            setLoading(false);
            clearInterval(interval);
            return;
          }

          if (sjson.status === 'done') {
            setResult(sjson.result || '');
            setLoading(false);
            clearInterval(interval);
            return;
          }

          if (sjson.status === 'error') {
            setError(sjson.error || 'Analysis failed');
            setLoading(false);
            clearInterval(interval);
            return;
          }

          if (Date.now() - start > maxWaitMs) {
            setError('Timed out waiting for analysis. Try again later.');
            setLoading(false);
            clearInterval(interval);
            return;
          }
          // otherwise still pending/running
        } catch (pollErr) {
          setError('Network error while polling result');
          setLoading(false);
          clearInterval(interval);
        }
      };

      // Start polling immediately and then at interval
      await poll();
      const interval = setInterval(poll, pollIntervalMs);

    } catch (err) {
      setError('Network error');
      setLoading(false);
    }
  };

  return (
    <div className="app-container">
      <Navigation />

      <main className="main-content ai-main-top">
        <div className="content-card content-card--wide content-card--top">

          <form onSubmit={handleSubmit} className="ai-form">
            <textarea
              className="ai-question-box"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder="Type your question about cyber security events..."
              rows={4}
            />
            <div className="ai-actions">
              <button type="submit" className="btn btn-primary" disabled={loading || !question.trim()}>
                {loading ? "Analyzing..." : "Submit"}
              </button>
              <button
                type="button"
                className="btn btn-secondary"
                disabled={!result || loading}
                onClick={async () => {
                  // Generate PDF from the rendered result box with proper paging
                  try {
                    const el = document.querySelector('.ai-result-box');
                    if (!el) return;

                    // Render element to canvas at higher scale for better quality
                    const scale = 2;
                    const canvas = await html2canvas(el, { scale });

                    const imgWidth = canvas.width;
                    const imgHeight = canvas.height;

                    // Create PDF in points (pt). jsPDF default unit is 'pt' when using 'pt'.
                    const pdf = new jsPDF('p', 'pt', 'a4');
                    const pdfWidth = pdf.internal.pageSize.getWidth();
                    const pdfHeight = pdf.internal.pageSize.getHeight();

                    // Calculate the ratio to fit canvas width into PDF page width
                    const ratio = pdfWidth / imgWidth;
                    const scaledImgHeight = imgHeight * ratio;

                    if (scaledImgHeight <= pdfHeight) {
                      // Single page
                      const imgData = canvas.toDataURL('image/png');
                      pdf.addImage(imgData, 'PNG', 0, 0, pdfWidth, scaledImgHeight);
                    } else {
                      // Multi-page: slice the canvas vertically into page-sized chunks
                      const pageCanvasHeight = Math.floor(pdfHeight / ratio); // height in canvas pixels per PDF page
                      let yOffset = 0;
                      while (yOffset < imgHeight) {
                        // Create a temporary canvas to hold one page slice
                        const tmpCanvas = document.createElement('canvas');
                        tmpCanvas.width = imgWidth;
                        tmpCanvas.height = Math.min(pageCanvasHeight, imgHeight - yOffset);
                        const tmpCtx = tmpCanvas.getContext('2d');
                        // draw the slice from the main canvas
                        tmpCtx.drawImage(canvas, 0, yOffset, imgWidth, tmpCanvas.height, 0, 0, imgWidth, tmpCanvas.height);

                        const imgData = tmpCanvas.toDataURL('image/png');
                        const imgPageHeight = tmpCanvas.height * ratio; // height in PDF points

                        if (yOffset > 0) pdf.addPage();
                        pdf.addImage(imgData, 'PNG', 0, 0, pdfWidth, imgPageHeight);

                        yOffset += tmpCanvas.height;
                      }
                    }

                    pdf.save('cyber_analysis_report.pdf');
                  } catch (err) {
                    console.error('PDF generation failed', err);
                    alert('Failed to generate PDF.');
                  }
                }}
              >
                Create PDF Report
              </button>
            </div>
          </form>

          {error && <div className="ai-error">{error}</div>}

          <div className="ai-result-container">
            <div className="ai-result-box">
              {result ? (
                // Some models return a "Final Output:" prefix or raw HTML.
                // Normalize: strip common prefixes, then detect HTML vs Markdown.
                (() => {
                  const raw = result || "";
                  // Remove common leading tag like 'Final Output:'
                  const withoutPrefix = raw.replace(/^\s*Final Output:\s*/i, "").trim();

                  let htmlContent;
                  if (withoutPrefix.startsWith("<")) {
                    // Already HTML — render as-is (no sanitization per user request)
                    htmlContent = withoutPrefix;
                  } else {
                    // Treat as Markdown
                    htmlContent = marked.parse(withoutPrefix);
                  }

                  return (
                    <div
                      className="ai-markdown"
                      dangerouslySetInnerHTML={{ __html: htmlContent }}
                    />
                  );
                })()
              ) : loading ? (
                <span>Waiting for AI response...</span>
              ) : (
                <span className="ai-placeholder">Result will appear here.</span>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
