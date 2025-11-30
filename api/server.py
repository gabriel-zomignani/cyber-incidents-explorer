"""
Flask API Server for Cyber Security Events App
Connects the React frontend with the CrewAI backend
"""
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import ai_engine
sys.path.append(str(Path(__file__).parent.parent))

from ai_engine.cyber import CyberAnalysisCrew

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend
import threading
import uuid
from typing import Dict

# Reports directory (mounted via Docker or local)
REPORTS_DIR = (Path(__file__).parent.parent / "reports").resolve()

# In-memory job store for async analysis (simple single-instance approach)
JOBS: Dict[str, dict] = {}
JOBS_LOCK = threading.Lock()

# Configure longer timeout for AI analysis requests
# First-time Ollama model loading can take 30-60 seconds
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.config['REQUEST_TIMEOUT'] = 300  # 5 minutes for complex analysis

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Cyber Security Events Analysis API'
    })

@app.route('/api/analyze', methods=['POST'])
def analyze_question():
    """
    Analyze a user question using CrewAI
    
    Expected JSON body:
    {
        "question": "What are the trends in cyber attacks from 2014 to 2024?"
    }
    
    Returns:
    {
        "success": true,
        "analysis": "Complete analysis report...",
        "report_file": "reports/cyber_analysis_report.md"
    }
    """
    try:
        data = request.json
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({
                'success': False,
                'error': 'Question is required'
            }), 400
        
        # Initialize and run the crew
        # Note: This can take 1-2 minutes depending on question complexity
        # First-time runs may take longer as Ollama loads the model into memory
        print(f"Analyzing question: {question}")
        print("Initializing CrewAI agents...")
        crew = CyberAnalysisCrew()
        
        print("Starting analysis (this may take 1-2 minutes)...")
        result = crew.kickoff(inputs={'question': question})
        print("Analysis complete!")
        
        return jsonify({
            'success': True,
            'analysis': str(result),
            'report_file': 'reports/cyber_analysis_report.md'
        })
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/analyze_async', methods=['POST'])
def analyze_async():
    """Start analysis in background and return a job id immediately."""
    try:
        data = request.json
        question = data.get('question', '').strip()
        if not question:
            return jsonify({'success': False, 'error': 'Question is required'}), 400

        job_id = str(uuid.uuid4())
        with JOBS_LOCK:
            JOBS[job_id] = {'status': 'pending', 'result': None, 'error': None}

        def run_job(jid, q):
            try:
                with JOBS_LOCK:
                    JOBS[jid]['status'] = 'running'
                crew = CyberAnalysisCrew()
                res = crew.kickoff(inputs={'question': q})
                with JOBS_LOCK:
                    JOBS[jid]['status'] = 'done'
                    JOBS[jid]['result'] = str(res)
            except Exception as ex:
                with JOBS_LOCK:
                    JOBS[jid]['status'] = 'error'
                    JOBS[jid]['error'] = str(ex)

        t = threading.Thread(target=run_job, args=(job_id, question), daemon=True)
        t.start()

        return jsonify({'success': True, 'job_id': job_id}), 202
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/analyze_status/<job_id>', methods=['GET'])
def analyze_status(job_id):
    """Return status and result (if ready) for a background job."""
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404

    payload = {'success': True, 'status': job['status']}
    if job['status'] == 'done':
        payload['result'] = job['result']
    if job['status'] == 'error':
        payload['error'] = job['error']

    return jsonify(payload)

@app.route('/api/examples', methods=['GET'])
def get_example_questions():
    """Get example questions users can ask"""
    examples = [
        "What are the trends in cyber attacks from 2014 to 2024?",
        "Which countries experience the most state-sponsored attacks?",
        "What is the relationship between actor type and attack motive?",
        "Which industries are most frequently targeted by criminal actors?",
        "Are there seasonal patterns in ransomware attacks?",
        "Compare cyber incident patterns between NATO and non-NATO countries",
        "What attack types were most common in 2020 vs 2023?",
        "Which regions are most targeted by hacktivist groups?"
    ]
    
    return jsonify({
        'examples': examples
    })


def _humanize_filename(filename: str) -> str:
    """Convert a filename into a friendlier display name."""
    stem = Path(filename).stem
    friendly = stem.replace('_', ' ').replace('-', ' ').strip()
    return friendly.title() if friendly else filename


@app.route('/api/reports', methods=['GET'])
def list_reports():
    """List available report files from the reports directory."""
    if not REPORTS_DIR.exists() or not REPORTS_DIR.is_dir():
        return jsonify({'reports': []})

    reports = []
    for f in REPORTS_DIR.iterdir():
        if not f.is_file():
            continue
        try:
            mtime = datetime.fromtimestamp(f.stat().st_mtime).isoformat()
        except Exception:
            mtime = None
        reports.append({
            'id': f.name,
            'name': _humanize_filename(f.name),
            'description': 'Generated report from ETL pipeline',
            'dateGenerated': mtime,
            'status': 'Completed'
        })

    return jsonify({'reports': reports})


@app.route('/api/reports/<report_id>/download', methods=['GET'])
def download_report(report_id):
    """Download a specific report file by id (filename)."""
    # Prevent path traversal by resolving and ensuring directory containment
    target_path = (REPORTS_DIR / report_id).resolve()
    if not str(target_path).startswith(str(REPORTS_DIR)) or not target_path.is_file():
        return jsonify({'success': False, 'error': 'Report not found'}), 404

    return send_from_directory(REPORTS_DIR, report_id, as_attachment=True)

if __name__ == '__main__':
    ollama_host = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
    ollama_model = os.getenv('OLLAMA_MODEL', 'ollama/gpt-oss:120b-cloud')
    print("Starting Cyber Security Events Analysis API...")
    print(f"Using Ollama LLM at: {ollama_host}")
    print(f"Model: {ollama_model}")
    print("API will be available at http://localhost:5000")
    app.run(debug=True, port=5000, host='0.0.0.0')
