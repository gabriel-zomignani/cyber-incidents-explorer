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
import json

# Add parent directory to path to import ai_engine
sys.path.append(str(Path(__file__).parent.parent))

from ai_engine.cyber import CyberAnalysisCrew

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend
import threading
import uuid
from typing import Dict

# Reports directory and metadata
REPORTS_DIR = (Path(__file__).parent.parent / "reports").resolve()
METADATA_FILE = REPORTS_DIR / "metadata.json"

# In-memory job store for async analysis (simple single-instance approach)
JOBS: Dict[str, dict] = {}
JOBS_LOCK = threading.Lock()

def _save_report_metadata(filename: str, question: str):
    """Save metadata for a generated report."""
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        metadata = _load_metadata()
        
        # Add new report metadata
        metadata.append({
            'id': filename,
            'fileName': filename,
            'description': question,
            'dateGenerated': datetime.utcnow().isoformat(),
            'status': 'Completed'
        })
        
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
    except Exception as e:
        print(f"Error saving metadata: {e}")

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
        # Suppress interactive prompts by redirecting stdin
        import sys, os
        old_stdin = sys.stdin
        try:
            sys.stdin = open(os.devnull, 'r')
            result = crew.kickoff(inputs={'question': question})
        finally:
            sys.stdin = old_stdin
        print("Analysis complete!")
        
        # Extract filename and save metadata
        filename = None
        try:
            if hasattr(result, 'tasks_output') and result.tasks_output:
                last_task = result.tasks_output[-1]
                if hasattr(last_task, 'output_file') and last_task.output_file:
                    filename = Path(last_task.output_file).name
        except Exception:
            pass
        
        if filename:
            _save_report_metadata(filename, question)
        
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
            import sys, os
            try:
                with JOBS_LOCK:
                    JOBS[jid]['status'] = 'running'
                crew = CyberAnalysisCrew()
                # Suppress interactive prompts
                old_stdin = sys.stdin
                try:
                    sys.stdin = open(os.devnull, 'r')
                    res = crew.kickoff(inputs={'question': q})
                finally:
                    sys.stdin = old_stdin
                
                # Extract filename from crew result if available
                # CrewAI returns a CrewOutput object with tasks_output list
                filename = None
                try:
                    if hasattr(res, 'tasks_output') and res.tasks_output:
                        last_task = res.tasks_output[-1]
                        if hasattr(last_task, 'output_file') and last_task.output_file:
                            filename = Path(last_task.output_file).name
                except Exception:
                    pass
                
                # Save metadata with the question
                if filename:
                    _save_report_metadata(filename, q)
                
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


def _load_metadata():
    """Load metadata list from JSON file, returning an empty list on error."""
    if not METADATA_FILE.exists():
        return []
    try:
        with METADATA_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _build_entry_from_file(path: Path):
    """Create a default metadata entry from a filesystem file."""
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
    except Exception:
        mtime = None

    display_name = _humanize_filename(path.name)
    entry = {
        'id': path.name,
        'fileName': path.name,
        'displayName': display_name,
        'name': display_name,  # backward-compat alias
        'description': 'AI-generated cyber security analysis report',
        'dateGenerated': mtime,
        'status': 'Completed'
    }
    return entry


def _merge_reports():
    """Merge filesystem reports with metadata."""
    reports = []
    metadata = _load_metadata()
    meta_by_file = {}
    meta_by_id = {}
    for m in metadata:
        if not isinstance(m, dict):
            continue
        fid = m.get('id') or m.get('fileName')
        fname = m.get('fileName')
        if fname:
            meta_by_file[fname] = m
        if fid:
            meta_by_id[fid] = m

    seen = set()
    if REPORTS_DIR.exists() and REPORTS_DIR.is_dir():
        for f in REPORTS_DIR.iterdir():
            if not f.is_file():
                continue
            base = _build_entry_from_file(f)
            meta = meta_by_file.get(f.name) or meta_by_id.get(f.name)
            if meta:
                merged = {**base, **meta}
                merged['id'] = meta.get('id') or base['id']
                merged['fileName'] = meta.get('fileName') or base['fileName']
                merged['displayName'] = merged.get('displayName') or merged.get('name') or base['displayName']
                merged['name'] = merged.get('displayName')
                merged['description'] = merged.get('description') or base['description']
                merged['dateGenerated'] = merged.get('dateGenerated') or base['dateGenerated']
                merged['status'] = merged.get('status') or base['status']
            else:
                merged = base

            reports.append(merged)
            seen.add(merged['id'])
            seen.add(merged['fileName'])

    # Include metadata entries that reference missing files, marked as missing
    for m in metadata:
        if not isinstance(m, dict):
            continue
        fid = m.get('id') or m.get('fileName')
        fname = m.get('fileName')
        if fid in seen or fname in seen:
            continue
        fid = fid or f"missing-{len(reports)}"
        display = m.get('displayName') or _humanize_filename(fname or fid or "Report")
        reports.append({
            'id': fid,
            'fileName': fname,
            'displayName': display,
            'name': display,
            'description': m.get('description') or 'Metadata present, file missing',
            'dateGenerated': m.get('dateGenerated'),
            'status': m.get('status') or 'Missing'
        })

    # Sort by dateGenerated descending (newest first)
    reports.sort(key=lambda r: r.get('dateGenerated') or '', reverse=True)
    return reports


@app.route('/api/reports', methods=['GET'])
def list_reports():
    """List available report files from the reports directory."""
    return jsonify({'reports': _merge_reports()})


@app.route('/api/reports/<report_id>', methods=['GET'])
def get_report(report_id):
    """Return metadata for a single report."""
    reports = _merge_reports()
    for r in reports:
        if r.get('id') == report_id:
            return jsonify({'report': r})
    return jsonify({'success': False, 'error': 'Report not found'}), 404


@app.route('/api/reports/<report_id>/download', methods=['GET'])
def download_report(report_id):
    """Download a specific report file by id (filename)."""
    # Map the id to the actual file name using metadata if present
    entry = None
    for r in _merge_reports():
        if r.get('id') == report_id:
            entry = r
            break

    file_name = entry.get('fileName') if entry else report_id
    # Prevent path traversal by resolving and ensuring directory containment
    target_path = (REPORTS_DIR / file_name).resolve()
    if not str(target_path).startswith(str(REPORTS_DIR)) or not target_path.is_file():
        return jsonify({'success': False, 'error': 'Report not found'}), 404

    return send_from_directory(REPORTS_DIR, target_path.name, as_attachment=True)

if __name__ == '__main__':
    ollama_host = os.getenv('OLLAMA_URL', 'http://localhost:11434')
    ollama_model = os.getenv('OLLAMA_MODEL', 'ollama/gpt-oss:120b-cloud')
    print("Starting Cyber Security Events Analysis API...")
    print(f"Using Ollama LLM at: {ollama_host}")
    print(f"Model: {ollama_model}")
    print("API will be available at http://localhost:5000")
    app.run(debug=True, port=5000, host='0.0.0.0')
