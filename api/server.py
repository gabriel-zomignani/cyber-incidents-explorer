"""
Flask API Server for Cyber Incidents Explorer
Connects the React frontend with the CrewAI backend
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os
from pathlib import Path

# Set Ollama host before importing CrewAI
os.environ['OLLAMA_HOST'] = 'http://localhost:11434'

# Add parent directory to path to import ai_engine
sys.path.append(str(Path(__file__).parent.parent))

from ai_engine.cyber import CyberAnalysisCrew

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'Cyber Incidents Analysis API'
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
        print(f"Analyzing question: {question}")
        crew = CyberAnalysisCrew()
        result = crew.kickoff(inputs={'question': question})
        
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

if __name__ == '__main__':
    print("Starting Cyber Incidents Analysis API...")
    print("Using Ollama LLM at: localhost:11434")
    print("Model: gpt-oss:120b-cloud")
    print("API will be available at http://localhost:5000")
    app.run(debug=True, port=5000, host='0.0.0.0')
