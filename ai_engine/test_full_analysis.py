"""
Full CrewAI analysis test - tests the complete pipeline
"""
import sys
import os
from pathlib import Path

# Set Ollama host BEFORE importing any LiteLLM/CrewAI modules
os.environ['OLLAMA_HOST'] = 'http://localhost:11434'

sys.path.insert(0, str(Path(__file__).parent.parent))

# Force reload to pick up config changes
import importlib
if 'ai_engine.cyber' in sys.modules:
    importlib.reload(sys.modules['ai_engine.cyber'])

from ai_engine.cyber import CyberAnalysisCrew

def test_full_analysis():
    """Test complete analysis with a simple question"""
    print("=" * 60)
    print("Full CrewAI Analysis Test")
    print("=" * 60)
    
    question = "How many cyber attacks occurred in NATO countries in 2016?"
    
    print(f"\nQuestion: {question}")
    print("\nInitializing crew and running analysis...")
    print("(This will take 1-2 minutes)\n")
    
    try:
        crew = CyberAnalysisCrew()
        result = crew.kickoff(inputs={'question': question})
        
        print("\n" + "=" * 60)
        print("ANALYSIS RESULT")
        print("=" * 60)
        print(result)
        print("\n✅ Full analysis completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_full_analysis()
