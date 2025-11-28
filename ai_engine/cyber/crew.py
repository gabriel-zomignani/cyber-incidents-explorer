# Cyber Incident Analysis Crew
# CrewAI-based system for analyzing cyber security incident patterns and trends.

import os
from crewai import Agent, Crew, Process, Task, LLM
from crewai.project import CrewBase, agent, crew, task
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from .tools.cyber_data_tools import (
    query_cyber_data,
    temporal_analysis,
    geographic_analysis,
    correlation_analysis,
    get_summary_statistics
)

load_dotenv()

@CrewBase
class CyberAnalysisCrew():
    # Cyber Incident Analysis Crew
    # These will be overridden in __init__
    agents_config = 'agents.yaml'
    tasks_config = 'tasks.yaml'

    def __init__(self) -> None:
        # Get the directory of this file
        import pathlib
        config_dir = pathlib.Path(__file__).parent
        
        # Load configs directly - don't use decorator paths
        import yaml
        with open(config_dir / 'agents.yaml', 'r') as f:
            self.agents_config = yaml.safe_load(f)
        with open(config_dir / 'tasks.yaml', 'r') as f:
            self.tasks_config = yaml.safe_load(f)
        
        # Initialize LLM - using Ollama with deepseek-r1:14b locally
        # Deepseek has better reasoning capabilities than llama3.1
        self.llm = LLM(
            model="ollama/gpt-oss:120b-cloud",  # CrewAI needs ollama/ prefix for LiteLLM routing
            base_url="http://localhost:11434",
            api_key="no-key",  # Ollama doesn't need real key
            temperature=0.7
        )
        
        # Database query tools for the analyst
        self.analysis_tools = [
            query_cyber_data,
            temporal_analysis,
            geographic_analysis,
            correlation_analysis,
            get_summary_statistics
        ]
    
    @agent
    def cyber_analyst(self) -> Agent:
        # Expert cybersecurity analyst with full database access
        return Agent(
            config=self.agents_config['cyber_analyst'],
            llm=self.llm,
            tools=self.analysis_tools,
            verbose=True
        )
    
    @task
    def analysis_task(self) -> Task:
        # Comprehensive analysis task
        return Task(
            config=self.tasks_config['analysis_task'],
            agent=self.cyber_analyst(),
            output_file='reports/cyber_analysis_report.md'
        )
    
    @crew
    def crew(self) -> Crew:
        # Assemble the streamlined crew with single expert agent
        return Crew(
            agents=[self.cyber_analyst()],
            tasks=[self.analysis_task()],
            process=Process.sequential,
            verbose=True
        )
    
    def kickoff(self, inputs: dict):
        # Execute the crew analysis
        # Args:
        #     inputs: Dictionary containing 'question' - the analysis question to answer
        # Returns:
        #     Analysis results from the crew
        return self.crew().kickoff(inputs=inputs)
