# Cyber Incident Analysis Crew
# CrewAI-based system for analyzing cyber security incident patterns and trends.

import os
from crewai import Agent, Crew, Process, Task, LLM
from crewai.project import CrewBase, agent, crew, task
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

from .tools.cyber_data_tools import (
    QueryCyberIncidentsTool,
    temporal_analysis,
    geographic_analysis,
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
        
        # Initialize LLM - using Ollama with configurable model
        # Deepseek has better reasoning capabilities than llama3.1
        ollama_host = os.getenv('OLLAMA_URL', 'http://localhost:11434')
        ollama_model = os.getenv('OLLAMA_MODEL', 'ollama/gpt-oss:120b-cloud')
        self.llm = LLM(
            model=ollama_model,  # CrewAI needs ollama/ prefix for LiteLLM routing
            base_url=ollama_host,
            api_key="no-key",  # Ollama doesn't need real key
            temperature=0.3,
            timeout=300  # 5 minute timeout for LLM calls (handles cold start)
        )
        
        # Database query tools - return pre-aggregated statistics
        self.analysis_tools = [
            QueryCyberIncidentsTool(),
            temporal_analysis,
            geographic_analysis,
            get_summary_statistics
        ]
    
    @agent
    def data_collector(self) -> Agent:
        # Database query specialist - ONLY calls tools
        return Agent(
            config=self.agents_config['data_collector'],
            llm=self.llm,
            tools=self.analysis_tools,  # HAS tools
            verbose=True,
            function_calling_llm=self.llm,
            max_iter=5,
            allow_delegation=False
        )
    
    @agent
    def data_analyst(self) -> Agent:
        # Statistical analyst - NO tools, just analyzes data
        return Agent(
            config=self.agents_config['data_analyst'],
            llm=self.llm,
            tools=[],  # NO tools - works with provided data
            verbose=True,
            max_iter=3,
            allow_delegation=False
        )
    
    @agent
    def report_writer(self) -> Agent:
        # Report author - NO tools, just writes
        return Agent(
            config=self.agents_config['report_writer'],
            llm=self.llm,
            tools=[],  # NO tools - works with analysis
            verbose=True,
            max_iter=3,
            allow_delegation=False
        )
    
    @task
    def data_collection_task(self) -> Task:
        # Step 1: Query database
        return Task(
            config=self.tasks_config['data_collection_task'],
            agent=self.data_collector()
        )
    
    @task
    def data_analysis_task(self) -> Task:
        # Step 2: Analyze the data
        return Task(
            config=self.tasks_config['data_analysis_task'],
            agent=self.data_analyst(),
            context=[self.data_collection_task()]  # Receives output from data_collection
        )
    
    @task
    def report_writing_task(self) -> Task:
        # Step 3: Write the report
        from datetime import datetime
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        out_name = f'reports/cyber_analysis_report_{ts}.md'
        return Task(
            config=self.tasks_config['report_writing_task'],
            agent=self.report_writer(),
            context=[self.data_analysis_task()],  # Receives output from analysis
            output_file=out_name
        )
    
    @crew
    def crew(self) -> Crew:
        # 3-agent pipeline: collect → analyze → write
        return Crew(
            agents=[self.data_collector(), self.data_analyst(), self.report_writer()],
            tasks=[self.data_collection_task(), self.data_analysis_task(), self.report_writing_task()],
            process=Process.sequential,
            verbose=True,
            share_crew=False,
            enable_rpm_tracking=False
        )
    
    def kickoff(self, inputs: dict):
        # Execute the crew analysis
        # Args:
        #     inputs: Dictionary containing 'question' - the analysis question to answer
        # Returns:
        #     Analysis results from the crew
        return self.crew().kickoff(inputs=inputs)
