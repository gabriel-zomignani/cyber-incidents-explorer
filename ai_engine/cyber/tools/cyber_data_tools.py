# Database query tools for cyber incident analysis
# Tools return PRE-AGGREGATED statistics - LLM interprets and writes reports

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
from crewai.tools import BaseTool, tool
from typing import Optional, Type, Annotated
from pydantic import BaseModel, Field


DB_PATH = Path(__file__).parent.parent.parent.parent / "db" / "cyber.db"
DB_URL = f"sqlite:///{DB_PATH}"


def _load_filtered_data(year=0, month=0, country="", event_type="", actor_type="", region="", start_date="", end_date=""):
    """Load filtered data from database."""
    engine = create_engine(DB_URL)
    
    query = "SELECT * FROM cyber_events WHERE 1=1"
    params = {}
    
    if start_date:
        query += " AND event_date >= :start_date"
        params['start_date'] = start_date
    
    if end_date:
        query += " AND event_date <= :end_date"
        params['end_date'] = end_date
    
    if year != 0:
        query += " AND year = :year"
        params['year'] = year
    
    if month != 0:
        query += " AND month = :month"
        params['month'] = month
    
    if country:
        query += " AND LOWER(country) LIKE LOWER(:country)"
        params['country'] = f"%{country}%"
    
    if event_type:
        query += " AND LOWER(event_type) LIKE LOWER(:event_type)"
        params['event_type'] = f"%{event_type}%"
    
    if actor_type:
        query += " AND LOWER(actor_type) LIKE LOWER(:actor_type)"
        params['actor_type'] = f"%{actor_type}%"
    
    if region:
        region_col = region.lower()
        query += f" AND {region_col} = 1"
    
    df = pd.read_sql(query, engine, params=params)
    df.columns = df.columns.str.lower().str.strip()
    
    # Convert event_date to datetime
    if 'event_date' in df.columns:
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    
    return df


class QueryCyberIncidentsInput(BaseModel):
    """Input for QueryCyberIncidents tool."""
    year: int = 0
    month: int = 0
    country: str = ""
    event_type: str = ""
    actor_type: str = ""
    region: str = ""
    start_date: str = ""
    end_date: str = ""


class QueryCyberIncidentsTool(BaseTool):
    name: str = "Query Cyber Incidents"
    description: str = "Get aggregated statistics for cyber incidents with optional filters. Returns pre-computed counts, distributions, and top categories."
    args_schema: Type[BaseModel] = QueryCyberIncidentsInput

    def _run(self, year: int = 0, month: int = 0, country: str = "", event_type: str = "", 
             actor_type: str = "", region: str = "", start_date: str = "", end_date: str = "") -> str:
        try:
            df = _load_filtered_data(year, month, country, event_type, actor_type, region, start_date, end_date)
            
            if df.empty:
                return "No incidents found with the specified filters."
            
            # Build filter description
            filters = []
            if year != 0:
                filters.append(f"year={year}")
            if month != 0:
                filters.append(f"month={month}")
            if start_date:
                filters.append(f"from={start_date}")
            if end_date:
                filters.append(f"to={end_date}")
            if country:
                filters.append(f"country~{country}")
            if event_type:
                filters.append(f"event_type~{event_type}")
            if actor_type:
                filters.append(f"actor_type~{actor_type}")
            if region:
                filters.append(f"region={region}")
            
            result = f"### Query Results\n"
            result += f"Filters: {', '.join(filters) if filters else 'none'}\n"
            result += f"**Total Incidents: {len(df)}**\n\n"
            
            # Top countries
            if 'country' in df.columns:
                top_countries = df['country'].value_counts().head(10)
                result += "**Top 10 Countries:**\n"
                for country, count in top_countries.items():
                    pct = (count / len(df)) * 100
                    result += f"- {country}: {count} ({pct:.1f}%)\n"
                result += "\n"
            
            # Event types distribution
            if 'event_type' in df.columns:
                event_types = df['event_type'].value_counts()
                result += "**Event Types:**\n"
                for etype, count in event_types.items():
                    pct = (count / len(df)) * 100
                    result += f"- {etype}: {count} ({pct:.1f}%)\n"
                result += "\n"
            
            # Actor types distribution
            if 'actor_type' in df.columns:
                actor_types = df['actor_type'].value_counts()
                result += "**Actor Types:**\n"
                for atype, count in actor_types.items():
                    pct = (count / len(df)) * 100
                    result += f"- {atype}: {count} ({pct:.1f}%)\n"
                result += "\n"
            
            # Top actors
            if 'actor' in df.columns:
                top_actors = df['actor'].value_counts().head(10)
                result += "**Top 10 Actors:**\n"
                for actor, count in top_actors.items():
                    result += f"- {actor}: {count}\n"
                result += "\n"
            
            return result
            
        except Exception as e:
            return f"Error querying data: {str(e)}"


@tool("Temporal Analysis")
def temporal_analysis(
    start_year: Annotated[int, Field(default=0)] = 0,
    end_year: Annotated[int, Field(default=0)] = 0,
    region: Annotated[str, Field(default="")] = "",
    start_date: Annotated[str, Field(default="")] = "",
    end_date: Annotated[str, Field(default="")] = ""
) -> str:
    """Get temporal trends and year-over-year statistics."""
    try:
        df = _load_filtered_data(region=region, start_date=start_date, end_date=end_date)
        
        if df.empty:
            return "No data available."
        
        if start_year > 0:
            df = df[df['year'] >= start_year]
        if end_year > 0:
            df = df[df['year'] <= end_year]
        
        result = f"### Temporal Analysis\n"
        if region:
            result += f"Region: {region.upper()}\n"
        result += f"**Total Incidents: {len(df)}**\n\n"
        
        # Year-by-year counts
        yearly = df.groupby('year').size().sort_index()
        result += "**Incidents by Year:**\n"
        prev_count = None
        for year, count in yearly.items():
            if prev_count:
                growth = ((count - prev_count) / prev_count) * 100
                result += f"- {int(year)}: {count} ({growth:+.1f}% from previous year)\n"
            else:
                result += f"- {int(year)}: {count}\n"
            prev_count = count
        result += "\n"
        
        # Monthly distribution
        if 'month' in df.columns:
            monthly = df.groupby('month').size().sort_index()
            result += "**Average Distribution by Month:**\n"
            for month, count in monthly.items():
                result += f"- Month {int(month)}: {count} incidents\n"
            result += "\n"
        
        return result
        
    except Exception as e:
        return f"Error in temporal analysis: {str(e)}"


@tool("Geographic Analysis")
def geographic_analysis(
    year: Annotated[int, Field(default=0)] = 0,
    month: Annotated[int, Field(default=0)] = 0,
    region: Annotated[str, Field(default="")] = "",
    start_date: Annotated[str, Field(default="")] = "",
    end_date: Annotated[str, Field(default="")] = ""
) -> str:
    """Get geographic distribution and regional statistics."""
    try:
        df = _load_filtered_data(year=year, month=month, region=region, start_date=start_date, end_date=end_date)
        
        if df.empty:
            return "No data available."
        
        result = f"### Geographic Analysis\n"
        if year:
            result += f"Year: {year}\n"
        if month:
            result += f"Month: {month}\n"
        if region:
            result += f"Region: {region.upper()}\n"
        result += f"**Total Incidents: {len(df)}**\n\n"
        
        # Top countries
        country_counts = df['country'].value_counts().head(20)
        result += "**Top 20 Countries:**\n"
        for country, count in country_counts.items():
            pct = (count / len(df)) * 100
            result += f"- {country}: {count} ({pct:.1f}%)\n"
        result += "\n"
        
        # Regional distribution
        regions = ['nato', 'eu', 'g7', 'g20', 'five_eyes', 'asean', 'au', 'oecd']
        result += "**Regional Distribution:**\n"
        for reg in regions:
            if reg in df.columns:
                count = df[df[reg] == 1].shape[0]
                if count > 0:
                    pct = (count / len(df)) * 100
                    result += f"- {reg.upper()}: {count} ({pct:.1f}%)\n"
        result += "\n"
        
        return result
        
    except Exception as e:
        return f"Error in geographic analysis: {str(e)}"


@tool("Get Summary Statistics")
def get_summary_statistics() -> str:
    """Get overall dataset statistics and high-level summary."""
    try:
        df = _load_filtered_data()
        
        result = f"### Dataset Summary\n"
        result += f"**Total Incidents: {len(df)}**\n\n"
        
        # Date range
        if 'year' in df.columns:
            result += f"**Year Range:** {int(df['year'].min())} - {int(df['year'].max())}\n\n"
        
        # Top countries
        result += "**Top 10 Countries:**\n"
        for country, count in df['country'].value_counts().head(10).items():
            result += f"- {country}: {count}\n"
        result += "\n"
        
        # Event types
        result += "**Event Types:**\n"
        for etype, count in df['event_type'].value_counts().items():
            result += f"- {etype}: {count}\n"
        result += "\n"
        
        # Actor types
        result += "**Actor Types:**\n"
        for atype, count in df['actor_type'].value_counts().items():
            result += f"- {atype}: {count}\n"
        result += "\n"
        
        return result
        
    except Exception as e:
        return f"Error getting summary: {str(e)}"
