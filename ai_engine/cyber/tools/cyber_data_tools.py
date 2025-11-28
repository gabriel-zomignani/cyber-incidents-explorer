"""
Database query tools for cyber incident analysis
These tools allow CrewAI agents to query and analyze the cyber incidents data.
"""

from pathlib import Path
from typing import Optional
import pandas as pd
from crewai_tools import tool

# Path to the cleaned cyber incidents CSV
CSV_PATH = Path(__file__).parent.parent.parent.parent / "outputs" / "cyber_clean.csv"


def _load_data():
    """Load the cyber incidents dataset"""
    try:
        df = pd.read_csv(CSV_PATH, low_memory=False)
        # Ensure lowercase column names for consistency
        df.columns = df.columns.str.lower().str.strip()
        return df
    except Exception as e:
        return f"Error loading data: {str(e)}"


# ---------------------------------------------------------------------------
# 1) MAIN QUERY TOOL: **STRUCTURED FILTERS**
# ---------------------------------------------------------------------------

@tool("Query Cyber Incident Data")
def query_cyber_data(
    year: int,
    country: str,
    event_type: str,
    actor_type: str,
    region: str,
) -> str:
    """
    Query the cyber incidents dataset with structured filters.

    ALL 5 parameters are required. Pass empty string "" for any filter you don't want to use.

    Example: {"year": 2023, "country": "", "event_type": "", "actor_type": "", "region": "eu"}
    Example: {"year": 2023, "country": "Ukraine", "event_type": "", "actor_type": "", "region": ""}

    Args:
        year: Filter by incident year (e.g. 2023) or 0 to not filter by year.
        country: Filter by country (exact match) or "" to not filter.
        event_type: Filter by event_type (e.g. 'Exploitive') or "" to not filter.
        actor_type: Filter by actor_type (e.g. 'Criminal') or "" to not filter.
        region: Filter by region/group (e.g. 'eu', 'nato', 'g7', 'g20', 'five_eyes', 'oecd', 'asean', etc.) or "" to not filter.
                Available regions: eu, nato, g7, g20, five_eyes, shanghai_coop, oas, mercosur, au, ecowas, asean, opec, gulf_coop, aukus, csto, oecd, osce

    Returns:
        A concise, CSV-grounded summary string.
    """
    df = _load_data()
    if isinstance(df, str):
        # Error loading data
        return df

    filtered = df.copy()
    filters = []

    # Year filter (skip if year is 0 or None)
    if year is not None and year != 0 and "year" in filtered.columns:
        filtered = filtered[filtered["year"] == year]
        filters.append(f"year={year}")

    # Region filter (EU, NATO, G7, etc.)
    if region is not None and str(region).strip() != "" and str(region).lower() in filtered.columns:
        region_col = str(region).lower()
        filtered = filtered[filtered[region_col] == 1]
        filters.append(f"region='{region}'")

    # Country filter
    if country is not None and str(country).strip() != "" and "country" in filtered.columns:
        filtered = filtered[filtered["country"].str.lower() == str(country).lower()]
        filters.append(f"country='{country}'")

    # Event type filter
    if event_type is not None and str(event_type).strip() != "" and "event_type" in filtered.columns:
        filtered = filtered[filtered["event_type"].str.lower() == str(event_type).lower()]
        filters.append(f"event_type='{event_type}'")

    # Actor type filter
    if actor_type is not None and str(actor_type).strip() != "" and "actor_type" in filtered.columns:
        filtered = filtered[filtered["actor_type"].str.lower() == str(actor_type).lower()]
        filters.append(f"actor_type='{actor_type}'")

    if filtered.empty:
        return (
            "No incidents found with the given filters.\n"
            f"Filters: {', '.join(filters) if filters else 'none'}"
        )

    total = len(filtered)

    by_year = (
        filtered.groupby("year").size().sort_index().to_string()
        if "year" in filtered.columns
        else "Year column not available."
    )

    by_event_type = (
        filtered["event_type"].value_counts().head(5).to_string()
        if "event_type" in filtered.columns
        else "event_type column not available."
    )

    by_actor_type = (
        filtered["actor_type"].value_counts().head(5).to_string()
        if "actor_type" in filtered.columns
        else "actor_type column not available."
    )

    by_country = (
        filtered["country"].value_counts().head(5).to_string()
        if "country" in filtered.columns
        else "country column not available."
    )

    return f"""
Query results based on the CSV:
- Filters: {', '.join(filters) if filters else 'none'}
- Matching incidents: {total}

Incidents by year:
{by_year}

Top event types:
{by_event_type}

Top actor types:
{by_actor_type}

Top countries in this subset:
{by_country}
""".strip()


# ---------------------------------------------------------------------------
# 2) TEMPORAL ANALYSIS
# ---------------------------------------------------------------------------

@tool("Temporal Analysis")
def temporal_analysis(start_year: int = None, end_year: int = None) -> str:
    """
    Analyze temporal patterns in cyber incidents.

    Args:
        start_year: Starting year for analysis (optional).
        end_year: Ending year for analysis (optional).

    Returns:
        String report of temporal patterns.
    """
    df = _load_data()
    if isinstance(df, str):
        return df

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["year"] = df["event_date"].dt.year
    df["month"] = df["event_date"].dt.month

    if start_year is not None:
        df = df[df["year"] >= start_year]
    if end_year is not None:
        df = df[df["year"] <= end_year]

    if df.empty:
        return (
            "No incidents found for the given time range.\n"
            f"start_year={start_year}, end_year={end_year}"
        )

    # Yearly trends
    yearly = df.groupby("year").size().to_string()

    # Monthly patterns
    monthly = df.groupby("month").size().to_string()

    # Year-over-year by attack type
    yearly_by_type = df.groupby(["year", "event_type"]).size().unstack(fill_value=0)

    yoy_growth = df.groupby("year").size().pct_change().mul(100).round(1).to_string()

    report = f"""
Temporal Analysis Report:

Filters:
- start_year={start_year}
- end_year={end_year}

Incidents by Year:
{yearly}

Incidents by Month (all years in range):
{monthly}

Top Attack Types by Year:
{yearly_by_type.to_string()}

Year-over-Year Growth (%):
{yoy_growth}
"""
    return report.strip()


# ---------------------------------------------------------------------------
# 3) GEOGRAPHIC ANALYSIS
# ---------------------------------------------------------------------------

@tool("Geographic Analysis")
def geographic_analysis(country: str = None, region: str = None) -> str:
    """
    Analyze geographic patterns and distributions in cyber incidents.

    Args:
        country: Specific country to analyze (optional).
        region: Specific region to analyze (optional) — not currently used, placeholder.

    Returns:
        String report of geographic patterns.
    """
    df = _load_data()
    if isinstance(df, str):
        return df

    if country:
        df_filtered = df[df["country"].str.lower() == country.lower()]
        if df_filtered.empty:
            return f"No incidents found for country '{country}'."

        country_report = f"""
Geographic Analysis for {country}:

- Total Incidents: {len(df_filtered)}

Top Attack Types:
{df_filtered['event_type'].value_counts().head().to_string()}

Top Actor Types:
{df_filtered['actor_type'].value_counts().head().to_string()}

Top Target Industries:
{df_filtered['industry'].value_counts().head().to_string()}
"""
        return country_report.strip()

    # General geographic analysis
    top_countries = df["country"].value_counts().head(10)

    top_country_list = top_countries.index.tolist()[:5]
    attack_by_country = (
        df[df["country"].isin(top_country_list)]
        .groupby(["country", "event_type"])
        .size()
        .unstack(fill_value=0)
    )

    nato_stats = (
        df.groupby("nato")["event_type"].value_counts().unstack(fill_value=0)
        if "nato" in df.columns
        else "NATO data not available"
    )

    report = f"""
Geographic Analysis Report (Global):

Top 10 Countries by Incident Count:
{top_countries.to_string()}

Attack Types by Top 5 Countries:
{attack_by_country.to_string()}

NATO Membership Analysis:
{nato_stats if isinstance(nato_stats, str) else nato_stats.to_string()}
"""
    return report.strip()


# ---------------------------------------------------------------------------
# 4) CORRELATION ANALYSIS
# ---------------------------------------------------------------------------

@tool("Correlation Analysis")
def correlation_analysis(field1: str, field2: str) -> str:
    """
    Analyze correlations between two fields in the cyber incidents dataset.

    Args:
        field1: First field to correlate (e.g., 'actor_type', 'motive', 'industry').
        field2: Second field to correlate (e.g., 'event_type', 'country').

    Returns:
        String report showing correlations between the two fields.
    """
    df = _load_data()
    if isinstance(df, str):
        return df

    field1 = field1.lower()
    field2 = field2.lower()

    if field1 not in df.columns:
        return f"Error: Field '{field1}' not found in dataset. Available fields: {', '.join(df.columns)}"
    if field2 not in df.columns:
        return f"Error: Field '{field2}' not found in dataset. Available fields: {', '.join(df.columns)}"

    crosstab = pd.crosstab(df[field1], df[field2], margins=True)

    report = f"""
Correlation Analysis: {field1} vs {field2}

Cross-tabulation (counts):
{crosstab.to_string()}

Top 5 most common combinations:
{df.groupby([field1, field2]).size().sort_values(ascending=False).head().to_string()}
"""
    return report.strip()


# ---------------------------------------------------------------------------
# 5) SUMMARY STATISTICS
# ---------------------------------------------------------------------------

@tool("Get Summary Statistics")
def get_summary_statistics() -> str:
    """
    Get comprehensive summary statistics of the cyber incidents dataset.

    Returns:
        String with detailed summary statistics.
    """
    df = _load_data()
    if isinstance(df, str):
        return df

    valid_dates = df["event_date"].dropna()
    if len(valid_dates) > 0:
        try:
            date_range = f"{valid_dates.min()} to {valid_dates.max()}"
        except Exception:
            date_range = "Date range unavailable"
    else:
        date_range = "No valid dates"

    report = f"""
Cyber Incidents Database - Summary Statistics

Dataset Overview:
- Total Records: {len(df):,}
- Date Range: {date_range}
- Countries: {df['country'].nunique()}
- Unique Actors: {df['actor'].nunique() if 'actor' in df.columns else 'N/A'}

Attack Types Distribution:
{df['event_type'].value_counts().to_string()}

Actor Types Distribution:
{df['actor_type'].value_counts().to_string()}

Motive Distribution:
{df['motive'].value_counts().to_string() if 'motive' in df.columns else 'N/A'}

Top 10 Targeted Industries:
{df['industry'].value_counts().head(10).to_string() if 'industry' in df.columns else 'N/A'}

Missing Data Summary:
{df.isnull().sum().to_string()}
"""
    return report.strip()
