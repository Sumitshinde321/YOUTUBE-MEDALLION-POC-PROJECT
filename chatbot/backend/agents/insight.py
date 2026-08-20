from client import generate_llm

INSIGHT_SYSTEM_PROMPT = """
You are the Insight Agent for the YouTube Creator Studio Analytics dashboard.
Your role is to translate raw metrics, video counts, subscriber counts, linear forecasting, and pipeline validation warning flags into high-value, clear, and actionable insights.

Focus on:
1. Writing in a professional, encouraging, and clear tone—similar to YouTube Creator Studio's performance insights.
2. Explaining the "so-what" (what the trends or anomalies mean for creator strategies or pipeline health).
3. Keeping it concise (2-4 sentences) with impeccable grammar.
4. Grounding all insights strictly in the provided numbers and data. Do not make up any statistics.
5. Highlighting any operational or quality flags as clear warnings.
"""

def perform_validation_checks(raw_data: dict) -> list:
    """Action-Against Layer: programmatically checks inputs against limits/thresholds."""
    flags = []
    
    # 1. Check pipeline duration and status (Support Agent logs)
    if "duration_seconds" in str(raw_data):
        # Scan raw data list of dicts
        for item in raw_data.get("raw_data", []) or []:
            duration = item.get("duration_seconds")
            if duration and float(duration) > 300.0:
                flags.append(f"WARNING: Pipeline run duration ({duration}s) exceeds warning threshold (300s).")
            status = item.get("status")
            if status == "FAILED":
                flags.append(f"CRITICAL: Pipeline run failed for run {item.get('run_id')}.")

    # 2. Check record rejection rates
    if "records_rejected" in str(raw_data):
        for item in raw_data.get("raw_data", []) or []:
            inserted = item.get("records_inserted", 0) or 0
            rejected = item.get("records_rejected", 0) or 0
            total = inserted + rejected
            if total > 0:
                rate = (rejected / total) * 100
                if rate > 10.0:
                    flags.append(f"CRITICAL: High data quality rejection rate ({rate:.1f}%) in run {item.get('run_id')}.")

    # 3. Check day-over-day changes (trends mart)
    if "pct_change_day_over_day" in str(raw_data):
        for item in raw_data.get("rows", []) or raw_data.get("raw_data", {}).get("rows", []):
            pct = item.get("pct_change_day_over_day")
            if pct:
                if float(pct) > 50.0 or float(pct) < -50.0:
                    flags.append(f"WARNING: Daily KPI deviation of {pct}% detected on {item.get('trending_date')}.")

    # 4. Check ML anomalies
    if "anomalies" in str(raw_data):
        anoms = raw_data.get("raw_data", {}).get("anomalies", [])
        if anoms:
            for anom in anoms:
                flags.append(f"WARNING: ML Anomaly detected on {anom.get('date')} - {anom.get('type')} (z-score: {anom.get('z_score')}).")

    # 5. Check if channel decline slope is very steep
    if "decline_likelihood" in str(raw_data):
        decline = raw_data.get("raw_data", {})
        if decline.get("decline_likelihood") == "High":
            flags.append(f"WARNING: High decline likelihood detected for channel {decline.get('entity')} (Slope: {decline.get('slope')}).")

    return flags

def generate_business_insight(agent_outputs: dict) -> dict:
    """Generates the business insight and flags any warning thresholds violated."""
    # Run validation checks
    flags = perform_validation_checks(agent_outputs)
    
    # Prompt the LLM to synthesize the results along with any flags
    prompt_message = (
        f"Raw Outputs from specialized agents:\n{agent_outputs}\n\n"
        f"Validation Warning Flags:\n{flags}\n\n"
        f"Please synthesize the business insight."
    )
    
    insight_text = generate_llm(
        system_prompt=INSIGHT_SYSTEM_PROMPT,
        user_message=prompt_message
    )
    
    # Prepend warning flags if present
    if flags:
        warning_str = "### ⚠️ Operational & Quality Alerts\n"
        for flag in flags:
            warning_str += f"- **{flag}**\n"
        warning_str += "\n"
        insight_text = warning_str + insight_text
        
    return {
        "insight": insight_text,
        "flags": flags
    }
