import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from db import run_query
from client import generate_llm

ML_SYSTEM_PROMPT = """
You are the ML Agent for the YouTube Medallion Data Engineering Pipeline.
Your role is to classify if the user wants one of the following statistical/predictive tasks:
1. "forecast": Numerical time-series forecasting of views or likes.
2. "anomaly": Outlier or anomaly detection in daily views or likes.
3. "decline": Evaluating whether a channel or category is declining in popularity/views.
4. "unknown": Any other prediction or unsupported model query.

Your response must be a JSON object with this schema:
{
  "task": "forecast" | "anomaly" | "decline" | "unknown",
  "target_metric": "views" | "likes",
  "entity": string or null
}

Reply ONLY with the JSON object. Do not include markdown code block formatting.
"""

def detect_ml_task(user_message: str) -> dict:
    """Classifies the predictive task and details using Gemini."""
    raw_response = generate_llm(
        system_prompt=ML_SYSTEM_PROMPT,
        user_message=f"Question: \"{user_message}\""
    )
    cleaned = raw_response.strip().strip("`").replace("json\n", "", 1).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        # Fallback
        import json
        try:
            return json.loads(cleaned)
        except Exception:
            return {"task": "unknown", "target_metric": "views", "entity": None}

def run_forecast(metric: str, entity: str = None) -> dict:
    """Fits an on-the-fly linear trend regression and projects next 30 days with confidence intervals."""
    # 1. Fetch historical series
    metric_col = "total_views" if metric == "views" else "total_likes"
    if entity:
        # If entity is specified, fetch daily stats for that channel
        query = f"""
            SELECT f.trending_date, SUM(f.{metric}) as val
            FROM gold.fact_video_metrics f
            JOIN gold.dim_channel c ON f.dim_channel_key = c.dim_channel_key
            WHERE c.channel_title ILIKE '%{entity}%'
            GROUP BY f.trending_date
            ORDER BY f.trending_date ASC
        """
    else:
        # Otherwise, fetch overall daily KPIs
        query = f"""
            SELECT trending_date, {metric_col} as val
            FROM gold.trending_kpi_summary_mart
            ORDER BY trending_date ASC
        """
    
    try:
        res = run_query(query)
        rows = res.get("rows", [])
        if len(rows) < 5:
            return {
                "status": "error",
                "error": "Insufficient historical data points (minimum 5 required) to fit forecasting model."
            }
        
        df = pd.DataFrame(rows)
        df["trending_date"] = pd.to_datetime(df["trending_date"])
        df = df.sort_values("trending_date")
        
        # 2. Fit simple linear regression on index
        df["day_index"] = np.arange(len(df))
        x = df["day_index"].values
        y = df["val"].values.astype(float)
        
        slope, intercept = np.polyfit(x, y, 1)
        residuals = y - (slope * x + intercept)
        std_res = np.std(residuals) if len(residuals) > 1 else 0
        
        # 3. Forecast next 30 days
        last_date = df["trending_date"].max()
        forecast_rows = []
        for i in range(1, 31):
            future_day_idx = len(df) + i
            future_date = last_date + timedelta(days=i)
            pred = slope * future_day_idx + intercept
            # Clip negative predictions at 0
            pred = max(0.0, pred)
            lower = max(0.0, pred - 1.96 * std_res)
            upper = pred + 1.96 * std_res
            
            forecast_rows.append({
                "date": future_date.strftime("%Y-%m-%d"),
                "predicted": round(pred, 2),
                "lower_bound": round(lower, 2),
                "upper_bound": round(upper, 2)
            })
            
        return {
            "status": "success",
            "metric": metric,
            "entity": entity,
            "historical_mean": round(float(np.mean(y)), 2),
            "historical_slope": round(float(slope), 2),
            "forecast": forecast_rows
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def run_anomaly_detection(metric: str) -> dict:
    """Finds daily anomalies in views/likes using standard deviation thresholding."""
    metric_col = "total_views" if metric == "views" else "total_likes"
    query = f"""
        SELECT trending_date, {metric_col} as val
        FROM gold.trending_kpi_summary_mart
        ORDER BY trending_date ASC
    """
    try:
        res = run_query(query)
        rows = res.get("rows", [])
        if not rows:
            return {"status": "error", "error": "No daily KPI metrics found."}
        
        df = pd.DataFrame(rows)
        y = df["val"].values.astype(float)
        mean = np.mean(y)
        std = np.std(y)
        
        anomalies = []
        for index, row in df.iterrows():
            val = float(row["val"])
            z_score = (val - mean) / std if std > 0 else 0
            if abs(z_score) > 2.0:
                anomalies.append({
                    "date": str(row["trending_date"]),
                    "value": int(val),
                    "z_score": round(z_score, 2),
                    "type": "Spike" if z_score > 0 else "Drop"
                })
                
        return {
            "status": "success",
            "metric": metric,
            "mean": round(mean, 2),
            "std": round(std, 2),
            "anomalies": anomalies
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def run_decline_analysis(entity: str) -> dict:
    """Evaluates whether a channel has a negative trend slope, indicating performance decline."""
    if not entity:
        return {"status": "error", "error": "Decline analysis requires a specific channel_title."}
        
    query = f"""
        SELECT f.trending_date, SUM(f.views) as val
        FROM gold.fact_video_metrics f
        JOIN gold.dim_channel c ON f.dim_channel_key = c.dim_channel_key
        WHERE c.channel_title ILIKE '%{entity}%'
        GROUP BY f.trending_date
        ORDER BY f.trending_date ASC
    """
    try:
        res = run_query(query)
        rows = res.get("rows", [])
        if len(rows) < 3:
            return {
                "status": "error",
                "error": f"Insufficient historical data (found {len(rows)} days) to compute popularity decline for channel '{entity}'."
            }
            
        df = pd.DataFrame(rows)
        y = df["val"].values.astype(float)
        x = np.arange(len(y))
        slope, _ = np.polyfit(x, y, 1)
        
        decline_likelihood = "Low"
        if slope < 0:
            pct_decline = abs(slope) / np.mean(y)
            if pct_decline > 0.05:
                decline_likelihood = "High"
            else:
                decline_likelihood = "Medium"
                
        return {
            "status": "success",
            "entity": entity,
            "total_views_history": [int(v) for v in y],
            "slope": round(float(slope), 2),
            "decline_likelihood": decline_likelihood
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def handle_ml_query(user_message: str) -> dict:
    """Orchestrates ML analytical tasks based on prompt classification."""
    task_info = detect_ml_task(user_message)
    task = task_info.get("task", "unknown")
    metric = task_info.get("target_metric", "views")
    entity = task_info.get("entity")
    
    if task == "forecast":
        res = run_forecast(metric, entity)
        if res.get("status") == "success":
            intro = f"Successfully generated a 30-day linear projection for {entity or 'overall'} {metric}."
            return {"result": intro, "raw_data": res, "status": "success"}
        else:
            return {"result": f"Forecasting failed: {res.get('error')}", "raw_data": res, "status": "error"}
            
    elif task == "anomaly":
        res = run_anomaly_detection(metric)
        if res.get("status") == "success":
            intro = f"Completed outlier detection check on daily {metric}."
            return {"result": intro, "raw_data": res, "status": "success"}
        else:
            return {"result": f"Anomaly detection failed: {res.get('error')}", "raw_data": res, "status": "error"}
            
    elif task == "decline":
        res = run_decline_analysis(entity)
        if res.get("status") == "success":
            intro = f"Analyzed popularity trend and decline likelihood for channel: {entity}."
            return {"result": intro, "raw_data": res, "status": "success"}
        else:
            return {"result": f"Decline analysis failed: {res.get('error')}", "raw_data": res, "status": "error"}
            
    else:
        # Graceful fallback response
        response_text = (
            "I do not have a pre-trained model for that query. Currently, I support:\n"
            "1. **Trend Forecasts**: 'Forecast views for 5-Minute Crafts' or 'predict daily likes trend'.\n"
            "2. **Anomaly Detection**: 'Check for daily anomalies in views' or 'find spikes in likes'.\n"
            "3. **Decline Likelihood**: 'Check if views for Cocomelon are declining'."
        )
        return {
            "result": response_text,
            "raw_data": {"supported_tasks": ["forecast", "anomaly", "decline"]},
            "status": "graceful_fallback"
        }
