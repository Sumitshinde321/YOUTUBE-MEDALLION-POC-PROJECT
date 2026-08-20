import os
import sys
import asyncio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List, Optional, Any

# Ensure backend and agents paths are in sys.path
backend_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(backend_dir)
sys.path.append(os.path.join(backend_dir, "agents"))

from agents.router import route_question
from agents.support import handle_support_query
from agents.data import handle_data_query
from agents.ml import handle_ml_query
from agents.insight import generate_business_insight
from agents.report import build_report_payload

app = FastAPI(title="Data Engineering Pipeline AI Chatbot API")

# Configure CORS explicitly to prevent browser fetch blocks (including local server ports)
cors_origins_env = os.getenv("CORS_ORIGINS", "*")
origins = [o.strip() for o in cors_origins_env.split(",")]

# If allowing all origins, credentials cannot be True
allow_credentials = True
if "*" in origins:
    allow_credentials = False

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Models
class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = "default"

class RouteRequest(BaseModel):
    message: str

class AgentRequest(BaseModel):
    message: str
    parameters: Optional[dict] = None

class InsightRequest(BaseModel):
    raw_data: dict

class ReportRequest(BaseModel):
    message: str
    agent_outputs: dict
    insight: dict

# Specialized Agent Endpoints
@app.post("/router")
async def api_router(req: RouteRequest):
    result = await asyncio.to_thread(route_question, req.message)
    return result

@app.post("/support")
async def api_support(req: AgentRequest):
    result = await asyncio.to_thread(handle_support_query, req.message)
    return result

@app.post("/data")
async def api_data(req: AgentRequest):
    result = await asyncio.to_thread(handle_data_query, req.message)
    return result

@app.post("/ml")
async def api_ml(req: AgentRequest):
    result = await asyncio.to_thread(handle_ml_query, req.message)
    return result

@app.post("/insight")
async def api_insight(req: InsightRequest):
    result = await asyncio.to_thread(generate_business_insight, req.raw_data)
    return result

@app.post("/report")
async def api_report(req: ReportRequest):
    result = await asyncio.to_thread(build_report_payload, req.message, req.agent_outputs, req.insight)
    return result

# Chat Orchestration Endpoint
@app.post("/chat")
async def api_chat(req: ChatRequest):
    # 1. Classify intents using the Router Agent
    routing = await asyncio.to_thread(route_question, req.message)
    intents = routing.get("intents", ["data"])
    
    # 2. Call specialized agents in parallel based on classification
    tasks = []
    if "support" in intents:
        tasks.append(asyncio.to_thread(handle_support_query, req.message))
    if "data" in intents:
        tasks.append(asyncio.to_thread(handle_data_query, req.message))
    if "ml" in intents:
        tasks.append(asyncio.to_thread(handle_ml_query, req.message))
        
    if not tasks:
        # Default to data agent fallback if no intent is classified
        tasks.append(asyncio.to_thread(handle_data_query, req.message))
        
    agent_results = await asyncio.gather(*tasks)
    
    # 3. Merge agent outputs
    merged_output = {}
    combined_result_text = []
    
    for r in agent_results:
        if r.get("result"):
            combined_result_text.append(r["result"])
        for k, v in r.items():
            if k != "result":
                if k not in merged_output:
                    merged_output[k] = v
                else:
                    if isinstance(merged_output[k], list) and isinstance(v, list):
                        merged_output[k].extend(v)
                    elif isinstance(merged_output[k], dict) and isinstance(v, dict):
                        merged_output[k].update(v)
                        
    merged_output["result"] = "\n\n".join(combined_result_text)
    
    # 4. Synthesize business insight via the Insight Agent (with threshold checks)
    insight_res = await asyncio.to_thread(generate_business_insight, merged_output)
    
    # 5. Format final structured response using the Report Agent
    report_res = await asyncio.to_thread(
        build_report_payload, 
        req.message, 
        merged_output, 
        insight_res
    )
    
    return report_res

@app.get("/health")
def health():
    return {"status": "ok", "database": "connected"}

# Serve the HTML frontend
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    ui_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 
        "frontend", 
        "index.html"
    )
    if not os.path.exists(ui_path):
        return HTMLResponse("<h2>index.html UI file not found</h2>", status_code=404)
    with open(ui_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content, status_code=200)

# Serve generated reports statically
reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
os.makedirs(reports_dir, exist_ok=True)
app.mount("/reports", StaticFiles(directory=reports_dir), name="reports")
