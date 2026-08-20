import argparse
import sys
import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Adjust Python path to load config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pipeline_config.config import get_db_engine, log_audit, XML_FILE_PATH

def generate_sample_xml(filepath):
    """Generates a sample XML file with channel metadata if it does not exist."""
    print(f"Generating sample XML at: {filepath}")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    root = ET.Element("channels")
    
    # Mock data corresponding to prominent channels in USvideos.csv
    channels_data = [
        {"id": "UCqgUR0M9T3yFkwm1P-J8pww", "title": "CaseyNeistat", "subscribers": "12500000", "country": "US", "last_updated": "2026-08-19T00:00:00"},
        {"id": "UC3XTzVzaHQEd30rQbuvAQLQ", "title": "LastWeekTonight", "subscribers": "9200000", "country": "US", "last_updated": "2026-08-18T12:00:00"},
        {"id": "UC-9-kyTW8ZkZNDHQJ6FgpwQ", "title": "Rudy Mancuso", "subscribers": "7500000", "country": "US", "last_updated": "2026-08-17T09:00:00"},
        {"id": "UCi1O33A4sVn20A415s6XNtw", "title": "Good Mythical Morning", "subscribers": "18200000", "country": "US", "last_updated": "2026-08-19T06:00:00"},
        {"id": "UC0C-w0YjGpqDXGB8IHb66AV", "title": "YouTube Spotlight", "subscribers": "35000000", "country": "US", "last_updated": "2026-08-19T00:00:00"}
    ]
    
    for c in channels_data:
        chan_elem = ET.SubElement(root, "channel")
        ET.SubElement(chan_elem, "channel_id").text = c["id"]
        ET.SubElement(chan_elem, "channel_title").text = c["title"]
        ET.SubElement(chan_elem, "subscriber_count").text = c["subscribers"]
        ET.SubElement(chan_elem, "country").text = c["country"]
        ET.SubElement(chan_elem, "last_updated").text = c["last_updated"]
        
    tree = ET.ElementTree(root)
    tree.write(filepath, encoding='utf-8', xml_declaration=True)

def ingest_xml(run_id):
    task_name = "bronze_ingestion"
    step_name = "ingest_xml"
    
    print(f"Starting XML ingestion from {XML_FILE_PATH}")
    log_audit(run_id, task_name, step_name, "STARTED", 0, f"Reading XML: {XML_FILE_PATH}")
    
    if not os.path.exists(XML_FILE_PATH):
        try:
            generate_sample_xml(XML_FILE_PATH)
        except Exception as e:
            err_msg = f"Failed to generate sample XML: {str(e)}"
            log_audit(run_id, task_name, step_name, "FAILED", 0, err_msg)
            raise e
            
    try:
        # Parse XML
        tree = ET.parse(XML_FILE_PATH)
        root = tree.getroot()
        
        records = []
        for channel in root.findall('channel'):
            chan_id = channel.find('channel_id').text
            title = channel.find('channel_title').text
            subs = channel.find('subscriber_count').text
            country = channel.find('country').text
            last_upd = channel.find('last_updated').text
            
            records.append({
                "channel_id": chan_id,
                "channel_title": title,
                "subscriber_count": subs,
                "country": country,
                "last_updated": last_upd,
                "source_system": "channels_xml",
                "load_timestamp": datetime.now()
            })
            
        df = pd.DataFrame(records)
        
        engine = get_db_engine()
        # Idempotency: truncate table before load
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE bronze.raw_channels")
            
        df.to_sql(
            name='raw_channels',
            con=engine,
            schema='bronze',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        log_audit(run_id, task_name, step_name, "COMPLETED", len(df), f"Successfully ingested {len(df)} rows from XML")
        print(f"XML Ingestion complete: {len(df)} rows written.")
        return len(df)
    except Exception as e:
        log_audit(run_id, task_name, step_name, "FAILED", 0, f"Error: {str(e)}")
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    ingest_xml(args.run_id)
