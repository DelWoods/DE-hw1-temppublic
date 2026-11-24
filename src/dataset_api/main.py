"""
Dataset REST API
================
Reads the local csv dump once (only raw + numeric columns) and serves it page by page.
"""

import os
import json
import logging

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

from utils import load_source_config
from logger import setup_logger


setup_logger()
logger = logging.getLogger(__name__)

cfg = load_source_config(os.getenv('SOURCE_CONFIG', '/app/source_config.json'))
app = FastAPI(title="Toxic messages dataset API", version="1.0.0")


def load_dataset(config: dict) -> pd.DataFrame:
    """Read full dump if it exists, otherwise the small committed sample"""

    path = config['csv_path']
    if not os.path.exists(path):
        logger.warning(f'{path} not found, using sample {config["sample_csv_path"]}')
        path = config['sample_csv_path']

    logger.info(f'Reading {path} ...')
    # text_* derived columns are skipped => much less memory: 
    df = pd.read_csv(path, usecols=config['columns'], encoding='utf-8')
    df = df[config['columns']]
    logger.info(f'Dataset loaded: {df.shape}')

    return df


DF = load_dataset(cfg)


@app.get("/health")
def health():
    return {"status": "ok", "rows": int(len(DF))}


@app.get("/schema")
def schema():
    """column -> pandas dtype"""
    return {"total": int(len(DF)), "columns": {c: str(t) for c, t in DF.dtypes.items()}}


@app.get("/messages")
def messages(
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=1000, ge=1),
):
    """Paginated rows: {total, offset, limit, items}"""

    if limit > cfg['max_page_size']:
        raise HTTPException(status_code=400, detail=f"limit must be <= {cfg['max_page_size']}")

    page = DF.iloc[offset: offset + limit]
    # NaN -> null for json: 
    items = json.loads(page.to_json(orient='records', force_ascii=False))

    return {"total": int(len(DF)), "offset": offset, "limit": limit, "items": items}
