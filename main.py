from fastapi import FastAPI, HTTPException
import pandas as pd
from pathlib import Path
import joblib

app = FastAPI(title="Enterprise Lakehouse API")

def load_parquet_dir(p: str) -> pd.DataFrame:
    path = Path(p)
    if not path.exists():
        return pd.DataFrame()
    files = sorted(path.glob("*.parquet")) or list(path.glob("*/*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(str(f)) for f in files], ignore_index=True)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics/daily_revenue")
def daily_revenue():
    df = load_parquet_dir("/data/gold/daily_revenue")
    if df.empty:
        return {"rows": 0, "data": []}
    df["order_date"] = df["order_date"].astype(str)
    return {"rows": len(df), "data": df.to_dict(orient="records")}

@app.get("/predict/clv/{customer_id}")
def predict_clv(customer_id: int):
    model_path = Path("/ml/clv_model.joblib")
    feats_path = Path("/data/gold/clv_features")
    if not model_path.exists():
        raise HTTPException(404, "Model not trained yet")
    if not feats_path.exists():
        raise HTTPException(404, "CLV features not found")
    model = joblib.load(model_path)
    feats = load_parquet_dir(str(feats_path))
    row = feats[feats["customer_id"] == customer_id]
    if row.empty:
        raise HTTPException(404, f"customer_id {customer_id} not found")
    X = row.drop(columns=["customer_id", "label"], errors="ignore")
    pred = float(model.predict(X)[0])
    return {"customer_id": int(customer_id), "predicted_clv": round(pred, 2)}