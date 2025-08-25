import pandas as pd, joblib, mlflow, os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from pathlib import Path

feats_path = Path("/data/gold/clv_features")
files = list(feats_path.glob("*.parquet")) or list(feats_path.glob("*/*.parquet"))
if not files:
    raise SystemExit("No CLV features found; run Spark job first.")

df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
X = df.drop(columns=["customer_id","label"], errors="ignore")
y = df["label"] if "label" in df.columns else df["total_spent"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_tracking_uri("http://mlflow:5001")
mlflow.set_experiment("clv")

with mlflow.start_run():
    model = RandomForestRegressor(n_estimators=200, random_state=42)
    model.fit(X_train, y_train)
    r2 = model.score(X_test, y_test)

    mlflow.log_metric("r2", float(r2))
    mlflow.sklearn.log_model(model, "model")

    os.makedirs("/ml", exist_ok=True)
    joblib.dump(model, "/ml/clv_model.joblib")
    print("Model saved to /ml/clv_model.joblib, R2=", r2)