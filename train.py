"""
agent/train.py — Improved Model Training Script

Run this once to retrain on your dataset (testout51.csv → mac_1.csv format).
Improvements over original mac_1.ipynb:

  1. 3 PCA components instead of 1
  2. EarlyStopping + ModelCheckpoint — stops before overfitting
  3. ReduceLROnPlateau — adaptive learning rate
  4. Bidirectional LSTM option — sees future context during training
  5. Cross-validation split instead of random shuffle (preserves time order)
  6. Saves model in TF2 format (.keras) + legacy H5 for compatibility
  7. Prints RMSE, MAE, and MAPE metrics

Usage:
    python -m agent.train --data testout51.csv --output ./agent/models/
    python -m agent.train --data testout51.csv --epochs 100 --bidirectional
"""

from __future__ import annotations

import argparse
import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def load_and_engineer(csv_path: str) -> tuple[pd.DataFrame, pd.Series]:
    """Load raw CSV and run feature engineering pipeline."""
    from analysis.feature_engineer import FeatureEngineer

    log.info("Loading dataset: %s", csv_path)
    df = pd.read_csv(csv_path)

    # Timestamp column — handle either column name variant
    ts_col = next((c for c in df.columns if 'CPU' in c and ('YBPL' in c or 'YBLP' in c)), None)
    if ts_col is None:
        raise ValueError("Cannot find timestamp column in dataset.")

    timestamps = pd.to_datetime(df[ts_col])

    fe = FeatureEngineer(n_pca_components=3, rolling_window=5)
    features = fe.fit_transform(df)
    fe.save("./agent/models/feature_engineer.pkl")

    # Target: first PCA component (matches original model target)
    target = features['pc1']

    log.info("Features: %s | Target: %s rows", features.shape, len(target))
    return features, target, timestamps


def build_sequences(values: np.ndarray, window: int = 50) -> tuple[np.ndarray, np.ndarray]:
    """Sliding window → (X, y) sequences for LSTM."""
    X, y = [], []
    for i in range(len(values) - window):
        X.append(values[i:i + window])
        y.append(values[i + window])
    return np.array(X), np.array(y)


def build_model(window: int = 50, bidirectional: bool = False):
    """Improved architecture with configurable bidirectionality."""
    import tensorflow as tf
    from tensorflow.keras.models import Sequential
    from tensorflow.keras.layers import (
        LSTM, Dense, Dropout, Bidirectional, Input
    )

    model = Sequential([
        Input(shape=(window, 1)),

        # Layer 1: return sequences for stacking
        (Bidirectional(LSTM(50, return_sequences=True, dropout=0.2, recurrent_dropout=0.1))
         if bidirectional
         else LSTM(50, return_sequences=True, dropout=0.2, recurrent_dropout=0.1)),

        Dropout(0.3),

        # Layer 2: summary layer (256 units, matches original)
        (Bidirectional(LSTM(256, dropout=0.2))
         if bidirectional
         else LSTM(256, dropout=0.2)),

        Dropout(0.3),
        Dense(64, activation='relu'),   # NEW: extra dense layer
        Dense(1, activation='linear'),
    ])

    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=1e-3),
        loss='mse',
        metrics=['mae'],
    )
    model.summary()
    return model


def train(
    csv_path   : str,
    output_dir : str = "./agent/models/",
    window     : int = 50,
    epochs     : int = 50,
    batch_size : int = 20,
    bidir      : bool = False,
):
    import tensorflow as tf
    from sklearn.preprocessing import MinMaxScaler
    from sklearn.metrics import mean_squared_error, mean_absolute_error

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    features, target, timestamps = load_and_engineer(csv_path)

    # Scale target to [-1, 1] (matches original)
    scaler = MinMaxScaler(feature_range=(-1, 1))
    scaled = scaler.fit_transform(target.values.reshape(-1, 1)).flatten()

    # Build sequences
    X, y = build_sequences(scaled, window)
    X = X.reshape(X.shape[0], X.shape[1], 1)

    # Time-series split (NOT random shuffle — preserves temporal order)
    split = int(0.75 * len(X))
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    log.info("Train: %d samples | Test: %d samples", len(X_train), len(X_test))

    model = build_model(window=window, bidirectional=bidir)

    callbacks = [
        tf.keras.callbacks.EarlyStopping(
            monitor='val_loss', patience=8, restore_best_weights=True, verbose=1
        ),
        tf.keras.callbacks.ReduceLROnPlateau(
            monitor='val_loss', factor=0.5, patience=4, min_lr=1e-6, verbose=1
        ),
        tf.keras.callbacks.ModelCheckpoint(
            str(Path(output_dir) / "model_best.keras"),
            monitor='val_loss', save_best_only=True, verbose=0
        ),
    ]

    log.info("Training for up to %d epochs…", epochs)
    history = model.fit(
        X_train, y_train,
        batch_size     = batch_size,
        epochs         = epochs,
        validation_split = 0.1,
        callbacks      = callbacks,
        verbose        = 1,
    )

    # ── Evaluate ──────────────────────────────────────────────────────────────
    preds_scaled = model.predict(X_test, verbose=0)
    preds_real   = scaler.inverse_transform(preds_scaled)
    actuals_real = scaler.inverse_transform(y_test.reshape(-1, 1))

    rmse = np.sqrt(mean_squared_error(actuals_real, preds_real))
    mae  = mean_absolute_error(actuals_real, preds_real)
    mape = np.mean(np.abs((actuals_real - preds_real) / (actuals_real + 1e-6))) * 100

    log.info("=" * 50)
    log.info("TEST RESULTS:")
    log.info("  RMSE : %.4f", rmse)
    log.info("  MAE  : %.4f", mae)
    log.info("  MAPE : %.2f%%", mape)
    log.info("=" * 50)

    # ── Save ──────────────────────────────────────────────────────────────────
    # Modern format (.keras)
    model.save(str(Path(output_dir) / "model1.keras"))
    # Legacy weights format for compatibility
    model.save_weights(str(Path(output_dir) / "model1.weights.h5"))
    
    # Save model architecture as YAML (if supported)
    try:
        model_yaml = model.to_yaml()
        (Path(output_dir) / "model1.yaml").write_text(model_yaml)
    except AttributeError:
        # TF2 doesn't have to_yaml(), save as JSON instead
        import json
        model_config = model.to_json()
        (Path(output_dir) / "model1.json").write_text(model_config)

    import pickle
    with open(str(Path(output_dir) / "scaler.pkl"), "wb") as f:
        pickle.dump(scaler, f)

    log.info("Model saved to %s", output_dir)
    log.info("Best epoch stopped at %d", len(history.history['loss']))

    return {"rmse": rmse, "mae": mae, "mape": mape}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train improved LSTM failure predictor")
    parser.add_argument("--data",         default="testout51.csv",    help="Raw CSV dataset path")
    parser.add_argument("--output",       default="./agent/models/",  help="Model output directory")
    parser.add_argument("--window",       type=int,   default=50,     help="LSTM input window size")
    parser.add_argument("--epochs",       type=int,   default=50,     help="Max training epochs")
    parser.add_argument("--batch-size",   type=int,   default=20,     help="Batch size")
    parser.add_argument("--bidirectional",action="store_true",        help="Use BiLSTM (better accuracy, slower)")
    args = parser.parse_args()

    train(
        csv_path   = args.data,
        output_dir = args.output,
        window     = args.window,
        epochs     = args.epochs,
        batch_size = args.batch_size,
        bidir      = args.bidirectional,
    )
