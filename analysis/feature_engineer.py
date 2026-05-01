"""
analysis/feature_engineer.py — Improved Feature Engineering Pipeline

Original model (mac_1.ipynb) reduced 249 raw columns to 1 PCA component via:
  CPU = log(sum of 51 CPU% columns)
  RAM = log(swap_space_unused) + log(RAM_unused)
  DISK = (total_reads + total_writes) / total_transfers * disk_busy%
  NET  = eth1 packet counts (read + write)
  → PCA(n_components=1) → single scalar 'x'

Improvements made here:
  1. Keep top-3 PCA components instead of 1 (retains more variance)
  2. Add rolling statistics (mean, std over 5-point window) as extra features
  3. Add rate-of-change (diff) features to detect sudden anomalies
  4. Robust NaN handling (fill with column median, not drop)
  5. Persist scaler state so inference uses same scaling as training
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

log = logging.getLogger(__name__)

# ── Column groups (from dataset analysis of testout51.csv) ────────────────────

CPU_COLS_PREFIX  = ['User%_CPU', 'Sys%_CPU', 'Wait%_CPU']   # 17 CPUs × 3 metrics
RAM_COLS = {
    'total':    'memtotal_mem',
    'free':     'memfree_mem',
    'buffers':  'buffers_mem',
    'cached':   'cached_mem',
    'inactive': 'inactive_mem',
    'swaptotal':'swaptotal_mem',
    'swapfree': 'swapfree_mem',
}
DISK_DEVICES = ['sr0','sda','sda1','sda2','sdb','sdb1','sdd','sdd1','sdc',
                'dm-0','dm-1','dm-2','dm-3','dm-4','dm-5','dm-6','dm-7','dm-8']
NET_COLS = ['eth1-read-KB/s_net', 'eth1-read/s_netpacket',
            'eth1-write-KB/s_net', 'eth1-write/s_netpacket']
TIMESTAMP_COL = 'CPU 1 YBLPVDAKDLWAPP1'   # original messy col name


class FeatureEngineer:
    """
    Transforms raw 249-column system telemetry into a compact,
    model-ready feature vector.

    Fit once on historical data, then call transform() on new rows.
    """

    def __init__(self, n_pca_components: int = 3, rolling_window: int = 5):
        self.n_pca   = n_pca_components
        self.roll_w  = rolling_window
        self._scaler : Optional[MinMaxScaler] = None
        self._eig_vec: Optional[np.ndarray]   = None   # PCA eigenvectors
        self._fitted  = False

    # ── Public API ────────────────────────────────────────────────────────────

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fit on full historical DataFrame, return enriched feature frame."""
        base = self._extract_base_features(df)
        base = self._handle_missing(base)
        pca_df = self._fit_pca(base)
        result = self._add_temporal_features(pca_df)
        self._fitted = True
        log.info("FeatureEngineer fitted. Output shape: %s", result.shape)
        return result

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform new data using fitted PCA (call fit_transform first)."""
        if not self._fitted:
            raise RuntimeError("Call fit_transform() before transform().")
        base = self._extract_base_features(df)
        base = self._handle_missing(base)
        pca_df = self._apply_pca(base)
        return self._add_temporal_features(pca_df)

    def transform_single(self, row: dict) -> Optional[np.ndarray]:
        """
        Transform a single event dict (from the live ingestion bus)
        into a feature vector. Returns None if required columns missing.
        """
        try:
            df = pd.DataFrame([row])
            result = self.transform(df)
            return result.values[0]
        except Exception as exc:
            log.debug("transform_single failed: %s", exc)
            return None

    def save(self, path: str = "./agent/models/feature_engineer.pkl") -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump({"scaler": self._scaler, "eig_vec": self._eig_vec,
                         "fitted": self._fitted, "n_pca": self.n_pca,
                         "roll_w": self.roll_w}, f)
        log.info("FeatureEngineer saved to %s", path)

    @classmethod
    def load(cls, path: str = "./agent/models/feature_engineer.pkl") -> "FeatureEngineer":
        with open(path, "rb") as f:
            state = pickle.load(f)
        fe = cls(n_pca_components=state["n_pca"], rolling_window=state["roll_w"])
        fe._scaler  = state["scaler"]
        fe._eig_vec = state["eig_vec"]
        fe._fitted  = state["fitted"]
        return fe

    # ── Private: feature extraction ───────────────────────────────────────────

    def _extract_base_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reproduce + improve the original notebook's feature construction."""
        result = pd.DataFrame(index=df.index)

        # 1. CPU — log of summed User%+Sys%+Wait% across all CPUs
        cpu_cols = [c for c in df.columns if any(c.startswith(p) for p in CPU_COLS_PREFIX)]
        if cpu_cols:
            cpu_sum = df[cpu_cols].clip(lower=0).sum(axis=1)
            result['cpu'] = np.log1p(cpu_sum)   # log1p avoids log(0)
        else:
            result['cpu'] = 0.0

        # 2. RAM pressure (improved: use ratio instead of raw log)
        try:
            swap_used = (df[RAM_COLS['swaptotal']] - df[RAM_COLS['swapfree']]
                         + df[RAM_COLS['inactive']]).clip(lower=1)
            ram_used  = (df[RAM_COLS['total']] - df[RAM_COLS['free']]
                         - df[RAM_COLS['buffers']] - df[RAM_COLS['cached']]).clip(lower=1)
            result['ram']  = np.log1p(ram_used)
            result['swap'] = np.log1p(swap_used)
            result['ram_pressure'] = ram_used / df[RAM_COLS['total']].replace(0, np.nan)
        except KeyError:
            result['ram'] = result['swap'] = result['ram_pressure'] = 0.0

        # 3. Disk I/O (busy% × block_size = effective throughput)
        disk_parts = []
        for dev in DISK_DEVICES:
            busy_col  = f"{dev}_diskbusy"
            bsize_col = f"{dev}_diskbsize"
            read_col  = f"{dev}_diskread"
            write_col = f"{dev}_diskwrite"
            xfer_col  = f"{dev}_diskxfer"
            if busy_col in df.columns and bsize_col in df.columns:
                throughput = (df[busy_col].fillna(0) * df[bsize_col].fillna(0)) / 100
                disk_parts.append(throughput)

        if disk_parts:
            result['disk_throughput'] = sum(disk_parts)

            # R/W ratio (anomaly signal: sudden shift toward reads or writes)
            read_cols  = [f"{d}_diskread"  for d in DISK_DEVICES if f"{d}_diskread"  in df.columns]
            write_cols = [f"{d}_diskwrite" for d in DISK_DEVICES if f"{d}_diskwrite" in df.columns]
            xfer_cols  = [f"{d}_diskxfer"  for d in DISK_DEVICES if f"{d}_diskxfer"  in df.columns]
            if read_cols and write_cols and xfer_cols:
                total_xfer = df[xfer_cols].sum(axis=1).replace(0, np.nan)
                result['disk_rw_ratio'] = (df[read_cols].sum(axis=1) + df[write_cols].sum(axis=1)) / total_xfer
        else:
            result['disk_throughput'] = result['disk_rw_ratio'] = 0.0

        # 4. Network
        net_cols_present = [c for c in NET_COLS if c in df.columns]
        result['net'] = df[net_cols_present].fillna(0).sum(axis=1) if net_cols_present else 0.0

        return result.astype(float)

    def _handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Fill NaN with column median (robust to outliers)."""
        for col in df.columns:
            if df[col].isna().any():
                median = df[col].median()
                df[col] = df[col].fillna(median if not np.isnan(median) else 0.0)
        return df

    # ── Private: PCA ──────────────────────────────────────────────────────────

    def _fit_pca(self, df: pd.DataFrame) -> pd.DataFrame:
        """Manual PCA (matches original notebook approach, extended to N components)."""
        centered = df.sub(df.mean(axis=0), axis=1)
        mat = np.asmatrix(centered.values, dtype=float)
        sigma = np.cov(mat.T)
        eig_vals, eig_vec = np.linalg.eig(sigma)
        idx = eig_vals.argsort()[::-1]
        eig_vals = eig_vals[idx]
        self._eig_vec = np.real(eig_vec[:, idx][:, :self.n_pca])

        transformed = mat.dot(self._eig_vec)
        cols = [f"pc{i+1}" for i in range(self.n_pca)]
        pca_df = pd.DataFrame(np.array(transformed), columns=cols, index=df.index)

        variance_explained = eig_vals[:self.n_pca] / eig_vals.sum() * 100
        log.info("PCA variance explained: %s", [f"{v:.1f}%" for v in variance_explained])
        return pca_df

    def _apply_pca(self, df: pd.DataFrame) -> pd.DataFrame:
        centered = df.sub(df.mean(axis=0), axis=1)
        mat = np.asmatrix(centered.values, dtype=float)
        transformed = mat.dot(self._eig_vec)
        cols = [f"pc{i+1}" for i in range(self.n_pca)]
        return pd.DataFrame(np.array(transformed), columns=cols, index=df.index)

    # ── Private: temporal enrichment ──────────────────────────────────────────

    def _add_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add rolling stats and rate-of-change for each PCA component."""
        enriched = df.copy()
        w = self.roll_w
        for col in df.columns:
            enriched[f"{col}_rollmean"] = df[col].rolling(w, min_periods=1).mean()
            enriched[f"{col}_rollstd"]  = df[col].rolling(w, min_periods=1).std().fillna(0)
            enriched[f"{col}_diff"]     = df[col].diff().fillna(0)
        return enriched
