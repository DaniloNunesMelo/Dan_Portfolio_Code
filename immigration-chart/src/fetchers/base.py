from abc import ABC, abstractmethod
from pathlib import Path
import hashlib
import json
from datetime import datetime, timezone

import pandas as pd

CACHE_DIR = Path(__file__).parents[2] / "data" / "cache"
_memory: dict[str, tuple[pd.DataFrame, datetime]] = {}


class FetchError(Exception):
    pass


class DataUnavailableError(Exception):
    pass


class BaseFetcher(ABC):
    TTL_HOURS: int = 24

    def _cache_key(self, **params) -> str:
        raw = json.dumps(params, sort_keys=True)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _load_disk(self, key: str) -> pd.DataFrame | None:
        meta_path = CACHE_DIR / f"{key}.meta.json"
        parq_path = CACHE_DIR / f"{key}.parquet"
        if not meta_path.exists() or not parq_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text())
            fetched = datetime.fromisoformat(meta["fetched_at"])
            age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
            if age_hours > self.TTL_HOURS:
                return None
            return pd.read_parquet(parq_path)
        except Exception:
            return None

    def _save_disk(self, key: str, df: pd.DataFrame) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        parq_path = CACHE_DIR / f"{key}.parquet"
        meta_path = CACHE_DIR / f"{key}.meta.json"
        df.to_parquet(parq_path, index=False)
        meta_path.write_text(
            json.dumps(
                {
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "ttl_hours": self.TTL_HOURS,
                }
            )
        )

    def fetch(self, **params) -> tuple[pd.DataFrame, str]:
        key = self._cache_key(**params)
        # Memory cache
        if key in _memory:
            df, ts = _memory[key]
            age = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age < self.TTL_HOURS:
                return df, "memory_cache"
        # Disk cache
        df = self._load_disk(key)
        if df is not None:
            _memory[key] = (df, datetime.now(timezone.utc))
            return df, "disk_cache"
        # Live fetch
        try:
            df = self._fetch_live(**params)
            self._save_disk(key, df)
            _memory[key] = (df, datetime.now(timezone.utc))
            return df, "live"
        except Exception as e:
            raise FetchError(str(e)) from e

    @abstractmethod
    def _fetch_live(self, **params) -> pd.DataFrame: ...

    def clear_cache(self, **params) -> None:
        key = self._cache_key(**params)
        _memory.pop(key, None)
        for ext in [".parquet", ".meta.json"]:
            p = CACHE_DIR / f"{key}{ext}"
            if p.exists():
                p.unlink()
