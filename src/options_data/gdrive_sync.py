"""Google Drive synchronization utilities using the `gdrive` CLI."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import threading
from pathlib import Path

from rich.console import Console

logger = logging.getLogger(__name__)
console = Console()

# Default Google Drive folder ID for cloud archive sync (configured via GDRIVE_FOLDER_ID in .env)
DEFAULT_GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
_ID_PATTERN = re.compile(r"\(ID:\s*([A-Za-z0-9_-]+)\)")


def get_gdrive_base_cmd() -> tuple[list[str], dict[str, str]]:
    """Resolve the gdrive CLI for the current user.

    Antigravity installs ``gdrive`` under the user's home. Dynamic path resolution
    ensures sync works across different operating systems and user accounts.
    """
    env = os.environ.copy()
    antigravity_bin = Path.home() / ".gemini" / "antigravity" / "bin"
    env["PATH"] = os.pathsep.join([str(antigravity_bin), env.get("PATH", "")])

    server_bins = sorted(Path.home().glob(".antigravity-server/bin/*/language_server"))
    gdrive_in_antigravity = antigravity_bin / "gdrive"
    if server_bins and gdrive_in_antigravity.exists():
        return [str(server_bins[-1]), "plugin-cli", "gdrive"], env

    gdrive_path = shutil.which("gdrive", path=env["PATH"]) or str(gdrive_in_antigravity)
    return [gdrive_path], env


class GoogleDriveSync:
    """Uploads and updates Parquet files in Google Drive via the `gdrive` CLI."""

    def __init__(self, root_folder_id: str = DEFAULT_GDRIVE_FOLDER_ID) -> None:
        self.root_folder_id = root_folder_id
        self._folder_cache: dict[tuple[str, str], str] = {}
        self._lock = threading.Lock()

    def _run(self, args: list[str]) -> str:
        """Run a `gdrive` command and return stdout."""
        base_cmd, env = get_gdrive_base_cmd()
        cmd = base_cmd + args
        res = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
        if res.returncode != 0:
            err_msg = (res.stderr or res.stdout).strip()
            raise RuntimeError(f"gdrive command failed ({' '.join(args)}): {err_msg}")
        return res.stdout.strip()

    def list_children(self, folder_id: str) -> list[dict[str, str]]:
        """List all direct children (files and folders) of a Drive folder."""
        out = self._run(["readonly", "ls", folder_id, "--max", "0", "--json"])
        if not out:
            return []
        data = json.loads(out)
        return data if isinstance(data, list) else []

    def ensure_subfolder(self, parent_id: str, name: str) -> str:
        """Find or create a subfolder named `name` inside `parent_id` and return its folder ID."""
        with self._lock:
            cache_key = (parent_id, name)
            if cache_key in self._folder_cache:
                return self._folder_cache[cache_key]

            for item in self.list_children(parent_id):
                if item.get("name") == name and item.get("mimeType") == FOLDER_MIME_TYPE:
                    folder_id = item["id"]
                    self._folder_cache[cache_key] = folder_id
                    return folder_id

            out = self._run(["mutate", "mkdir", name, "--parent", parent_id])
            m = _ID_PATTERN.search(out)
            if m:
                folder_id = m.group(1)
                self._folder_cache[cache_key] = folder_id
                return folder_id

            # Fallback: re-list parent folder
            for item in self.list_children(parent_id):
                if item.get("name") == name and item.get("mimeType") == FOLDER_MIME_TYPE:
                    folder_id = item["id"]
                    self._folder_cache[cache_key] = folder_id
                    return folder_id

            raise RuntimeError(f"Failed to resolve folder ID after creating subfolder '{name}' in {parent_id}")

    def ensure_path(self, relative_parts: list[str], base_folder_id: str | None = None) -> str:
        """Ensure a nested folder path (e.g. `['options_eod', 'symbol=SPY']`) exists in Drive."""
        curr_id = base_folder_id or self.root_folder_id
        for part in relative_parts:
            curr_id = self.ensure_subfolder(curr_id, part)
        return curr_id

    def upload_or_update_file(
        self,
        local_path: str | Path,
        parent_id: str,
        *,
        mime_type: str = "application/vnd.apache.parquet",
    ) -> str:
        """Upload `local_path` into `parent_id`, or update in-place if a file with the same name exists."""
        with self._lock:
            path = Path(local_path)
            if not path.is_file():
                raise FileNotFoundError(f"Local file does not exist: {path}")

            existing_id: str | None = None
            for item in self.list_children(parent_id):
                if item.get("name") == path.name and item.get("mimeType") != FOLDER_MIME_TYPE:
                    existing_id = item["id"]
                    break

            if existing_id:
                out = self._run(
                    ["mutate", "update", existing_id, str(path), "--mime-type", mime_type]
                )
                return existing_id
            else:
                out = self._run(
                    ["mutate", "upload", str(path), "--parent", parent_id, "--mime-type", mime_type]
                )
                m = _ID_PATTERN.search(out)
                return m.group(1) if m else out

    def upload_underlying_parquet(self, local_path: Path) -> str:
        """Upload an underlying EOD Parquet file to `<root>/underlying_eod/<filename>`."""
        folder_id = self.ensure_path(["underlying_eod"])
        file_id = self.upload_or_update_file(local_path, folder_id)
        console.print(
            f"  [bold blue]☁ Google Drive[/bold blue]: Uploaded [bold]{local_path.name}[/bold] "
            f"-> [cyan]Options Strategy/thetadata_parquet/underlying_eod/[/cyan] (ID: {file_id})"
        )
        return file_id

    def upload_yearly_options_parquet(self, symbol: str, local_path: Path) -> str:
        """Upload a consolidated yearly options Parquet file to `<root>/options_eod/symbol=<SYM>/<filename>`."""
        sym = symbol.upper()
        folder_id = self.ensure_path(["options_eod", f"symbol={sym}"])
        file_id = self.upload_or_update_file(local_path, folder_id)
        console.print(
            f"  [bold blue]☁ Google Drive[/bold blue]: Uploaded [bold]{local_path.name}[/bold] "
            f"-> [cyan]Options Strategy/thetadata_parquet/options_eod/symbol={sym}/[/cyan] (ID: {file_id})"
        )
        return file_id

    def find_subfolder(self, parent_id: str, name: str) -> str | None:
        """Find an existing subfolder named `name` inside `parent_id` without creating it (readonly)."""
        with self._lock:
            cache_key = (parent_id, name)
            if cache_key in self._folder_cache:
                return self._folder_cache[cache_key]

            for item in self.list_children(parent_id):
                if item.get("name") == name and item.get("mimeType") == FOLDER_MIME_TYPE:
                    folder_id = item["id"]
                    self._folder_cache[cache_key] = folder_id
                    return folder_id
            return None

    def find_path(self, relative_parts: list[str], base_folder_id: str | None = None) -> str | None:
        """Resolve an existing nested folder path in Drive without creating folders (readonly)."""
        curr_id: str | None = base_folder_id or self.root_folder_id
        if not curr_id:
            return None
        for part in relative_parts:
            curr_id = self.find_subfolder(curr_id, part)
            if curr_id is None:
                return None
        return curr_id

    def download_symbol_parquets(
        self,
        symbol: str,
        data_dir: str | Path = "data",
        *,
        years: list[int] | None = None,
    ) -> list[Path]:
        """On-demand download of a symbol's consolidated yearly Parquet files from Google Drive into local cache."""
        sym = symbol.upper()
        target_dir = Path(data_dir) / "processed" / "options_eod" / f"symbol={sym}"
        target_dir.mkdir(parents=True, exist_ok=True)

        folder_id = self.find_path(["options_eod", f"symbol={sym}"])
        if not folder_id:
            return sorted(target_dir.glob("*.parquet"))

        children = self.list_children(folder_id)
        downloaded: list[Path] = []
        for item in children:
            name = item.get("name", "")
            if not name.endswith(".parquet") or item.get("mimeType") == FOLDER_MIME_TYPE:
                continue
            if years is not None:
                matched_year = False
                for yr in years:
                    if name == f"{sym}_{yr}.parquet":
                        matched_year = True
                        break
                if not matched_year:
                    continue

            dest = target_dir / name
            if dest.exists() and dest.stat().st_size > 0:
                downloaded.append(dest)
                continue

            tmp_dest = target_dir / f".{name}.tmp"
            self._run(["readonly", "download", item["id"], "--out", str(tmp_dest)])
            if tmp_dest.exists() and tmp_dest.stat().st_size > 0:
                tmp_dest.replace(dest)
                downloaded.append(dest)
                console.print(
                    f"  [bold green]☁ Google Drive Cache[/bold green]: Pulled [bold]{name}[/bold] -> {dest}"
                )
        return sorted(target_dir.glob("*.parquet"))

    def upload_daily_options_parquet(self, symbol: str, year: int, local_path: Path) -> str:
        """Upload a single daily options Parquet file to `<root>/options_eod_raw/symbol=<SYM>/year=<YYYY>/<filename>`."""
        sym = symbol.upper()
        folder_id = self.ensure_path(["options_eod_raw", f"symbol={sym}", f"year={year}"])
        return self.upload_or_update_file(local_path, folder_id)
