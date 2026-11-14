#!/usr/bin/env python3
"""
Automated Screenshot Generator for Options Dashboard
Uses Chrome DevTools Protocol (CDP) to capture high-res screenshots of all dashboard tabs.
"""

import asyncio
import base64
import json
import os
import subprocess
import time
import urllib.request
import websockets

SCREENSHOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "images"))
CHROME_BIN = "/mnt/c/Program Files/Google/Chrome/Application/chrome.exe"
CDP_PORT = 9222
APP_URL = "http://localhost:8050/"

os.makedirs(SCREENSHOT_DIR, exist_ok=True)

class CDPClient:
    def __init__(self, ws_url):
        self.ws_url = ws_url
        self.ws = None
        self.req_id = 0

    async def connect(self):
        self.ws = await websockets.connect(self.ws_url, max_size=100 * 1024 * 1024)

    async def call(self, method, params=None):
        self.req_id += 1
        cid = self.req_id
        payload = {"id": cid, "method": method, "params": params or {}}
        await self.ws.send(json.dumps(payload))
        while True:
            resp_raw = await self.ws.recv()
            resp = json.loads(resp_raw)
            if resp.get("id") == cid:
                if "error" in resp:
                    raise RuntimeError(f"CDP Error in {method}: {resp['error']}")
                return resp.get("result", {})

    async def eval_js(self, expression, await_promise=True):
        res = await self.call("Runtime.evaluate", {
            "expression": expression,
            "awaitPromise": await_promise,
            "returnByValue": True
        })
        return res.get("result", {}).get("value")

    async def screenshot(self, filepath):
        res = await self.call("Page.captureScreenshot", {"format": "png"})
        img_data = base64.b64decode(res["data"])
        with open(filepath, "wb") as f:
            f.write(img_data)
        print(f"Captured: {filepath} ({len(img_data) // 1024} KB)")

    async def close(self):
        if self.ws:
            await self.ws.close()


async def main():
    subprocess.run(["taskkill.exe", "/F", "/IM", "chrome.exe"], capture_output=True)
    time.sleep(0.5)

    print(f"Launching headless Chrome connecting to {APP_URL}...")
    chrome_proc = subprocess.Popen([
        CHROME_BIN,
        "--headless=new",
        f"--remote-debugging-port={CDP_PORT}",
        "--disable-gpu",
        "--hide-scrollbars",
        "--window-size=1440,940",
        r'--user-data-dir=C:\Users\Public\chrome_profile_tmp',
        APP_URL
    ])

    try:
        # Wait for CDP endpoint to respond
        ws_url = None
        for _ in range(20):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{CDP_PORT}/json") as resp:
                    tabs = json.loads(resp.read().decode())
                    for tab in tabs:
                        if tab.get("type") == "page":
                            ws_url = tab.get("webSocketDebuggerUrl")
                            break
                    if ws_url:
                        break
            except Exception:
                pass
            time.sleep(0.5)

        if not ws_url:
            raise RuntimeError("Failed to obtain Chrome WebSocket Debugger URL")

        print(f"Connected to CDP: {ws_url}")
        client = CDPClient(ws_url)
        await client.connect()

        # Enable Page and Runtime
        await client.call("Page.enable")
        await client.call("Runtime.enable")

        # Ensure we are freshly navigated to Option Chain Tab
        print("Navigating to Option Chain tab...")
        await client.call("Page.navigate", {"url": APP_URL})
        await asyncio.sleep(1.0)
        await client.eval_js("switchTab('chain')")

        # 1. Wait for Option Chain data to finish loading
        print("Waiting for Option Chain to load...")
        for _ in range(30):
            has_chain = await client.eval_js(
                "document.querySelectorAll('#chain-table-body tr').length > 0 && !document.getElementById('status-indicator').innerText.includes('Loading')"
            )
            if has_chain:
                break
            await asyncio.sleep(0.5)

        # Ensure top view is visible (header, toolbar, KPIs, carousel, dual-sided table)
        await client.eval_js("window.scrollTo(0, 0);")
        await asyncio.sleep(1.0)
        img1 = os.path.join(SCREENSHOT_DIR, "daily_option_chain.png")
        await client.screenshot(img1)

        # 2. Click Call button for the ATM strike to view Contract Lifecycle
        print("Navigating to Contract Lifecycle...")
        await client.eval_js("""
            (() => {
                const atmRow = document.querySelector('.strike-atm')?.parentElement;
                if (atmRow) {
                    const btn = atmRow.querySelector('button.btn-action-view');
                    if (btn) { btn.click(); return; }
                }
                const anyBtn = document.querySelector('#chain-table-body button.btn-action-view');
                if (anyBtn) anyBtn.click();
            })()
        """)

        # Wait for Plotly chart to render
        for _ in range(30):
            has_chart = await client.eval_js(
                "document.querySelectorAll('#view-lifecycle .plotly').length >= 3 && !document.getElementById('status-indicator').innerText.includes('Loading')"
            )
            if has_chart:
                break
            await asyncio.sleep(0.5)

        await client.eval_js("window.scrollTo(0, 0);")
        await asyncio.sleep(1.5)
        img2 = os.path.join(SCREENSHOT_DIR, "contract_lifecycle.png")
        await client.screenshot(img2)

        # 3. Switch to IV Smile Tab
        print("Navigating to IV Smile & Greeks Cross-Section...")
        await client.eval_js("switchTab('smile')")

        for _ in range(30):
            has_smile = await client.eval_js(
                "document.querySelectorAll('#view-smile .plotly').length >= 2 && !document.getElementById('status-indicator').innerText.includes('Loading')"
            )
            if has_smile:
                break
            await asyncio.sleep(0.5)

        await client.eval_js("window.scrollTo(0, 0);")
        await asyncio.sleep(1.5)
        img3 = os.path.join(SCREENSHOT_DIR, "iv_smile.png")
        await client.screenshot(img3)

        # 4. Switch to Raw Historical Data Table Tab
        print("Navigating to Raw Data Table...")
        await client.eval_js("switchTab('table')")

        for _ in range(30):
            has_table = await client.eval_js(
                "document.querySelectorAll('#table-body tr').length > 0 && !document.getElementById('status-indicator').innerText.includes('Loading')"
            )
            if has_table:
                break
            await asyncio.sleep(0.5)

        await client.eval_js("window.scrollTo(0, 0);")
        await asyncio.sleep(1.0)
        img4 = os.path.join(SCREENSHOT_DIR, "raw_data_table.png")
        await client.screenshot(img4)

        await client.close()
        print("All screenshots successfully captured!")

    finally:
        chrome_proc.terminate()
        try:
            chrome_proc.wait(timeout=3)
        except Exception:
            subprocess.run(["taskkill.exe", "/F", "/IM", "chrome.exe"], capture_output=True)


if __name__ == "__main__":
    asyncio.run(main())
