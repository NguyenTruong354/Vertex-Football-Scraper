"""
Cloudflare Crawl API (March 2026) - Quick Test Client
Requirements: pip install httpx
"""
import httpx
import time
import sys

# === CONFIGURATION ===
# Lay tu: https://dash.cloudflare.com/
CF_ACCOUNT_ID = "9571fd5a9109893ede41ce662f187efb" 
# Tao Token tai: https://dash.cloudflare.com/profile/api-tokens 
# (Can quyen "Browser Rendering - Edit")
CF_API_TOKEN = "cfut_LHRXI4rCLKFSIiSDVQEqS51KZJ2BYtM6Sw9OYyYt6061e063"

URL_TO_CRAWL = "https://fbref.com/en/comps/9/2023-2024/2023-2024-Premier-League-Stats"

API_BASE = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/browser-rendering"

headers = {
    "Authorization": f"Bearer {CF_API_TOKEN}",
    "Content-Type": "application/json"
}

def start_crawl():
    print(f"[*] Initializing Crawl for: {URL_TO_CRAWL}")
    payload = {
        "url": URL_TO_CRAWL,
        "render": True       # Chay JS de vuot Turnstile
    }
    
    try:
        resp = httpx.post(f"{API_BASE}/crawl", json=payload, headers=headers, timeout=30)
        if resp.status_code != 200:
            print(f"[!] Error: {resp.status_code} - {resp.text}")
            return None
        
        data = resp.json()
        job_id = data["result"] 
        print(f"[+] Job started! ID: {job_id}")
        return job_id
    except Exception as e:
        print(f"[!] Error during start: {e}")
        return None

def get_result(job_id):
    print(f"[-] Waiting for result (Job: {job_id})...")
    for _ in range(60): # Cho tối đa 60s
        time.sleep(2)
        try:
            resp = httpx.get(f"{API_BASE}/crawl/{job_id}", headers=headers)
            data = resp.json()
            
            # API might return result as a string or object
            res = data.get("result", {})
            if isinstance(res, str):
                print(f"  ...status: {res}")
                continue

            status = res.get("status")
            if status == "completed":
                with open("cf_raw_response.json", "w", encoding="utf-8") as debug_file:
                    import json
                    json.dump(data, debug_file)
                print(f"FULL RESPONSE SAVED TO cf_raw_response.json")
                pages = res.get("pages", [])
                if pages:
                    html = pages[0].get("content")
                    print(f"[*] SUCCESS! Captured {len(html)} bytes HTML.")
                    return html
                else:
                    print("[!] Job completed but no pages found.")
                    return None
            elif status == "failed":
                print(f"[!] Crawl failed. Data: {res}")
                return None
            else:
                print(f"  ...status: {status}")
        except Exception as e:
            print(f"  ...monitoring error: {e}")
            
    print("[!] Timeout waiting for results.")
    return None

if __name__ == "__main__":
    if "YOUR_" in CF_ACCOUNT_ID or "YOUR_" in CF_API_TOKEN:
        print("[!] No credentials provided!")
        sys.exit(1)
        
    job_id = start_crawl()
    if job_id:
        html = get_result(job_id)
        if html:
            with open("cf_crawl_result.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("[*] Result saved to cf_crawl_result.html")
