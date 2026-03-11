import json

# Test embed payload structure (Discord expects this format)
embed = {
    "title": "Live Insight | Liverpool vs Chelsea",
    "color": 0xFFA500,
    "fields": [
        {"name": "English", "value": "Liverpool dominates with 70% possession.", "inline": False},
        {"name": "Vietnamese", "value": "Liverpool ap dao voi 70% cam bong.", "inline": False},
    ],
    "footer": {"text": "League: EPL | Prompt: v1 | Latency: 450ms"},
}

payload = {"embeds": [embed]}
print("Embed JSON valid:", bool(json.dumps(payload)))
print("Fields count:", len(embed["fields"]))
print("Title:", embed["title"])
print()

# Test health report embed
health = {
    "title": "Daily Health Report",
    "color": 0x9B59B6,
    "description": "Report for 2026-03-11 09:00 UTC",
    "fields": [
        {"name": "Leagues", "value": "EPL, LALIGA, UCL", "inline": True},
        {"name": "Maintenance", "value": "All tasks completed", "inline": True},
        {"name": "AI Pipeline (24h)", "value": "Total: 45 jobs\nSucceeded: 42 (93%)\nDropped: 2\nFailed: 1", "inline": False},
        {"name": "Circuit Breaker", "value": "No circuit breaker events (all healthy)", "inline": False},
    ],
    "footer": {"text": "Vertex Football Scraper v2.0 | e2-micro"},
}
health_payload = {"embeds": [health]}
print("Health JSON valid:", bool(json.dumps(health_payload)))
print("Health fields count:", len(health["fields"]))
print()
print("All embed structures are valid!")
