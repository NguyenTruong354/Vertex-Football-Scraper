import requests
headers = {"User-Agent": "Mozilla/5.0"}
seasons = requests.get("https://api.sofascore.com/api/v1/unique-tournament/17/seasons", headers=headers).json()
season_id = seasons["seasons"][0]["id"]
print("Season ID:", season_id, "Name:", seasons["seasons"][0]["name"])
st = requests.get(f"https://api.sofascore.com/api/v1/unique-tournament/17/season/{season_id}/standings/total", headers=headers).json()
for r in st["standings"][0]["rows"]: print(r["position"], r["team"]["id"], r["team"]["name"])
