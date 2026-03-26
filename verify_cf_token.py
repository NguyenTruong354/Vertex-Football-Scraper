import httpx
token = "cfut_LHRXI4rCLKFSIiSDVQEqS51KZJ2BYtM6Sw9OYyYt6061e063"
resp = httpx.get(
    "https://api.cloudflare.com/client/v4/user/tokens/verify",
    headers={"Authorization": f"Bearer {token}"}
)
print(f"Status: {resp.status_code}")
print(f"Response: {resp.text}")
