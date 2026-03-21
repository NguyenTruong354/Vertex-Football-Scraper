import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import curl_cffi.requests

from network.http_client import CurlCffiClient
import core.config as cfg

@pytest.fixture
def mock_cfg(monkeypatch):
    """Fixture to mock config for stealth network testing."""
    monkeypatch.setattr(cfg, "USE_STEALTH_NETWORK", True)
    monkeypatch.setattr(cfg, "STEALTH_BRIDGES", ["https://bridge1.com", "https://bridge2.com"])
    monkeypatch.setattr(cfg, "BRIDGE_SECRET", "test_secret")
    return cfg

import pytest_asyncio

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_notifier():
    return MagicMock()

@pytest_asyncio.fixture
async def stealth_client(mock_cfg, mock_logger, mock_notifier):
    client = CurlCffiClient(mock_logger, mock_notifier)
    await client.start()
    yield client
    await client.stop()

@pytest.mark.asyncio
async def test_tc07_circuit_breaker_ttl_and_recovery(stealth_client):
    """TC-07: Verify bridge is skipped when failing, but recovers after TTL (cooldown)."""
    # Force _mark_unhealthy directly
    with patch("time.time", return_value=1000.0):
        stealth_client._mark_unhealthy("https://bridge1.com", cooldown=300)
    
    # At t=1050 (during cooldown), bridge1 should be considered unhealthy
    with patch("time.time", return_value=1050.0):
        # The index is 0, so next would be bridge1. But it's unhealthy. 
        # It should skip bridge1 and return bridge2.
        bridge = await stealth_client._next_healthy_bridge()
        assert bridge == "https://bridge2.com", "Expected bridge2 because bridge1 is on cooldown."
    
    # At t=1301 (after cooldown), bridge1 should have recovered
    with patch("time.time", return_value=1301.0):
        # We simulate we are looping around
        # But wait, index is currently 2. 2 % 2 = 0. So it will check bridge1 again.
        bridge = await stealth_client._next_healthy_bridge()
        assert bridge == "https://bridge1.com", "Expected bridge1 to have auto-recovered."

@pytest.mark.asyncio
async def test_tc08_graceful_degradation(stealth_client, mock_logger):
    """TC-08: Mock all bridges failing to verify direct fetching fallback."""
    
    # Mock post to fail for all bridges
    stealth_client._session.post = AsyncMock(side_effect=Exception("Bridge Down"))
    
    # Mock get to succeed (fallback)
    mock_resp_get = MagicMock(status_code=200)
    mock_resp_get.json.return_value = {"fallback": "success"}
    stealth_client._session.get = AsyncMock(return_value=mock_resp_get)
    
    # Run fetch
    data = await stealth_client.get_json("/test-endpoint")
    
    # Assertions
    assert data == {"fallback": "success"}, "Expected successful payload from direct fallback"
    assert stealth_client._session.get.called, "Fallback direct GET was not called"
    
    # Check that both bridges were marked unhealthy eventually
    assert len(stealth_client._unhealthy_bridges) >= 1, "Expected bridges to be marked unhealthy"

@pytest.mark.asyncio
async def test_tc09_token_security_and_leakage(stealth_client, mock_logger):
    """TC-09: 401 response from bridge should not leak target URL in WARNING logs."""
    
    # Mock post to return 401
    mock_resp_post = MagicMock(status_code=401)
    stealth_client._session.post = AsyncMock(return_value=mock_resp_post)
    
    # Mock get to return 200 (allow fallback)
    mock_resp_get = MagicMock(status_code=200)
    mock_resp_get.json.return_value = {"data": "direct"}
    stealth_client._session.get = AsyncMock(return_value=mock_resp_get)
    
    await stealth_client.get_json("/secret-endpoint")
    
    # Verify debug log was called for 401 instead of warning/error
    calls = mock_logger.debug.call_args_list
    unauthorized_logged = any("401 Unauthorized" in call[0][0] for call in calls)
    assert unauthorized_logged, "Expected 401 to be logged at DEBUG level to avoid spam/leaks."
    
    # Verify no WARNING or ERROR logs expose the URL for security issues on the bridge
    warning_calls = mock_logger.warning.call_args_list
    url_leaked = any("/secret-endpoint" in str(call) for call in warning_calls)
    assert not url_leaked, "Expected URL to NOT be leaked in WARNING logs during 401 Bridge Auth Failure"

@pytest.mark.asyncio
async def test_tc10_deterministic_rotation(stealth_client):
    """TC-10: Verify exact Round-Robin sequence across active bridge pool with safe locking."""
    # We expect bridge1, bridge2, bridge1, bridge2
    b1 = await stealth_client._next_healthy_bridge()
    b2 = await stealth_client._next_healthy_bridge()
    b3 = await stealth_client._next_healthy_bridge()
    b4 = await stealth_client._next_healthy_bridge()
    
    assert b1 == "https://bridge1.com"
    assert b2 == "https://bridge2.com"
    assert b3 == "https://bridge1.com"
    assert b4 == "https://bridge2.com"
