import asyncio

import pytest
from aiohttp import ClientSession

from src.workflows.etl.mock_api import MockApiConfig, MockApiServer


@pytest.mark.asyncio
async def test_mock_api_offline_and_online_controls() -> None:
    server = MockApiServer(MockApiConfig(port=0))
    await server.start()
    try:
        port = server.bound_port
        base_url = f"http://127.0.0.1:{port}"
        async with ClientSession() as session:
            # Baseline request should succeed while service is online.
            response = await session.get(f"{base_url}/datasets/transactions")
            assert response.status == 200
            await response.text()

            # Take the server offline for a short window and verify outage response.
            offline_response = await session.post(
                f"{base_url}/_admin/mock/offline", json={"duration_seconds": 1}
            )
            assert offline_response.status == 200
            await offline_response.text()

            outage = await session.get(f"{base_url}/datasets/transactions")
            assert outage.status == 503
            await outage.text()

            # Wait until the outage window expires and ensure service returns.
            await asyncio.sleep(1.2)
            recovered = await session.get(f"{base_url}/datasets/transactions")
            assert recovered.status == 200
            await recovered.text()

            # Force an indefinite outage and confirm manual restore works.
            indefinite = await session.post(f"{base_url}/_admin/mock/offline", json={})
            assert indefinite.status == 200
            await indefinite.text()
            unavailable = await session.get(f"{base_url}/datasets/transactions")
            assert unavailable.status == 503
            await unavailable.text()

            restore = await session.post(f"{base_url}/_admin/mock/online", json={})
            assert restore.status == 200
            await restore.text()
            healthy = await session.get(f"{base_url}/datasets/transactions")
            assert healthy.status == 200
            await healthy.text()
    finally:
        await server.stop()
