# tests/test_reconnection_modes.py
import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from derivpy._base_client import BaseDerivWSClient
from derivpy.reconnection import ReconnectionMode, ReconnectionConfig

class TestReconnectionModes:
    """Valida el comportamiento de los tres modos de reconexión."""
    
    @pytest.fixture
    def mock_websocket(self):
        """Mock del websocket para simular fallos."""
        ws = AsyncMock()
        ws.recv = AsyncMock(side_effect=Exception("Connection closed"))
        return ws
    
    @pytest.mark.asyncio
    async def test_stop_on_fatal_mode(self, mock_websocket):
        """Verifica que STOP_ON_FATAL detiene tras el primer fallo."""
        config = ReconnectionConfig(
            mode=ReconnectionMode.STOP_ON_FATAL,
            max_attempts=5  # No debería importar
        )
        
        client = BaseDerivWSClient("wss://test.com", reconnection_config=config)
        
        # Mock connect para que siempre falle
        with patch.object(client, 'connect', side_effect=Exception("Connection failed")):
            # Debe retornar False inmediatamente
            should_continue = await client._should_continue_retrying()
            assert should_continue is False
    
    @pytest.mark.asyncio
    async def test_limited_retry_mode_respects_max_attempts(self):
        """Verifica que LIMITED_RETRY respeta max_attempts."""
        config = ReconnectionConfig(
            mode=ReconnectionMode.LIMITED_RETRY,
            max_attempts=3
        )
        
        client = BaseDerivWSClient("wss://test.com", reconnection_config=config)
        
        # Simular fallos consecutivos
        client._consecutive_failures = 1
        assert await client._should_continue_retrying() is True
        
        client._consecutive_failures = 2
        assert await client._should_continue_retrying() is True
        
        client._consecutive_failures = 3
        assert await client._should_continue_retrying() is False  # Alcanzó el límite
        
        client._consecutive_failures = 4
        assert await client._should_continue_retrying() is False
    
    @pytest.mark.asyncio
    async def test_infinite_retry_mode_uses_callback(self):
        """Verifica que INFINITE_RETRY consulta al callback."""
        config = ReconnectionConfig(mode=ReconnectionMode.INFINITE_RETRY)
        client = BaseDerivWSClient("wss://test.com", reconnection_config=config)
        
        # Callback que decide continuar
        async def continue_callback(*args):
            return True
        
        client.set_event_callback(continue_callback)
        client._consecutive_failures = 100  # Número alto
        
        assert await client._should_continue_retrying() is True
        
        # Callback que decide parar
        async def stop_callback(*args):
            return False
        
        client.set_event_callback(stop_callback)
        assert await client._should_continue_retrying() is False