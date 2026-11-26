# tests/test_client_coordination.py
import pytest
from derivpy.market_data import MarketDataClient
from derivpy.trading import TradingClient
from derivpy.reconnection import ReconnectionConfig, ReconnectionMode

class TestClientCoordination:
    """Valida la coordinación entre MarketDataClient y TradingClient."""
    
    @pytest.mark.asyncio
    async def test_both_clients_accept_reconnection_config(self):
        """Verifica que ambos clientes aceptan configuración de reconexión."""
        config = ReconnectionConfig(
            mode=ReconnectionMode.INFINITE_RETRY,
            max_delay=120.0,
            zombie_timeout=180.0
        )
        
        # MarketDataClient
        md_client = MarketDataClient(reconnection_config=config)
        assert md_client._reconnection_config.mode == ReconnectionMode.INFINITE_RETRY
        assert md_client._reconnection_config.max_delay == 120.0
        
        # TradingClient
        trading_client = TradingClient(
            account_id="test123",
            token="test_token",
            app_id="test_app",
            reconnection_config=config
        )
        assert trading_client._reconnection_config.mode == ReconnectionMode.INFINITE_RETRY
        assert trading_client._reconnection_config.max_delay == 120.0
    
    @pytest.mark.asyncio
    async def test_is_connection_stable_property(self):
        """Verifica que is_connection_stable refleja el estado del event."""
        client = MarketDataClient()
        
        # Inicialmente inestable
        assert client.is_connection_stable is False
        assert client._connection_event.is_set() is False
        
        # Simular conexión exitosa
        client._connection_event.set()
        assert client.is_connection_stable is True
        
        # Simular desconexión
        client._connection_event.clear()
        assert client.is_connection_stable is False