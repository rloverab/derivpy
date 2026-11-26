# tests/test_base_client_callback.py
import pytest
import asyncio
from derivpy._base_client import BaseDerivWSClient
from derivpy.reconnection import ReconnectionEvent, ReconnectionConfig

class TestInvokeEventCallback:
    """Valida la robustez del callback con timeout y manejo de errores."""
    
    @pytest.fixture
    def client(self):
        """Crea un cliente de prueba."""
        config = ReconnectionConfig(event_callback_timeout=2.0)
        client = BaseDerivWSClient("wss://test.com", reconnection_config=config)
        return client
    
    @pytest.mark.asyncio
    async def test_callback_timeout_protection(self, client):
        """Verifica que el callback no bloquea más allá del timeout."""
        async def slow_callback(*args):
            await asyncio.sleep(10)  # Mucho más que el timeout
            return True
        
        client.set_event_callback(slow_callback, timeout=1.0)
        
        start = asyncio.get_event_loop().time()
        result = await client._invoke_event_callback(
            ReconnectionEvent.FATAL_FAILURE,
            1
        )
        elapsed = asyncio.get_event_loop().time() - start
        
        # Debe retornar False (fallback seguro) en ~1 segundo, no 10
        assert elapsed < 2.0
        assert result is False  # Fallback para FATAL_FAILURE
    
    @pytest.mark.asyncio
    async def test_callback_exception_handling(self, client):
        """Verifica que las excepciones en el callback no propagan."""
        async def failing_callback(*args):
            raise RuntimeError("Error inesperado en callback")
        
        client.set_event_callback(failing_callback, timeout=1.0)
        
        # No debe lanzar excepción
        result = await client._invoke_event_callback(
            ReconnectionEvent.HEALTH_CHECK_PASSED,
            0
        )
        
        # Fallback seguro: True para eventos no fatales
        assert result is True
    
    @pytest.mark.asyncio
    async def test_callback_returns_correct_value(self, client):
        """Verifica que el valor retornado por el callback se respeta."""
        async def custom_callback(client_name, event, failure_count):
            if event == ReconnectionEvent.RATE_LIMITED:
                return False  # No continuar
            return True  # Continuar
        
        client.set_event_callback(custom_callback, timeout=1.0)
        
        # Debe retornar False para RATE_LIMITED
        result1 = await client._invoke_event_callback(
            ReconnectionEvent.RATE_LIMITED,
            5
        )
        assert result1 is False
        
        # Debe retornar True para otros eventos
        result2 = await client._invoke_event_callback(
            ReconnectionEvent.HEALTH_CHECK_PASSED,
            0
        )
        assert result2 is True
    
    @pytest.mark.asyncio
    async def test_callback_receives_correct_arguments(self, client):
        """Verifica que el callback recibe los argumentos correctos."""
        received_args = []
        
        async def tracking_callback(client_name, event, failure_count):
            received_args.append((client_name, event, failure_count))
            return True
        
        client.set_event_callback(tracking_callback, timeout=1.0)
        
        await client._invoke_event_callback(
            ReconnectionEvent.REQUIRES_HISTORY_RELOAD,
            3
        )
        
        assert len(received_args) == 1
        client_name, event, failure_count = received_args[0]
        
        assert client_name == "BaseDerivWSClient"
        assert event == ReconnectionEvent.REQUIRES_HISTORY_RELOAD
        assert failure_count == 3
    
    @pytest.mark.asyncio
    async def test_no_callback_uses_default_behavior(self, client):
        """Verifica el comportamiento por defecto sin callback registrado."""
        # Sin callback registrado
        assert client._event_callback is None
        
        # FATAL_FAILURE debe retornar False (parar)
        result1 = await client._invoke_event_callback(
            ReconnectionEvent.FATAL_FAILURE,
            1
        )
        assert result1 is False
        
        # Otros eventos deben retornar True (continuar)
        result2 = await client._invoke_event_callback(
            ReconnectionEvent.HEALTH_CHECK_PASSED,
            0
        )
        assert result2 is True