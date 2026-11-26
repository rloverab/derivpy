# tests/test_reconnection_config.py
import pytest
from derivpy.reconnection import ReconnectionConfig, ReconnectionMode

class TestReconnectionConfig:
    """Valida la configuración y cálculos de backoff."""
    
    def test_default_values(self):
        """Verifica que los valores por defecto sean razonables."""
        config = ReconnectionConfig()
        
        assert config.mode == ReconnectionMode.STOP_ON_FATAL
        assert config.max_attempts == 5
        assert config.base_delay == 1.0
        assert config.max_delay == 300.0
        assert config.backoff_multiplier == 2.0
        assert config.jitter is True
    
    def test_calculate_backoff_delay_exponential(self):
        """Verifica el crecimiento exponencial del backoff."""
        config = ReconnectionConfig(
            base_delay=1.0,
            max_delay=60.0,
            backoff_multiplier=2.0,
            jitter=False  # Desactivar jitter para tests determinísticos
        )
        
        # Intento 1: 1.0 * 2^0 = 1.0
        assert config.calculate_backoff_delay(1) == 1.0
        
        # Intento 2: 1.0 * 2^1 = 2.0
        assert config.calculate_backoff_delay(2) == 2.0
        
        # Intento 3: 1.0 * 2^2 = 4.0
        assert config.calculate_backoff_delay(3) == 4.0
        
        # Intento 7: 1.0 * 2^6 = 64.0, pero limitado a max_delay=60.0
        assert config.calculate_backoff_delay(7) == 60.0
    
    def test_calculate_backoff_delay_with_jitter(self):
        """Verifica que el jitter añade aleatoriedad controlada."""
        config = ReconnectionConfig(
            base_delay=10.0,
            jitter=True
        )
        
        # Ejecutar múltiples veces para verificar el rango
        delays = [config.calculate_backoff_delay(1) for _ in range(100)]
        
        # Con jitter +/-25%, el rango debe estar entre 7.5 y 12.5
        assert all(7.5 <= d <= 12.5 for d in delays)
        
        # Debe haber variación (no todos iguales)
        assert len(set(delays)) > 1
    
    def test_calculate_backoff_delay_respects_max(self):
        """Verifica que nunca excede max_delay."""
        config = ReconnectionConfig(
            base_delay=1.0,
            max_delay=10.0,
            backoff_multiplier=10.0,
            jitter=False
        )
        
        # Intento 5: 1.0 * 10^4 = 10000, pero limitado a 10.0
        assert config.calculate_backoff_delay(5) == 10.0