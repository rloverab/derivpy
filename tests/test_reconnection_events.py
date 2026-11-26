# tests/test_reconnection_events.py
import pytest
from derivpy.reconnection import ReconnectionEvent

class TestReconnectionEvents:
    """Valida que todos los eventos del Enum estén correctamente definidos."""
    
    def test_all_events_defined(self):
        """Verifica que todos los eventos esperados existan."""
        expected_events = {
            'FATAL_FAILURE',
            'REQUIRES_HISTORY_RELOAD',
            'HEALTH_CHECK_PASSED',
            'EMERGENCY_CLEANUP',
            'CONNECTION_DEGRADED',
            'RATE_LIMITED',
        }
        
        actual_events = {event.name for event in ReconnectionEvent}
        assert actual_events == expected_events
    
    def test_events_have_string_values(self):
        """Verifica que los valores sean strings para serialización."""
        for event in ReconnectionEvent:
            assert isinstance(event.value, str)
            assert event.value == event.value.lower().replace('_', '_')
    
    def test_event_exhaustiveness_in_callback(self):
        """
        Valida que un handler de eventos cubra todos los casos.
        Esto previene bugs cuando se añaden nuevos eventos al Enum.
        """
        def event_handler(event: ReconnectionEvent) -> str:
            """Handler que debe manejar todos los eventos."""
            if event == ReconnectionEvent.FATAL_FAILURE:
                return "fatal"
            elif event == ReconnectionEvent.REQUIRES_HISTORY_RELOAD:
                return "reload"
            elif event == ReconnectionEvent.HEALTH_CHECK_PASSED:
                return "ok"
            elif event == ReconnectionEvent.EMERGENCY_CLEANUP:
                return "cleanup"
            elif event == ReconnectionEvent.CONNECTION_DEGRADED:
                return "degraded"
            elif event == ReconnectionEvent.RATE_LIMITED:
                return "throttled"
            else:
                # Si llegamos aquí, falta manejar un evento
                raise AssertionError(f"Evento no manejado: {event}")
        
        # Debe poder procesar todos los eventos sin error
        for event in ReconnectionEvent:
            result = event_handler(event)
            assert isinstance(result, str)