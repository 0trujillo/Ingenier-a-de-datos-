"""
Utilitarios para reintentos con backoff exponencial y manejo de errores.
"""

import logging
import time
from functools import wraps
from typing import Callable, Any, Type, Tuple


def retry_with_backoff(max_attempts: int = 3, base_delay: float = 1, max_delay: float = 60):
    """
    Decorador que implementa reintentos con backoff exponencial.
    
    Args:
        max_attempts: Número máximo de intentos
        base_delay: Retardo base en segundos (se duplica en cada intento)
        max_delay: Retardo máximo en segundos
    
    Example:
        @retry_with_backoff(max_attempts=3, base_delay=2)
        def call_external_api():
            return requests.get("https://api.example.com")
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    
                    if attempt == max_attempts:
                        logging.error(
                            "❌ %s falló después de %d intentos: %s",
                            func.__name__, max_attempts, exc, exc_info=True
                        )
                        raise
                    
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    logging.warning(
                        "⏳ %s intento %d/%d falló. Reintentando en %.1f segundos... Error: %s",
                        func.__name__, attempt, max_attempts, delay, exc
                    )
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator


def retry_on_exception(
    exception_types: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    base_delay: float = 1
):
    """
    Decorador que reinteneta solo si se lanza una excepción específica.
    
    Args:
        exception_types: Tupla de tipos de excepción para reintentar
        max_attempts: Número máximo de intentos
        base_delay: Retardo base
    
    Example:
        @retry_on_exception((requests.ConnectionError, requests.Timeout), max_attempts=5)
        def fetch_data():
            return requests.get("https://api.example.com", timeout=10)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exception_types as exc:
                    if attempt == max_attempts:
                        logging.error(
                            "❌ %s falló después de %d intentos",
                            func.__name__, max_attempts, exc_info=True
                        )
                        raise
                    
                    delay = base_delay * (2 ** (attempt - 1))
                    logging.warning(
                        "⏳ %s intento %d/%d falló. Reintentando en %.1f segundos...",
                        func.__name__, attempt, max_attempts, delay
                    )
                    time.sleep(delay)
        
        return wrapper
    return decorator


def handle_and_log(error_message: str, default_return: Any = None):
    """
    Decorador que captura excepciones, las registra y retorna un valor por defecto.
    
    Args:
        error_message: Mensaje de error a registrar
        default_return: Valor a retornar en caso de error
    
    Example:
        @handle_and_log("Error procesando datos", default_return={})
        def process_data(data):
            return json.loads(data)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                logging.error(
                    "❌ %s: %s",
                    error_message, exc, exc_info=True
                )
                return default_return
        
        return wrapper
    return decorator
