import time
from threading import Lock


class RateLimiter:
    def __init__(self, max_calls_per_second: int | float):
        """
        cette classe implémente un rate limiter pour limiter le nombre de requêtes par seconde
        :param max_calls_per_second: Nombre maximal de requêtes par seconde
        """
        self.max_calls_per_second: int | float = max_calls_per_second  # Limite de requêtes par seconde
        self.lock: Lock = Lock()  # Verrou pour la gestion des accès simultanés
        self.tokens: int | float = max_calls_per_second  # Jetons représentant les requêtes disponibles
        self.last_timestamp: float = time.time()  #  Dernière mise à jour des jetons

    def acquire(self):
        """
        Acquiert un jeton pour envoyer une requête, en régulant la fréquence d'appels.
        """
        with self.lock:
            current_timestamp = time.time()
            elapsed = current_timestamp - self.last_timestamp
            # Recharge les jetons selon le temps écoulé
            self.tokens += elapsed * self.max_calls_per_second
            if self.tokens > self.max_calls_per_second:
                self.tokens = self.max_calls_per_second
            self.last_timestamp = current_timestamp
            if self.tokens >= 1:
                self.tokens -= 1  # Consomme un jeton
                return
            else:
                # Si aucun jeton disponible, attend jusqu'à la disponibilité d'un jeton
                sleep_time = (1 - self.tokens) / self.max_calls_per_second
                time.sleep(sleep_time)
                self.tokens = 0
                self.last_timestamp = time.time()


class EndpointRateLimiter:
    """
    Cette classe utilise RateLimiter pour limiter les appels non seulement globalement, mais aussi individuellement pour chaque table.
    """

    def __init__(self, max_calls_per_second: int | float = 150):
        self.endpoint_limiters: dict = {}
        self.global_rate_limiter: RateLimiter = RateLimiter(max_calls_per_second)  # Limite globale de 150 req/s

    def get_rate_limiter(self, endpoint: str, max_calls_per_second) -> RateLimiter:
        """
        Récupère le rate limiter pour une table spécifique, en le créant s'il n'existe pas.
        :param endpoint: valeur du nom du endpoint
        :param max_calls_per_second: nombre maximal de requêtes par seconde
        :return: le rate limiter pour l'endpoint de la table spécifiée
        """
        if endpoint not in self.endpoint_limiters:
            self.endpoint_limiters[endpoint] = RateLimiter(max_calls_per_second)
        return self.endpoint_limiters[endpoint]

    def acquire(self, endpoint: str, max_calls_per_second) -> None:
        """
        Acquiert un jeton pour un endpoint spécifique, en vérifiant d'abord la limite globale.
        :param endpoint: valeur du nom du endpoint
        :param max_calls_per_second: nombre maximal de requêtes par seconde
        :return: None
        """
        # D'abord, vérifier la limite globale
        self.global_rate_limiter.acquire()
        # Ensuite, vérifier la limite spécifique de la table
        endpoint_limiter = self.get_rate_limiter(endpoint, max_calls_per_second)
        endpoint_limiter.acquire()


class MemoryAccess:
    def __init__(self):
        self.__memory = {}
        self.__lock = Lock()

    def read(self, key):
        with self.__lock:
            return self.__memory.get(key)

    def write(self, key, value):
        with self.__lock:
            self.__memory[key] = value

    def alter_value(self, func: callable, key, *args, **kwargs):
        with self.__lock:
            self.__memory[key] = func(self.__memory[key], *args, **kwargs)
            if key == "metadata":
                self.__memory[key].to_excel("metadata.xlsx", index=False, header=True)

    def delete(self, key):
        with self.__lock:
            if key in self.__memory:
                del self.__memory[key]
            else:
                raise KeyError(f"La clé '{key}' n'existe pas dans la mémoire")

    def clear(self):
        with self.__lock:
            self.__memory.clear()
