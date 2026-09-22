import time

import requests

from .logs import MyLogger
from .raters import EndpointRateLimiter, MemoryAccess


class SamsaraClient:
    """
    Cette classe encapsule les appels à l'API Samsara, en gérant les erreurs de connexion et en utilisant la pagination.
    """

    def __init__(
        self,
        api_token: str,
        rate_limiter: EndpointRateLimiter,
        shared_vars_manager: MemoryAccess = None,
        delta_days: int = 1,
        timeout: tuple[float, float] = (10, 120),
        session: requests.Session | None = None,
    ):
        self.api_token: str = api_token
        self.base_url: str = "https://api.eu.samsara.com"
        self.headers: dict = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self.rate_limiter: EndpointRateLimiter = rate_limiter
        self.delta_days = delta_days
        self.timeout = timeout
        self.session = session or requests.Session()
        # Cloud Run only captures stdout/stderr while the job is running.  Keep
        # file logs as before, and mirror API progress to the console as well.
        self.logger = MyLogger("SamsaraClient")
        self.shared_vars_manager = shared_vars_manager

    def get_all_data(
        self,
        endpoint: str,
        params: dict = None,
        max_calls_per_second=5,
        rate_limit_key: str | None = None,
    ) -> list[dict]:
        """Return all pages for compatibility with dynamic endpoint workflows."""
        all_data = []
        for page, _pagination in self.iter_data_pages(
            endpoint,
            params=params,
            max_calls_per_second=max_calls_per_second,
            rate_limit_key=rate_limit_key,
        ):
            all_data.extend(page)
        return all_data

    def iter_data_pages(
        self,
        endpoint: str,
        params: dict = None,
        max_calls_per_second=5,
        rate_limit_key: str | None = None,
    ):
        """Yield normalized pages and pagination metadata without accumulating them."""
        url = f"{self.base_url}/{endpoint}"
        has_next_page = True
        params = (params or {}).copy()
        max_retries = 5

        while has_next_page:
            retry_count = 0
            success = False
            last_error = None

            while not success and retry_count < max_retries:
                # Dynamic URLs must share the quota of their endpoint template;
                # otherwise every vehicle ID gets an independent limiter.
                self.rate_limiter.acquire(
                    rate_limit_key or endpoint, max_calls_per_second
                )
                try:
                    self.logger.info(
                        f"Demande de données à {url} avec les paramètres {params}"
                    )
                    response = self.session.get(
                        url,
                        headers=self.headers,
                        params=params,
                        timeout=self.timeout,
                    )
                    # Gère le succès et les erreurs spécifiques de la requête
                    if response.status_code == 200:
                        data = response.json()
                        self.logger.info(
                            f"Données récupérées avec succès pour {url} et les paramètres {params}"
                        )
                        success = True
                    # Code 429 (trop de requêtes) déclenche une attente pour le retry
                    elif response.status_code == 429:
                        retry_after = float(response.headers.get("Retry-After", 1))
                        last_error = RuntimeError(
                            f"Limite de requêtes atteinte pour {url}"
                        )
                        self.logger.warning(
                            f"Reçu code 429, attente de {retry_after} secondes pour {url} et les paramètres {params}"
                        )
                        time.sleep(retry_after)
                        retry_count += 1
                    else:
                        self.logger.error(
                            f"Erreur lors de la récupération des données: {response.status_code} {response.text}"
                        )
                        response.raise_for_status()
                except requests.exceptions.RequestException as e:
                    last_error = e
                    self.logger.error(f"Exception lors de la requête: {e}")
                    status_code = getattr(getattr(e, "response", None), "status_code", None)
                    if status_code is not None and 400 <= status_code < 500:
                        raise
                    retry_count += 1
                    sleep_time = 2**retry_count
                    self.logger.info(f"Nouvelle tentative dans {sleep_time} secondes")
                    time.sleep(sleep_time)

            if not success:
                self.logger.error(
                    f"Échec après plusieurs tentatives, arrêt du traitement pour {url} et les paramètres {params}"
                )
                raise RuntimeError(
                    f"Échec de l'appel Samsara après {max_retries} tentatives: "
                    f"{url}. Dernière erreur: {last_error}"
                ) from last_error
            page = self._normalize_page(data)
            pagination = data.get("pagination", {})
            yield page, pagination
            has_next_page = pagination.get("hasNextPage", False)
            end_cursor = pagination.get("endCursor")
            if has_next_page and end_cursor:
                # Mise à jour du paramètre 'after' pour la pagination
                params["after"] = end_cursor
            else:
                has_next_page = False

    @staticmethod
    def _normalize_page(payload: dict) -> list[dict]:
        if payload.get("data") is None:
            return [payload] if isinstance(payload, dict) else []
        data = payload.get("data", [])
        if isinstance(data, dict):
            keys = list(data.keys())
            if len(keys) == 1 and isinstance(data[keys[0]], list):
                data = data[keys[0]]
            else:
                data = [data]
        return data
