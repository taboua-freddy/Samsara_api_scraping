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
    ):
        self.api_token: str = api_token
        self.base_url: str = "https://api.eu.samsara.com"
        self.headers: dict = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        self.rate_limiter: EndpointRateLimiter = rate_limiter
        self.delta_days = delta_days
        self.logger = MyLogger("SamsaraClient")
        self.shared_vars_manager = shared_vars_manager

    def get_all_data(
        self, endpoint: str, params: dict = None, max_calls_per_second=5
    ) -> list[dict]:
        """
        Récupère toutes les données pour un endpoint donné, en gérant la pagination.
        :param endpoint: valeur de l'endpoint
        :param params: paramètres de la requête
        :param max_calls_per_second: nombre maximal de requêtes par seconde
        :return:
        """
        all_data = []
        url = f"{self.base_url}/{endpoint}"
        has_next_page = True
        params = params or {}
        max_retries = 5

        while has_next_page:
            retry_count = 0
            success = False

            while not success and retry_count < max_retries:
                self.rate_limiter.acquire(endpoint, max_calls_per_second)
                try:
                    self.logger.info(
                        f"Demande de données à {url} avec les paramètres {params}"
                    )
                    response = requests.get(url, headers=self.headers, params=params)
                    # Gère le succès et les erreurs spécifiques de la requête
                    if response.status_code == 200:
                        data = response.json()
                        self.logger.info(
                            f"Données récupérées avec succès pour {url} et les paramètres {params}"
                        )
                        if data.get("data", None) is None:
                            # si la réponse n'a pas l'attribut 'data' et est juste un dictionnaire
                            if isinstance(data, dict):
                                all_data.append(data)
                        if d := data.get("data", []):
                            # si la réponse à l'attribut 'data' mais est un dictionnaire
                            if isinstance(d, dict):
                                keys = list(d.keys())
                                if len(keys) == 1 and isinstance(d[keys[0]], list):
                                    d = d[list(d.keys())[0]]
                                else:
                                    d = [d]
                            all_data.extend(d)
                        success = True
                    # Code 429 (trop de requêtes) déclenche une attente pour le retry
                    elif response.status_code == 429:
                        retry_after = float(response.headers.get("Retry-After", 1))
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
                    self.logger.error(f"Exception lors de la requête: {e}")
                    retry_count += 1
                    sleep_time = 2**retry_count
                    self.logger.info(f"Nouvelle tentative dans {sleep_time} secondes")
                    time.sleep(sleep_time)

            if not success:
                self.logger.error(
                    f"Échec après plusieurs tentatives, arrêt du traitement pour {url} et les paramètres {params}"
                )
                break
            # Gestion de la pagination pour récupérer toutes les pages

            pagination = data.get("pagination", {})
            has_next_page = pagination.get("hasNextPage", False)
            end_cursor = pagination.get("endCursor")
            if has_next_page and end_cursor:
                # Mise à jour du paramètre 'after' pour la pagination
                params["after"] = end_cursor
            else:
                has_next_page = False
                # self.shared_vars_manager.alter_value(set_metadata_is_processed,"metadata", endpoint=endpoint, is_processed=1, params=params)

        return all_data
