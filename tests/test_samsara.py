import unittest
from unittest.mock import Mock

import requests

from modules.raters import EndpointRateLimiter
from modules.samsara import SamsaraClient


class SamsaraClientTests(unittest.TestCase):
    def test_uses_timeout_and_paginates(self):
        first = Mock(status_code=200)
        first.json.return_value = {
            "data": [{"id": 1}],
            "pagination": {"hasNextPage": True, "endCursor": "next"},
        }
        second = Mock(status_code=200)
        second.json.return_value = {
            "data": [{"id": 2}],
            "pagination": {"hasNextPage": False},
        }
        session = Mock()
        session.get.side_effect = [first, second]
        client = SamsaraClient(
            "token",
            EndpointRateLimiter(),
            session=session,
            timeout=(1, 2),
        )

        self.assertEqual(
            client.get_all_data("endpoint", {"limit": 1}), [{"id": 1}, {"id": 2}]
        )
        self.assertEqual(session.get.call_args_list[0].kwargs["timeout"], (1, 2))
        self.assertEqual(
            session.get.call_args_list[1].kwargs["params"]["after"], "next"
        )

    def test_does_not_hide_client_errors(self):
        response = Mock(status_code=401, text="unauthorized")
        error = requests.HTTPError("unauthorized", response=response)
        response.raise_for_status.side_effect = error
        session = Mock()
        session.get.return_value = response
        client = SamsaraClient("token", EndpointRateLimiter(), session=session)

        with self.assertRaises(requests.HTTPError):
            client.get_all_data("endpoint")

    def test_dynamic_urls_share_the_endpoint_template_rate_limit(self):
        response = Mock(status_code=200)
        response.json.return_value = {"data": [], "pagination": {}}
        session = Mock()
        session.get.return_value = response
        rate_limiter = Mock()
        client = SamsaraClient("token", rate_limiter, session=session)

        client.get_all_data(
            "v1/fleet/vehicles/123/safety/score",
            max_calls_per_second=3,
            rate_limit_key="v1/fleet/vehicles/{vehicleId}/safety/score",
        )

        rate_limiter.acquire.assert_called_once_with(
            "v1/fleet/vehicles/{vehicleId}/safety/score", 3
        )

        self.assertEqual(session.get.call_count, 1)
