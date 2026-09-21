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

        self.assertEqual(session.get.call_count, 1)
