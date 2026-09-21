import unittest

from modules.utils import parallelize_execution


class RecordingLogger:
    def __init__(self):
        self.errors = []
        self.infos = []

    def error(self, message):
        self.errors.append(message)

    def info(self, message):
        self.infos.append(message)


class ParallelExecutionTests(unittest.TestCase):
    def test_propagates_task_failures(self):
        logger = RecordingLogger()

        def fail():
            raise ValueError("boom")

        with self.assertRaisesRegex(RuntimeError, "1 tâche"):
            parallelize_execution([object()], fail, logger, max_workers=1)

        self.assertTrue(logger.errors)

    def test_reports_parallel_progress(self):
        logger = RecordingLogger()

        results = parallelize_execution(
            [1, 2, 3], lambda: "done", logger, max_workers=2
        )

        self.assertEqual(results, ["done", "done", "done"])
        self.assertTrue(any("Démarrage de 3" in message for message in logger.infos))
        self.assertTrue(any("3/3" in message for message in logger.infos))
