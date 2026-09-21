import unittest

from modules.utils import parallelize_execution


class RecordingLogger:
    def __init__(self):
        self.errors = []

    def error(self, message):
        self.errors.append(message)


class ParallelExecutionTests(unittest.TestCase):
    def test_propagates_task_failures(self):
        logger = RecordingLogger()

        def fail():
            raise ValueError("boom")

        with self.assertRaisesRegex(RuntimeError, "1 tâche"):
            parallelize_execution([object()], fail, logger, max_workers=1)

        self.assertTrue(logger.errors)
