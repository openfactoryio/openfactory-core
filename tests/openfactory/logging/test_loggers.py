import unittest
from openfactory.logging.loggers import OpenFactoryLogger, OpenFactoryLoggerAdapter


class TestOpenFactoryLogger(unittest.TestCase):
    """ Tests for OpenFactoryLogger. """

    def test_with_context_returns_logger_adapter(self):
        """ with_context should return an OpenFactoryLoggerAdapter. """

        logger = OpenFactoryLogger("test")
        adapter = logger.with_context(asset_uuid="DEVICE-1")
        self.assertIsInstance(adapter, OpenFactoryLoggerAdapter)

    def test_with_context_sets_context(self):
        """ with_context should attach the supplied context. """

        logger = OpenFactoryLogger("test")
        adapter = logger.with_context(asset_uuid="DEVICE-1")
        self.assertEqual(adapter.extra, {"asset_uuid": "DEVICE-1"})


class TestOpenFactoryLoggerAdapter(unittest.TestCase):
    """ Tests for OpenFactoryLoggerAdapter. """

    def setUp(self):
        self.logger = OpenFactoryLogger("test")

    def test_process_adds_context(self):
        """ Adapter context should be added to the log record. """

        adapter = OpenFactoryLoggerAdapter(self.logger, {"asset_uuid": "DEVICE-1"})
        _, kwargs = adapter.process("hello", {})
        self.assertEqual(kwargs["extra"], {"asset_uuid": "DEVICE-1"})

    def test_process_merges_extra(self):
        """ Call-specific extra values should be merged with adapter context. """

        adapter = OpenFactoryLoggerAdapter(
            self.logger,
            {"asset_uuid": "DEVICE-1"},
        )

        _, kwargs = adapter.process(
            "hello",
            {"extra": {"temperature": 42.5}},
        )

        self.assertEqual(
            kwargs["extra"],
            {
                "asset_uuid": "DEVICE-1",
                "temperature": 42.5,
            },
        )

    def test_process_adapter_context_overrides_extra(self):
        """ Adapter context should override duplicate extra values. """

        adapter = OpenFactoryLoggerAdapter(
            self.logger,
            {"asset_uuid": "DEVICE-1"},
        )

        _, kwargs = adapter.process(
            "hello",
            {"extra": {"asset_uuid": "DEVICE-2"}},
        )

        self.assertEqual(kwargs["extra"], {"asset_uuid": "DEVICE-1"})

    def test_with_context_preserves_existing_context(self):
        """ with_context should preserve existing context. """

        adapter = OpenFactoryLoggerAdapter(
            self.logger,
            {"gateway": "opcua"},
        )

        child = adapter.with_context(
            asset_uuid="DEVICE-1",
        )

        self.assertEqual(
            child.extra,
            {
                "gateway": "opcua",
                "asset_uuid": "DEVICE-1",
            },
        )

    def test_with_context_overrides_existing_context(self):
        """ with_context should override duplicate context fields. """

        adapter = OpenFactoryLoggerAdapter(
            self.logger,
            {
                "gateway": "opcua",
                "asset_uuid": "OLD",
            },
        )

        child = adapter.with_context(
            asset_uuid="NEW",
        )

        self.assertEqual(
            child.extra,
            {
                "gateway": "opcua",
                "asset_uuid": "NEW",
            },
        )

    def test_process_does_not_modify_extra(self):
        """ process should not modify the caller's extra dictionary. """

        adapter = OpenFactoryLoggerAdapter(self.logger, {"asset_uuid": "DEVICE-1"})

        extra = {"temperature": 42.5}
        adapter.process("hello", {"extra": extra})

        self.assertEqual(extra, {"temperature": 42.5})
