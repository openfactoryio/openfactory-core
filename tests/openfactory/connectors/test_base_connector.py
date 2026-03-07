import unittest
from openfactory.connectors.base_connector import Connector
from openfactory.schemas.devices import Device


class TestConnectorABC(unittest.TestCase):
    """
    Unit tests for the Connector abstract base class
    """

    def test_cannot_instantiate_abstract_connector(self):
        """ Verify that Connector cannot be instantiated directly """
        with self.assertRaises(TypeError):
            Connector()

    def test_subclass_without_methods_fails(self):
        """ Verify that a subclass missing abstract methods cannot be instantiated """
        class IncompleteConnector(Connector):
            pass

        with self.assertRaises(TypeError):
            IncompleteConnector()

    def test_subclass_with_methods_instantiates(self):
        """ Verify that a concrete subclass can be instantiated """
        class DummyConnector(Connector):
            def deploy(self, device: Device, yaml_config_file: str) -> None:
                self.called = True

            def tear_down(self, device_uuid: str) -> None:
                self.called = True

        dummy = DummyConnector()
        self.assertIsInstance(dummy, DummyConnector)
