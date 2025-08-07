import unittest
from pydantic import ValidationError
from openfactory.schemas.connectors.mtconnect import Agent


class TestAgent(unittest.TestCase):
    """
    Unit tests for MTConnect Agent
    """

    def test_valid_external_agent(self):
        """ Test valid Agent with external IP (no device_xml or adapter). """
        data = {
            "ip": "10.0.0.1",
            "port": 5000,
        }
        agent = Agent(**data)
        self.assertEqual(agent.ip, "10.0.0.1")
        self.assertIsNone(agent.device_xml)
        self.assertIsNone(agent.adapter)

    def test_valid_internal_agent(self):
        """ Test valid Agent with device_xml and adapter (no IP). """
        data = {
            "port": 8080,
            "device_xml": "xml1",
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
            }
        }
        agent = Agent(**data)
        self.assertEqual(agent.device_xml, "xml1")
        self.assertIsNotNone(agent.adapter)

    def test_extra_fields_forbid_raises_validation_error(self):
        """ Providing undefined fields for an app should raise ValidationError """
        invalid_config = {
            "port": 8080,
            "device_xml": "xml1",
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
                },
            "foo": "bar",
        }

        with self.assertRaises(ValidationError) as cm:
            Agent(**invalid_config)

        # Basic sanity checks: the error mentions the offending field
        err_str = str(cm.exception)
        self.assertIn("foo", err_str)
        self.assertIn("extra", err_str.lower())

    def test_agent_with_deploy(self):
        """ Test agent with optional deploy field. """
        data = {
            "port": 8080,
            "device_xml": "xml1",
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
            },
            "deploy": {
                "replicas": 2
            }
        }
        agent = Agent(**data)
        self.assertEqual(agent.deploy.replicas, 2)

    def test_ip_none_behaves_as_internal(self):
        """ Explicit ip=None should trigger internal agent validation. """
        data = {
            "ip": None,
            "port": 5000,
            "device_xml": "xml1"
            # missing adapter
        }

        with self.assertRaises(ValueError) as ctx:
            Agent(**data)

        self.assertIn("'adapter' definition is missing", str(ctx.exception))

    def test_missing_device_xml(self):
        """
        Test error raised if device_xml missing
        """

        data = {
            "port": 5000,
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
                }
        }

        with self.assertRaises(ValueError) as context:
            Agent(**data)

        # Check error message
        self.assertIn("'device_xml' is missing", str(context.exception))

    def test_missing_adapter(self):
        """
        Test error raised if adapter missing
        """

        data = {
            "port": 5000,
            "device_xml": "xml1",
        }

        with self.assertRaises(ValueError) as context:
            Agent(**data)

        # Check error message
        self.assertIn("'adapter' definition is missing", str(context.exception))

    def test_adapter_external_agent(self):
        """
        Test error raised if adapter is defined for an external agent
        """

        data = {
            "ip": "10.0.0.1",
            "port": 5000,
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
                }
        }

        with self.assertRaises(ValueError) as context:
            Agent(**data)

        # Check error message
        self.assertIn("'adapter' can not be defined for an external agent", str(context.exception))

    def test_device_xml_external_agent(self):
        """
        Test error raised if device_xml is defined for an external agent
        """

        data = {
            "ip": "10.0.0.1",
            "port": 5000,
            "device_xml": "dev.xml"
        }

        with self.assertRaises(ValueError) as context:
            Agent(**data)

        # Check error message
        self.assertIn("'device_xml' can not be defined for an external agent", str(context.exception))

    def test_deploy_defaults_set_when_missing(self):
        """ Test that deploy is set to Deploy(replicas=1) when not provided. """
        data = {
            "port": 8080,
            "device_xml": "xml1",
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
            }
            # no deploy provided
        }
        agent = Agent(**data)
        self.assertIsNotNone(agent.deploy)
        self.assertEqual(agent.deploy.replicas, 1)

    def test_deploy_replicas_defaults_to_1_when_none(self):
        """ Test that deploy.replicas is set to 1 when explicitly None. """
        data = {
            "port": 8080,
            "device_xml": "xml1",
            "adapter": {
                "ip": "1.2.3.4",
                "port": 7878
            },
            "deploy": {
                "replicas": None
            }
        }
        agent = Agent(**data)
        self.assertIsNotNone(agent.deploy)
        self.assertEqual(agent.deploy.replicas, 1)
