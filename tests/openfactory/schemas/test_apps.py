import unittest
import os
import yaml
from unittest.mock import patch
from pydantic import ValidationError
from tempfile import NamedTemporaryFile
from openfactory.schemas.apps import OpenFactoryAppsConfig, RoutingError, get_apps_from_config_file
from openfactory.schemas.uns import UNSSchema


class TestOpenFactoryAppsConfig(unittest.TestCase):
    """
    Unit tests for class OpenFactoryAppsConfig
    """

    def setUp(self):
        # Define a valid UNS schema
        self.schema_data = {
            "namespace_structure": [
                {"inc": "OpenFactory"},
                {"workcenter": ["WC1", "WC2"]},
                {"asset": "ANY"},
                {"attribute": "ANY"}
            ],
            "uns_template": "inc/workcenter/asset/attribute"
        }

        # Write to temporary YAML file
        self.uns_schema_file = NamedTemporaryFile(mode="w+", delete=False)
        yaml.dump(self.schema_data, self.uns_schema_file)
        self.uns_schema_file.close()

        # Load actual UNSSchema instance
        self.uns_schema = UNSSchema(schema_yaml_file=self.uns_schema_file.name)

        # --- patch base domain
        self.base_domain_patcher = patch(
            "openfactory.schemas.apps.Config.OPENFACTORY_BASE_DOMAIN",
            "example.com",
            create=True
        )
        self.base_domain_patcher.start()

    def tearDown(self):
        os.remove(self.uns_schema_file.name)

    def test_valid_config(self):
        """ Test a valid configuration """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "environment": [
                        "KAFKA_BROKER=broker:9092",
                        "KSQL_URL=http://ksqldb-server:8088",
                    ],
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        self.assertEqual(config.apps["demo1"].uuid, "DEMO-APP")
        self.assertEqual(config.apps["demo1"].image, "demofact/demo1")
        self.assertIn("KAFKA_BROKER=broker:9092", config.apps["demo1"].environment)

    def test_extra_fields_forbid_raises_validation_error(self):
        """ Providing undefined fields for an app should raise ValidationError """
        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    # extra undefined field -> should trigger ValidationError
                    "foo": "bar",
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        # Basic sanity checks: the error mentions the offending field
        err_str = str(cm.exception)
        self.assertIn("foo", err_str)
        self.assertIn("extra", err_str.lower())

    def test_missing_required_fields(self):
        """ Test missing required fields (uuid & image) """
        invalid_config = {
            "apps": {
                "demo1": {
                    "environment": ["KAFKA_BROKER=broker:9092"]
                }
            }
        }

        with self.assertRaises(ValidationError) as context:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("uuid", str(context.exception))
        self.assertIn("image", str(context.exception))

    def test_missing_apps_key(self):
        """ Test missing `apps` key"""
        invalid_config = {
            "devices": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "environment": [
                        "KAFKA_BROKER=broker:9092",
                        "KSQL_URL=http://ksqldb-server:8088",
                    ],
                }
            }
        }

        with self.assertRaises(ValidationError) as context:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("apps", str(context.exception))

    def test_runtime_uid_gid_valid(self):
        """ runtime_uid and runtime_gid are parsed correctly """

        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "runtime_uid": 1234,
                    "runtime_gid": 5678
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)

        app = config.apps["demo1"]

        self.assertEqual(app.runtime_uid, 1234)
        self.assertEqual(app.runtime_gid, 5678)

    def test_runtime_uid_without_gid_invalid(self):
        """ runtime_uid without runtime_gid should raise ValidationError """

        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "runtime_uid": 1234
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn(
            "runtime_uid and runtime_gid must either both be defined or both be omitted",
            str(cm.exception)
        )

    def test_runtime_gid_without_uid_invalid(self):
        """ runtime_gid without runtime_uid should raise ValidationError """

        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "runtime_gid": 5678
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn(
            "runtime_uid and runtime_gid must either both be defined or both be omitted",
            str(cm.exception)
        )

    def test_runtime_uid_zero_invalid(self):
        """ runtime_uid must be >= 1 """

        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "runtime_uid": 0,
                    "runtime_gid": 5678
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("greater than or equal to 1", str(cm.exception))

    def test_runtime_gid_zero_invalid(self):
        """ runtime_gid must be >= 1 """

        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "runtime_uid": 1234,
                    "runtime_gid": 0
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("greater than or equal to 1", str(cm.exception))

    @patch("openfactory.schemas.apps.load_yaml")
    def test_runtime_uid_gid_integration_with_get_apps_from_config_file(self, mock_load_yaml):
        """ runtime_uid and runtime_gid are correctly parsed from YAML """

        mock_load_yaml.return_value = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "uns": {"workcenter": "WC2"},
                    "image": "demofact/demo1",
                    "runtime_uid": 1234,
                    "runtime_gid": 5678
                }
            }
        }

        result = get_apps_from_config_file(
            "dummy.yaml",
            self.uns_schema
        )

        self.assertIsNotNone(result)

        app = result["demo1"]

        self.assertEqual(app.runtime_uid, 1234)
        self.assertEqual(app.runtime_gid, 5678)

    def test_invalid_environment_type(self):
        """ Test invalid environment type (string instead of list) """
        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "environment": "KAFKA_BROKER=broker:9092"
                }
            }
        }

        with self.assertRaises(ValidationError) as context:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("environment", str(context.exception))

    def test_optional_environment(self):
        """ Test environment field is optional """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1"
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        self.assertIsNone(config.apps["demo1"].environment)

    @patch("openfactory.schemas.apps.load_yaml", return_value={"invalid": "data"})
    @patch("openfactory.models.user_notifications.user_notify.fail")
    def test_invalid_yaml_file(self, mock_notify, mock_load_yaml):
        """ Test invalid YAML file handling """
        result = get_apps_from_config_file("dummy_path.yaml", self.uns_schema)
        self.assertIsNone(result)
        mock_notify.assert_called_once()
        self.assertIn("invalid format", mock_notify.call_args[0][0])

    @patch("openfactory.schemas.apps.load_yaml")
    def test_valid_yaml_file(self, mock_load_yaml):
        """ Test a valid YAML file """
        mock_load_yaml.return_value = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "uns": {"workcenter": "WC2"},
                    "image": "demofact/demo1",
                    "environment": ["KAFKA_BROKER=broker:9092"]
                }
            }
        }

        result = get_apps_from_config_file("valid_config.yaml", self.uns_schema)
        self.assertIsNotNone(result)
        app = result["demo1"]
        self.assertEqual(app.uuid, "DEMO-APP")
        self.assertEqual(app.image, "demofact/demo1")

        self.assertIsNotNone(app.uns)
        self.assertEqual(app.uns["uns_id"], "OpenFactory/WC2/DEMO-APP")
        self.assertEqual(app.uns["levels"], {
            "inc": "OpenFactory",
            "workcenter": "WC2",
            "asset": "DEMO-APP"
        })

        self.assertIn("KAFKA_BROKER=broker:9092", app.environment)

    def test_valid_nfs_storage_backend(self):
        """ Test OpenFactoryApp with a valid NFSBackendConfig """
        app_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "storage": {
                        "type": "nfs",
                        "server": "192.168.1.10",
                        "remote_path": "/exports/data",
                        "mount_point": "/mnt/data",
                        "mount_options": ["rw", "vers=4.1", "noatime"]
                    }
                }
            }
        }
        config = OpenFactoryAppsConfig(**app_config)
        storage = config.apps["demo1"].storage
        self.assertEqual(storage.type, "nfs")
        self.assertEqual(storage.server, "192.168.1.10")
        self.assertEqual(storage.mount_point, "/mnt/data")

    def test_invalid_storage_type(self):
        """ Test that unknown storage backend type triggers ValidationError with correct message """

        app_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "storage": {
                        "type": "unknown",
                        "foo": "bar"
                    }
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**app_config)

        errors = cm.exception.errors()

        # There should be a union_tag_invalid error for 'storage'
        storage_error = [
            e for e in errors
            if e['loc'][-1] == 'storage' and e['type'] == 'union_tag_invalid'
        ]

        self.assertTrue(storage_error, "ValidationError should include 'union_tag_invalid' for 'storage' field")

    def test_optional_networks(self):
        """ networks field is optional """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1"
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        self.assertIsNone(config.apps["demo1"].networks)

    def test_valid_networks_list(self):
        """ networks can be provided as a list of non-empty strings """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "networks": ["factory-net", "monitoring-net"]
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        self.assertEqual(config.apps["demo1"].networks, ["factory-net", "monitoring-net"])

    def test_networks_empty_string_invalid(self):
        """ networks list with empty strings should raise ValidationError """
        invalid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "networks": ["factory-net", ""]
                }
            }
        }

        with self.assertRaises(ValidationError) as cm:
            OpenFactoryAppsConfig(**invalid_config)

        self.assertIn("Network names must not be empty", str(cm.exception))

    @patch("openfactory.schemas.apps.load_yaml")
    def test_networks_integration_with_get_apps_from_config_file(self, mock_load_yaml):
        """ networks are correctly parsed and attached using get_apps_from_config_file """
        mock_load_yaml.return_value = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "uns": {"workcenter": "WC2"},
                    "image": "demofact/demo1",
                    "networks": ["factory-net", "monitoring-net"]
                }
            }
        }

        result = get_apps_from_config_file("dummy_path.yaml", self.uns_schema)
        self.assertIsNotNone(result)

        app = result["demo1"]
        self.assertEqual(app.networks, ["factory-net", "monitoring-net"])
        # Still has UNS enrichment
        self.assertEqual(app.uns["uns_id"], "OpenFactory/WC2/DEMO-APP")

    def test_optional_deploy(self):
        """ deploy field is optional """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1"
                }
            }
        }
        config = OpenFactoryAppsConfig(**valid_config)
        self.assertIsNone(config.apps["demo1"].deploy)

    def test_deploy_with_replicas_and_resources(self):
        """ deploy field with replicas, resources, and placement """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "deploy": {
                        "replicas": 2,
                        "resources": {
                            "reservations": {"cpus": 0.5, "memory": "512Mi"},
                            "limits": {"cpus": 1.0, "memory": "1Gi"}
                        },
                        "placement": {
                            "constraints": ["node.labels.zone == building-a"]
                        }
                    }
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        deploy = config.apps["demo1"].deploy
        self.assertIsNotNone(deploy)
        self.assertEqual(deploy.replicas, 2)
        self.assertEqual(deploy.resources.reservations.cpus, 0.5)
        self.assertEqual(deploy.resources.limits.memory, "1Gi")
        self.assertEqual(deploy.placement.constraints, ["node.labels.zone == building-a"])

    @patch("openfactory.schemas.apps.load_yaml")
    def test_deploy_integration_with_get_apps_from_config_file(self, mock_load_yaml):
        """ deploy field is correctly parsed and attached using get_apps_from_config_file """
        mock_load_yaml.return_value = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "uns": {"workcenter": "WC2"},
                    "image": "demofact/demo1",
                    "deploy": {
                        "replicas": 3,
                        "resources": {
                            "reservations": {"cpus": 0.5},
                            "limits": {"cpus": 1.0}
                        },
                        "placement": {"constraints": ["node.labels.zone == WC2"]}
                    }
                }
            }
        }

        result = get_apps_from_config_file("dummy_path.yaml", self.uns_schema)
        self.assertIsNotNone(result)

        deploy = result["demo1"].deploy
        self.assertIsNotNone(deploy)
        self.assertEqual(deploy.replicas, 3)
        self.assertEqual(deploy.resources.reservations.cpus, 0.5)
        self.assertEqual(deploy.placement.constraints, ["node.labels.zone == WC2"])

    def test_optional_routing(self):
        """ routing field is optional """
        valid_config = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1"
                }
            }
        }

        config = OpenFactoryAppsConfig(**valid_config)
        self.assertIsNone(config.apps["demo1"].routing)

    def test_routing_basic(self):
        """ routing field is parsed correctly """
        config_data = {
            "apps": {
                "demo1": {
                    "uuid": "DEMO-APP",
                    "image": "demofact/demo1",
                    "routing": {
                        "expose": True,
                        "port": 8000
                    }
                }
            }
        }

        config = OpenFactoryAppsConfig(**config_data)
        routing = config.apps["demo1"].routing

        self.assertTrue(routing.expose)
        self.assertEqual(routing.port, 8000)
        self.assertIsNone(routing.hostname)

    @patch("openfactory.schemas.apps.load_yaml")
    def test_routing_hostname_generation(self, mock_load_yaml):
        """ canonical and alias hostnames are generated correctly """
        mock_load_yaml.return_value = {
            "apps": {
                "My_App": {
                    "uuid": "ABCD1234",
                    "image": "demo",
                    "routing": {
                        "expose": True,
                        "port": 8000,
                        "hostname": "My_Dashboard!!"
                    }
                }
            }
        }

        result = get_apps_from_config_file("dummy.yaml", self.uns_schema)
        app = result["My_App"]
        routing = app.routing

        self.assertEqual(
            routing.canonical_hostname,
            "abcd1234.example.com"
        )
        self.assertEqual(
            routing.alias_hostname,
            "my-dashboard.example.com"
        )

    @patch("openfactory.schemas.apps.load_yaml")
    def test_routing_no_alias(self, mock_load_yaml):
        """ alias is None if hostname not provided """
        mock_load_yaml.return_value = {
            "apps": {
                "demo1": {
                    "uuid": "ABCD1234",
                    "image": "demo",
                    "routing": {
                        "expose": True,
                        "port": 8000
                    }
                }
            }
        }

        result = get_apps_from_config_file("dummy.yaml", self.uns_schema)
        routing = result["demo1"].routing

        self.assertIsNotNone(routing.canonical_hostname)
        self.assertIsNone(routing.alias_hostname)

    def test_routing_hostname_too_long(self):
        """ too long hostname should raise error """
        long_name = "a" * 70

        config_data = {
            "apps": {
                "test_app": {
                    "uuid": long_name,
                    "image": "demo",
                    "routing": {
                        "expose": True,
                        "port": 8000
                    }
                }
            }
        }

        config = OpenFactoryAppsConfig(**config_data)

        with self.assertRaises(RoutingError):
            config.apps["test_app"].routing.build_hostnames(
                app_uuid=long_name,
                base_domain="example.com"
            )

    def test_routing_alias_too_long(self):
        """ too long alias should raise error """
        config_data = {
            "apps": {
                "demo1": {
                    "uuid": "ABCD1234",
                    "image": "demo",
                    "routing": {
                        "expose": True,
                        "port": 8000,
                        "hostname": "a" * 70
                    }
                }
            }
        }

        config = OpenFactoryAppsConfig(**config_data)

        with self.assertRaises(RoutingError):
            config.apps["demo1"].routing.build_hostnames(
                app_uuid="ABCD1234",
                base_domain="example.com"
            )
