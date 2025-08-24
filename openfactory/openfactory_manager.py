"""
OpenFactory Manager API.

This module provides the `OpenFactoryManager` class, which manages the deployment, configuration,
and teardown of devices and applications within an OpenFactory environment.

Core responsibilities:
    - Deploy MTConnect agents, supervisors, and OpenFactory applications
    - Manage Docker-based services via the configured deployment strategy
    - Register and deregister assets in the OpenFactory environment
    - Integrate deployed services with Kafka, ksqlDB, and other OpenFactory components
    - Validate configuration files against the UNS schema
    - Notify users of deployment results, warnings, and failures

Key integrations:
    - Docker for container lifecycle management
    - Kafka and ksqlDB for data streaming and querying
    - UNS Schema for configuration validation
    - User notifications for communicating operational outcomes
    - Plugin system for selecting deployment strategies

Error handling:
    - Raises `OFAException` for critical operational failures
    - Catches and logs Docker API errors
    - Skips existing or invalid deployments without stopping other deployments
"""

import docker
import json
import openfactory.config as config
from openfactory import OpenFactory
from openfactory.schemas.devices import Device, get_devices_from_config_file
from openfactory.schemas.apps import OpenFactoryAppSchema, get_apps_from_config_file
from openfactory.schemas.uns import UNSSchema
from openfactory.schemas.common import constraints, cpus_limit, cpus_reservation
from openfactory.assets import Asset
from openfactory.exceptions import OFAException
from openfactory.models.user_notifications import user_notify
from openfactory.utils import register_asset, deregister_asset, load_plugin
from openfactory.kafka.ksql import KSQLDBClient
from openfactory.connectors.mtconnect.mtc_connector import MTConnectConnector
from openfactory.openfactory_deploy_strategy import OpenFactoryServiceDeploymentStrategy, SwarmDeploymentStrategy


class OpenFactoryManager(OpenFactory):
    """
    OpenFactory Manager API.

    Allows to deploy services on OpenFactory.

    Important:
        User requires Docker access on the OpenFactory cluster.

    Attributes:
        deployment_strategy (OpenFactoryServiceDeploymentStrategy): The strategy used to deploy services.
    """

    def __init__(self, ksqlClient: KSQLDBClient,
                 bootstrap_servers: str = config.KAFKA_BROKER):
        """
        Initializes the OpenFactoryManager.

        Args:
            ksqlClient (KSQLDBClient): The client for interacting with ksqlDB.
            bootstrap_servers (str): The Kafka bootstrap server address. Defaults to config.KAFKA_BROKER.

        Note:
            The deployment strategy to use (e.g., swarm or docker) is selected based on `config.DEPLOYMENT_PLATFORM`
        """
        super().__init__(ksqlClient, bootstrap_servers)

        platform_cls = load_plugin("openfactory.deployment_platforms", config.DEPLOYMENT_PLATFORM)
        if not issubclass(platform_cls, OpenFactoryServiceDeploymentStrategy):
            raise TypeError(
                f"Plugin '{config.DEPLOYMENT_PLATFORM}' must inherit from OpenFactoryServiceDeploymentStrategy"
            )

        self.deployment_strategy: OpenFactoryServiceDeploymentStrategy = platform_cls()
        self.deployment_strategy = platform_cls()

        self.connectors = {
            "mtconnect": MTConnectConnector(self.deployment_strategy, self.ksql, self.bootstrap_servers),
            # Add other connectors here
        }

    def deploy_device_supervisor(self, device: Device) -> None:
        """
        Deploy an OpenFactory device supervisor.

        Args:
            device (Device): The device for which the supervisor is to be deployed.

        Raises:
            OFAException: If the supervisor cannot be deployed.
        """
        if device.supervisor is None:
            return

        # build environment variables
        supervisor_uuid = f"{device.uuid.upper()}-SUPERVISOR"
        env = [f"SUPERVISOR_UUID={supervisor_uuid}",
               f"DEVICE_UUID={device.uuid}",
               f"KAFKA_BROKER={self.bootstrap_servers}",
               f"KSQLDB_URL={self.ksql.ksqldb_url}",
               f"ADAPTER_IP={device.supervisor.adapter.ip}",
               f"ADAPTER_PORT={device.supervisor.adapter.port}",
               f"KSQLDB_LOG_LEVEL={config.KSQLDB_LOG_LEVEL}"]

        if device.supervisor.adapter.environment is not None:
            for item in device.supervisor.adapter.environment:
                var, val = item.split('=')
                env.append(f"{var.strip()}={val.strip()}")

        try:
            self.deployment_strategy.deploy(
                image=device.supervisor.image,
                name=device.uuid.lower() + '-supervisor',
                mode={"Replicated": {"Replicas": 1}},
                env=env,
                networks=[config.OPENFACTORY_NETWORK],
                resources={
                    "Limits": {"NanoCPUs": int(1000000000*cpus_limit(device.supervisor.deploy, 1.0))},
                    "Reservations": {"NanoCPUs": int(1000000000*cpus_reservation(device.supervisor.deploy, 0.5))}
                    },
                constraints=constraints(device.supervisor.deploy)
            )
        except docker.errors.APIError as err:
            user_notify.fail(f"Supervisor {device.uuid.lower()}-supervisor could not be deployed\n{err}")
            return
        register_asset(supervisor_uuid, uns=None, asset_type='Supervisor',
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers, docker_service=device.uuid.lower() + '-supervisor')
        dev = Asset(device.uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        dev.add_reference_below(supervisor_uuid)
        sup = Asset(supervisor_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        sup.add_reference_above(device.uuid)

        user_notify.success(f"Supervisor {supervisor_uuid} deployed successfully")

    def deploy_openfactory_application(self, application: OpenFactoryAppSchema) -> None:
        """
        Deploy an OpenFactory application.

        Args:
            application (OpenFactoryAppSchema): The application configuration.

        Raises:
            OFAException: If the application cannot be deployed.
        """
        # build environment variables
        env = [f"APP_UUID={application.uuid}",
               f"KAFKA_BROKER={self.bootstrap_servers}",
               f"KSQLDB_URL={self.ksql.ksqldb_url}",
               f"DOCKER_SERVICE={application.uuid.lower()}"]

        # Add STORAGE only if not None
        if application.storage is not None:
            # Serialize storage config as JSON
            storage_dict = application.storage.model_dump(exclude_none=True)
            env.append(f"STORAGE={json.dumps(storage_dict)}")

        if application.environment is not None:
            for item in application.environment:
                var, val = item.split('=')
                env.append(f"{var.strip()}={val.strip()}")

        # if KSQLDB_LOG_LEVEL is not set by user, set it to the default value
        if not any(var.startswith("KSQLDB_LOG_LEVEL=") for var in env):
            env.append(f"KSQLDB_LOG_LEVEL={config.KSQLDB_LOG_LEVEL}")

        # add storage
        mounts = []
        if application.storage:
            backend_instance = application.storage.create_backend_instance()
            if isinstance(self.deployment_strategy, SwarmDeploymentStrategy):
                if not backend_instance.compatible_with_swarm():
                    raise ValueError(f"{type(backend_instance).__name__} cannot be used with SwarmDeploymentStrategy")
            mount_spec = backend_instance.get_mount_spec()
            if mount_spec:
                mounts.append(mount_spec)

        try:
            self.deployment_strategy.deploy(
                image=application.image,
                name=application.uuid.lower(),
                mode={"Replicated": {"Replicas": 1}},
                env=env,
                networks=[config.OPENFACTORY_NETWORK],
                mounts=mounts
            )
        except docker.errors.APIError as err:
            user_notify.fail(f"Application {application.uuid} could not be deployed\n{err}")
            return

        register_asset(application.uuid, uns=application.uns, asset_type='OpenFactoryApp',
                       ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers, docker_service=application.uuid.lower())
        user_notify.success(f"Application {application.uuid} deployed successfully")

    def deploy_devices_from_config_file(self, yaml_config_file: str) -> None:
        """
        Deploy OpenFactory devices from a YAML configuration file.

        This method loads and validates the UNS schema, parses the device configurations
        from the specified YAML file, and deploys each device that is not already deployed.

        Deployment includes registering the device asset, deploying the MTConnect agent,
        Kafka producer, KSQLDB tables (if defined), and device supervisor (if defined).

        Args:
            yaml_config_file (str): Path to the YAML configuration file containing device definitions.

        Note:
            - If the UNS schema is invalid, a failure notification will be triggered, and deployment will abort early.
            - If device configurations fail to load or validate, deployment will abort early after notifying the user.
            - Deployment skips devices that are already deployed.
        """
        # load UNS schema and yaml description file
        try:
            uns_schema = UNSSchema(schema_yaml_file=config.OPENFACTORY_UNS_SCHEMA)
        except ValueError as e:
            user_notify.fail(f"The UNS schema '{config.OPENFACTORY_UNS_SCHEMA}' is invalid: {e}")
            return

        # load devices
        devices = get_devices_from_config_file(yaml_config_file, uns_schema)
        if devices is None:
            return

        for dev_name, device in devices.items():
            user_notify.info(f"{dev_name} - {device.uuid}:")

            if device.uuid in self.devices_uuid():
                user_notify.info(f"Device {device.uuid} exists already and was not deployed")
                continue

            if device.connector.type not in self.connectors:
                user_notify.warning(f"Device {device.uuid} has an unknown connector {device.connector.type}")
                continue

            self.connectors[device.connector.type].deploy(device, yaml_config_file)
            self.deploy_device_supervisor(device)

            user_notify.success(f"Device {device.uuid} deployed successfully")

    def deploy_apps_from_config_file(self, yaml_config_file: str) -> None:
        """
        Deploy OpenFactory applications from a YAML configuration file.

        This method loads and validates the UNS schema, parses the application
        configurations from the specified YAML file, and deploys each application
        that is not already deployed.

        Args:
            yaml_config_file (str): Path to the YAML configuration file containing application definitions.

        Note:
            - If the UNS schema is invalid, a failure notification will be triggered, and deployment will abort early.
            - If application configurations fail to load or validate, deployment will abort early after notifying the user.
            - Deployment skips applications that are already deployed.
        """
        # load UNS schema and yaml description file
        try:
            uns_schema = UNSSchema(schema_yaml_file=config.OPENFACTORY_UNS_SCHEMA)
        except ValueError as e:
            user_notify.fail(f"The UNS schema '{config.OPENFACTORY_UNS_SCHEMA}' is invalid: {e}")
            return

        # load apps
        apps = get_apps_from_config_file(yaml_config_file, uns_schema)
        if apps is None:
            return

        for app_name, app in apps.items():
            user_notify.info(f"{app_name}:")
            if app.uuid in self.applications_uuid():
                user_notify.info(f"Application {app.uuid} exists already and was not deployed")
                continue

            self.deploy_openfactory_application(app)

    def tear_down_device(self, device_uuid: str) -> None:
        """
        Tear down a device deployed on OpenFactory.

        Args:
            device_uuid (str): The UUID of the device to be torn down.

        Raises:
            OFAException: If the device cannot be torn down.
        """
        # tear down Adapter
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-adapter')
            user_notify.success(f"Adapter for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            # no adapter running as a Docker swarm service
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)

        # tear down Producer
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-producer')
            deregister_asset(device_uuid + '-PRODUCER', ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"Kafka producer for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            user_notify.info(f"Kafka producer for device {device_uuid} was not running")
        except docker.errors.APIError as err:
            raise OFAException(err)

        # tear down Agent
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-agent')
            deregister_asset(device_uuid + '-AGENT', ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"MTConnect Agent for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            # no agent running as a Docker swarm service
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)

        # tear down Supervisor
        try:
            self.deployment_strategy.remove(device_uuid.lower() + '-supervisor')
            deregister_asset(f"{device_uuid.upper()}-SUPERVISOR", ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"Supervisor for device {device_uuid} shut down successfully")
        except docker.errors.NotFound:
            # no supervisor
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)

        deregister_asset(device_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        user_notify.success(f"Device {device_uuid} shut down successfully")

    def shut_down_devices_from_config_file(self, yaml_config_file: str) -> None:
        """
        Shut down devices based on a config file.

        Args:
            yaml_config_file (str): Path to the yaml configuration file.

        Raises:
            OFAException: If the device cannot be shut down.
        """
        # Load yaml description file
        uns_schema = UNSSchema(schema_yaml_file=config.OPENFACTORY_UNS_SCHEMA)
        devices = get_devices_from_config_file(yaml_config_file, uns_schema=uns_schema)
        if devices is None:
            return

        uuid_list = [device.asset_uuid for device in self.devices()]

        for dev_name, device in devices.items():
            user_notify.info(f"{dev_name}:")
            if device.uuid not in uuid_list:
                user_notify.info(f"No device {device.uuid} deployed in OpenFactory")
                continue

            # Tear down Connector
            self.connectors[device.connector.type].tear_down(device.uuid)
            self.tear_down_device(device.uuid)

            # Tear down Supervisor
            try:
                self.deployment_strategy.remove(device.uuid.lower() + '-supervisor')
                deregister_asset(f"{device.uuid.upper()}-SUPERVISOR", ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
                user_notify.success(f"Supervisor for device {device.uuid} shut down successfully")
            except docker.errors.NotFound:
                # No supervisor
                pass
            except docker.errors.APIError as err:
                raise OFAException(err)

            deregister_asset(device.uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            user_notify.success(f"Device {device.uuid} shut down successfully")

    def tear_down_application(self, app_uuid: str) -> None:
        """
        Tear down a deployed OpenFactory application.

        Args:
            app_uuid (str): The UUID of the application to be torn down.

        Raises:
            OFAException: If the application cannot be torn down.
        """
        try:
            app = Asset(app_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
            self.deployment_strategy.remove(app.DockerService.value)
        except docker.errors.NotFound:
            # the application was not running as a Docker swarm service
            deregister_asset(app_uuid, ksqlClient=self.ksql)
            pass
        except docker.errors.APIError as err:
            raise OFAException(err)
        deregister_asset(app_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        user_notify.success(f"OpenFactory application {app_uuid} shut down successfully")

    def shut_down_apps_from_config_file(self, yaml_config_file: str) -> None:
        """
        Shut down OpenFactory applications based on a config file.

        Args:
            yaml_config_file (str): Path to the yaml configuration file.

        Raises:
            OFAException: If the application cannot be shut down.
        """
        # Load yaml description file
        uns_schema = UNSSchema(schema_yaml_file=config.OPENFACTORY_UNS_SCHEMA)
        apps = get_apps_from_config_file(yaml_config_file, uns_schema)
        if apps is None:
            return

        for app_name, app in apps.items():
            user_notify.info(f"{app_name}:")
            if app.uuid not in self.applications_uuid():
                user_notify.info(f"No application {app.uuid} deployed in OpenFactory")
                continue

            self.tear_down_application(app.uuid)

    def get_asset_uuid_from_docker_service(self, docker_service_name: str) -> str:
        """
        Return ASSET_UUID of the asset running on the Docker service docker_service_name.

        Args:
            docker_service_name (str): The name of the Docker service.

        Returns:
            str: The ASSET_UUID of the asset running on the Docker service.
        """
        query = f"select ASSET_UUID from DOCKER_SERVICES where DOCKER_SERVICE='{docker_service_name}';"
        df = self.ksql.query(query)
        if df.empty:
            return ""
        return df['ASSET_UUID'][0]
