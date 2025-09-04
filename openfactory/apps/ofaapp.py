""" Generic OpenFactory application. """

import os
import signal
import time
import json
from types import FrameType
from typing import Optional
from openfactory.utils.assets import deregister_asset
from openfactory.assets import Asset, AssetAttribute
from openfactory.setup_logging import configure_prefixed_logger
from openfactory.schemas.filelayer.storage import StorageBackendSchema
from openfactory.filelayer.backend import FileBackend
import openfactory.config as config


class OpenFactoryApp(Asset):
    """
    Generic OpenFactory application.

    Inherits from `Asset` and extends it to represent an OpenFactory application with standard metadata,
    logging, and lifecycle management.

    Attributes:
        APPLICATION_VERSION (str): Class attribute. Version string from `APPLICATION_VERSION` environment variable or 'latest'.
        APPLICATION_MANUFACTURER (str): Class attribute. Manufacturer from `APPLICATION_MANUFACTURER` environment variable or 'OpenFactory'.
        APPLICATION_LICENSE (str): Class attribute. License string from `APPLICATION_LICENSE` environment variable or 'BSD-3-Clause license'.
        logger (logging.Logger): Instance attribute. Prefixed logger instance configured with the app UUID.
        storage (Optional[FileBackend]): Storage backend instance created from the `STORAGE` environment variable, or `None` if not configured.

    Example usage:
        .. code-block:: python

            import time
            import os
            from openfactory.apps import OpenFactoryApp
            from openfactory.kafka import KSQLDBClient

            class DemoApp(OpenFactoryApp):

                def main_loop(self):
                    # For actual use case, add here your logic of the app
                    print("I don't do anything useful in this example.")
                    counter = 1
                    while True:
                        print(counter)
                        counter += 1
                        time.sleep(2)

                def app_event_loop_stopped(self):
                    # Not absolutely required as it is already done by the `KSQLDBClient` class
                    self.ksql.close()

            # When the Application is deployed on the OpenFactory Cluster, the
            # environment variables KSQLDB_URL and KAFKA_BROKER will be set.
            # The default values can be used for local development.
            app = DemoApp(
                app_uuid='DEMO-APP',
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092")
            )
            app.run()
    """

    # Application version number
    APPLICATION_VERSION = os.getenv('APPLICATION_VERSION', 'latest')
    APPLICATION_MANUFACTURER = os.getenv('APPLICATION_MANUFACTURER', 'OpenFactory')
    APPLICATION_LICENSE = os.getenv('APPLICATION_LICENSE', 'BSD-3-Clause license')

    def __init__(self,
                 app_uuid: str, ksqlClient,
                 bootstrap_servers: str = config.KAFKA_BROKER,
                 loglevel: str = 'INFO'):
        """
        Initializes the OpenFactory application.

        Sets up the application UUID, storage backend (if configured), standard
        attributes (version, manufacturer, license), a prefixed logger, and
        termination signal handlers.

        Args:
            app_uuid (str): The UUID of the application (overrides the environment variable `APP_UUID` if provided).
            ksqlClient (KSQLDBClient): The KSQL client instance.
            bootstrap_servers (str): Kafka bootstrap server URL, default value is read from `config.KAFKA_BROKER`.
            loglevel (str): Logging level for the app (e.g., 'INFO', 'DEBUG'). Defaults to 'INFO'.

        Side effects:
            - Configures logging with the application UUID as prefix.
            - Mounts a storage backend if the `STORAGE` environment variable is set.
            - Registers signal handlers for `SIGINT` and `SIGTERM`.
        """
        # get paramters from environment (set if deployed by ofa deployment tool)
        app_uuid = os.getenv('APP_UUID', app_uuid)
        super().__init__(app_uuid, ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers)

        # attach storage
        storage_env = os.environ.get("STORAGE")
        self.storage: FileBackend | None = None
        if storage_env:
            schema = StorageBackendSchema(storage=json.loads(storage_env))
            self.storage = schema.storage.create_backend_instance()

        # attributes of the application
        self.add_attribute(
            attribute_id='application_version',
            asset_attribute=AssetAttribute(
                value=self.APPLICATION_VERSION,
                type='Events',
                tag='Application.Version'
            )
        )
        self.add_attribute(
            attribute_id='application_manufacturer',
            asset_attribute=AssetAttribute(
                value=self.APPLICATION_MANUFACTURER,
                type='Events',
                tag='Application.Manufacturer'
            )
        )
        self.add_attribute(
            attribute_id='application_license',
            asset_attribute=AssetAttribute(
                value=self.APPLICATION_LICENSE,
                type='Events',
                tag='Application.License'
            )
        )

        # Set up logging
        self.logger = configure_prefixed_logger(
            app_uuid,
            prefix=app_uuid.upper(),
            level=loglevel)
        self.logger.info(f"Setup OpenFactory App {app_uuid}")

        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def welcome_banner(self) -> None:
        """
        Welcome banner printed to stdout.

        Can be redefined by children
        """
        print("--------------------------------------------------------------")
        print(f"Starting OpenFactory App {self.asset_uuid}")
        print("--------------------------------------------------------------")

    def app_event_loop_stopped(self) -> None:
        """ Called when main loop is stopped. """
        pass

    def signal_handler(self, signum: int, frame: Optional[FrameType]) -> None:
        """
        Handles SIGINT and SIGTERM signals, gracefully stopping the application.

        This method listens for termination signals, deregisters the asset from the system,
        and then stops the application’s event loop. It is typically used to handle clean
        shutdowns when the app receives signals like SIGINT or SIGTERM.

        Args:
            signum (int): The signal number that was received (e.g., SIGINT, SIGTERM).
            frame (Optional): The current stack frame when the signal was received.
        """
        signal_name = signal.Signals(signum).name
        self.logger.info(f"Received signal {signal_name}, stopping app gracefully ...")
        deregister_asset(self.asset_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)
        self.app_event_loop_stopped()
        exit(0)

    def main_loop(self) -> None:
        """
        Main loop of the OpenFactory App.

        This method must be implemented by child classes to define the main
        application loop behavior. It will be responsible for managing the
        application's lifecycle, handling events, and maintaining any necessary
        state while the application is running.

        Raises:
            NotImplementedError: If this method is not implemented by a subclass.
        """
        raise NotImplementedError("Method 'main_loop' must be implemented")

    def run(self) -> None:
        """
        Runs the OpenFactory app.

        This method initializes the app by displaying a welcome banner, adding
        an availability attribute, and then starts the main application loop by
        calling `main_loop`. If an exception occurs during the execution of the
        main loop, the error is caught, and the app is gracefully stopped.

        The following steps are performed:

        1. Display the welcome banner.

        2. Add the `avail` attribute with 'AVAILABLE' value.

        3. Start the main loop.

        4. Catch any exceptions that occur and stop the app gracefully.

        Raises:
            Exception: If any exception occurs during the execution of the main loop, it is caught and logged, and the app is stopped.
        """
        self.welcome_banner()
        self.add_attribute('avail', AssetAttribute(
            value='AVAILABLE',
            tag='Availability',
            type='Events'
        ))
        self.logger.info("Starting main loop")
        try:
            self.main_loop()

        except Exception:
            self.logger.exception("An error occurred in the main_loop of the app.")
            self.app_event_loop_stopped()
            deregister_asset(self.asset_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)

    async def async_main_loop(self) -> None:
        """
        Async main loop of the OpenFactory App.

        This method must be implemented by child classes to define the main
        application loop behavior. It will be responsible for managing the
        application's lifecycle, handling events, and maintaining any necessary
        state while the application is running.

        Raises:
            NotImplementedError: If this method is not implemented by a subclass.
        """
        raise NotImplementedError("Method 'async_main_loop' must be implemented")

    async def async_run(self) -> None:
        """
        Run the OpenFactory app asynchronously.

        Args:
            async_main_loop: Async function to use as the main loop
        """
        self.welcome_banner()
        self.add_attribute('avail', AssetAttribute(
            value='AVAILABLE',
            tag='Availability',
            type='Events'
        ))
        self.logger.info("Starting async main loop")
        try:
            await self.async_main_loop()
        except Exception:
            self.logger.exception("An error occurred in the async main loop")
            self.app_event_loop_stopped()
            deregister_asset(self.asset_uuid, ksqlClient=self.ksql, bootstrap_servers=self.bootstrap_servers)


if __name__ == "__main__":

    # Example usage of the OpenFactoryApp
    from openfactory.kafka import KSQLDBClient
    ksql = KSQLDBClient("http://localhost:8088")

    class MyApp(OpenFactoryApp):
        """ Example Application. """

        def main_loop(self):
            """ Main loop. """
            # For actual use case, add here your logic of the app
            print("I am the main loop of the app.\nI don't do anything useful in this example.")
            counter = 1
            while True:
                print(counter)
                counter += 1
                time.sleep(2)

        def app_event_loop_stopped(self):
            """
            Close connection to ksqlDB server.

            Not absolutely required as it is already done by KSQLDBClient class
            """
            self.ksql.close()

    app = MyApp(
        app_uuid='DEMO-APP',
        ksqlClient=ksql,
        bootstrap_servers="localhost:9092"
    )
    app.run()
