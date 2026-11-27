
""" OpenFactory application with an embedded Streamlit web interface. """

import asyncio
import logging
import os
from pathlib import Path
from types import FrameType

try:
    import streamlit as st
    import uvicorn
except ImportError as e:
    raise ImportError(
        "Streamlit is required for OpenFactoryStreamlitApp. "
        "Install with: pip install openfactory[streamlit]"
    ) from e

from openfactory.apps import OpenFactoryApp
from openfactory.kafka import KSQLDBClient
from openfactory.assets import AssetAttribute


class OpenFactoryStreamlitApp(OpenFactoryApp):
    """
    OpenFactory application with an embedded Streamlit web interface.

    Extends :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` by attaching a
    :class:`streamlit.App` application to the OpenFactory runtime.
    This allows developing interactive web interfaces and dashboards alongside the standard
    OpenFactory asset, attribute, and method mechanisms.

    The Streamlit application is available via the :attr:`app` attribute and is served
    by Uvicorn using the ASGI protocol.

    Execution model:
        - :meth:`run` starts both the OpenFactory runtime and an embedded Streamlit server.
        - The Streamlit server and OpenFactory application logic execute concurrently in the asyncio event loop.
        - Streamlit manages its own script execution threads and browser sessions.
        - The OpenFactory application instance persists independently of Streamlit script executions.
        - The HTTP server listens on 0.0.0.0.
        - The listening port is taken from the environment variable ``PORT`` or defaults to 4000.
        - Termination of either the Streamlit server or the OpenFactory application logic initiates application shutdown.

    Customization:
        - Define the Streamlit user interface in a Python script specified by the ``script_path`` constructor parameter.
        - Override :meth:`create_streamlit_app` to customize the Streamlit application.
        - Override :meth:`async_main_loop` to implement asynchronous background tasks.
        - The synchronous :meth:`OpenFactoryApp.main_loop() <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>` is intentionally not supported.

    Attributes:
        app (streamlit.App): Streamlit application instance attached to the OpenFactory app.
        script_path (pathlib.Path): Absolute path to the Streamlit entry-point script.
        log_http_requests (bool): Whether HTTP access logging is enabled.

    .. admonition:: Usage Example (basic Streamlit application)

        .. code-block:: python

            # main.py
            import os
            from openfactory.apps import OpenFactoryStreamlitApp
            from openfactory.kafka import KSQLDBClient

            if __name__ == "__main__":

                app = OpenFactoryStreamlitApp(
                    ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
                    script_path="dashboard.py",
                )

                app.run()

        .. code-block:: python

            # dashboard.py
            import streamlit as st

            st.title("OpenFactory Streamlit Application")
            st.write("Hello OpenFactory!")

        The application is started using:

        .. code-block:: bash

            python main.py

        The Streamlit interface is accessible at ``http://localhost:4000``
        when running locally with the default port.

    For applications requiring access to OpenFactory attributes and methods from the
    Streamlit interface, the OpenFactory application instance can be defined in a separate
    Python module and imported by both the application entry point and the Streamlit script.

    This allows Streamlit to access the existing OpenFactory application instance without
    recreating the asset, its connections, or its command subscriptions on each script rerun.

    .. admonition:: Usage Example (interactive OpenFactory dashboard)

        .. code-block:: python

            # app.py
            import os
            import asyncio
            from pathlib import Path
            from openfactory.apps import (
                OpenFactoryStreamlitApp,
                EventAttribute,
                SampleAttribute,
                ofa_method,
            )
            from openfactory.kafka import KSQLDBClient

            class DemoStreamlitApp(OpenFactoryStreamlitApp):

                status = EventAttribute(value="STOPPED", tag="Machine.Status")
                counter = SampleAttribute(value=0, tag="Machine.Counter")

                @ofa_method(description="Start the machine")
                def start_machine(self):
                    self.status = "RUNNING"

                @ofa_method(description="Stop the machine")
                def stop_machine(self):
                    self.status = "STOPPED"

                @ofa_method(description="Reset the production counter")
                def reset_counter(self):
                    self.counter = 0

                async def async_main_loop(self):
                    while True:
                        if self.status.value == "RUNNING":
                            self.counter = self.counter.value + 1

                        await asyncio.sleep(1)

            app = DemoStreamlitApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
                script_path=Path(__file__).with_name("dashboard.py"),
            )

        .. code-block:: python

            # main.py
            from app import app

            if __name__ == "__main__":
                app.run()

        .. code-block:: python

            # dashboard.py
            import streamlit as st
            from app import app

            st.title("OpenFactory Machine Monitoring")
            st.caption(f"Asset UUID: {app.asset_uuid}")

            col1, col2, col3 = st.columns(3)

            col1.button("Start machine", on_click=app.start_machine)
            col2.button("Stop machine", on_click=app.stop_machine)
            col3.button("Reset counter", on_click=app.reset_counter)

            @st.fragment(run_every="1s")
            def live_monitoring():
                col1, col2 = st.columns(2)
                col1.metric("Machine status", app.status.value)
                col2.metric("Parts produced", app.counter.value)

            live_monitoring()

        The OpenFactory application logic continues to execute independently of
        connected browser sessions. Changes to OpenFactory attributes, whether
        originating from the Streamlit interface, background application logic,
        or OpenFactory commands, are reflected in the dashboard on subsequent
        fragment executions.

        Methods decorated with ``@ofa_method`` remain callable through the
        standard OpenFactory command mechanism and can also be invoked directly
        from the Streamlit interface.

    For applications requiring additional Streamlit configuration, the Streamlit
    application instance can also be customized using the application factory pattern
    using the :meth:`create_streamlit_app` method.

    .. admonition:: Using a Streamlit application factory

        .. code-block:: python

            import os
            import streamlit as st
            from openfactory.apps import OpenFactoryStreamlitApp
            from openfactory.kafka import KSQLDBClient

            class DemoStreamlitApp(OpenFactoryStreamlitApp):

                def create_streamlit_app(self):
                    return st.App(
                        script_path=self.script_path,
                        debug=True,
                    )

            app = DemoStreamlitApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
                script_path="dashboard.py",
            )

            app.run()

    Note:
      - The Streamlit application is accessible via :attr:`app` and behaves like a standard Streamlit ASGI application.
      - The user interface is defined using standard Streamlit Python scripts.
      - Streamlit scripts may be executed repeatedly during browser sessions without restarting the OpenFactory application.
      - Only asynchronous execution is supported for OpenFactory application logic. Subclasses may optionally implement
        :meth:`async_main_loop` for background tasks.
      - The synchronous :meth:`OpenFactoryApp.main_loop() <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>`
        is not supported in this class.
      - OpenFactory features such as attributes, methods, storage backends, and asset communication remain unchanged.
      - HTTP access logging can be enabled or disabled using the ``log_http_requests`` constructor parameter.
      - When deployed on the OpenFactory platform, the ``PORT`` environment variable is set automatically
        by the deployment tool.
      - Applications should be started using :meth:`run` or :meth:`async_run` rather than the ``streamlit run``
        command to ensure that the OpenFactory application lifecycle is managed correctly.
      - When sharing the OpenFactory application instance between the application entry point and Streamlit scripts,
        it must be imported using the same Python module name to avoid duplicate application initialization.
      - The OpenFactory application instance must be initialized in the main thread because
        :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` registers process signal handlers.
      - Application state stored on the OpenFactory instance is shared across browser sessions,
        whereas Streamlit session state is specific to each session.
      - Shared mutable state and resources accessed concurrently by Streamlit script threads, OpenFactory
        background tasks, or command callbacks must be protected appropriately by the application developer.

    .. seealso::
        - :class:`openfactory.apps.ofaapp.OpenFactoryApp`
        - :class:`streamlit.App`
        - `Streamlit documentation <https://docs.streamlit.io/>`_
        - `Streamlit execution flow <https://docs.streamlit.io/develop/api-reference/execution-flow>`_
        - `Uvicorn documentation <https://uvicorn.dev/>`_
    """

    def __init__(
        self,
        ksqlClient: KSQLDBClient,
        script_path: str | Path,
        bootstrap_servers: str | None = None,
        asset_router_url: str | None = None,
        loglevel: str = "INFO",
        log_http_requests: bool = True,
        test_mode: bool = False,
    ) -> None:
        """
        Initialize the OpenFactory Streamlit application.

        This constructor validates the Streamlit entry-point script, forwards the
        OpenFactory-related parameters to
        :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`,
        and creates a :class:`streamlit.App` instance accessible via :attr:`app`.

        The Streamlit application is created using :meth:`create_streamlit_app`,
        which may be overridden by subclasses to customize the embedded
        Streamlit application.

        Args:
            ksqlClient: KSQL client instance.
            script_path: Path to the Python script defining the Streamlit user interface.
                Relative paths are resolved against the current working directory.
            bootstrap_servers: Kafka bootstrap server address.
            asset_router_url: Asset Router URL.
            loglevel: Logging level (e.g., ``INFO``, ``DEBUG``).
            log_http_requests: Enables logging of incoming HTTP requests handled by the embedded Uvicorn server.
            test_mode: Enables test mode (disables live Kafka/ksql interaction).

        Raises:
            FileNotFoundError: If the specified Streamlit script does not exist or is not a regular file.

        Note:
            - The Streamlit script path is resolved to an absolute path before the
              OpenFactory application is initialized.
            - The OpenFactory application must be initialized in the main thread.
            - The embedded HTTP server is not started during initialization.
              It is started by :meth:`run` or :meth:`async_run`.
            - The ``test_mode`` parameter does not disable the embedded Streamlit server.

        See also:
            :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`
            for full initialization details and environment variable handling.
        """

        # Validate script path
        script_path = Path(script_path).resolve()

        if not script_path.is_file():
            raise FileNotFoundError(script_path)

        super().__init__(
            ksqlClient=ksqlClient,
            bootstrap_servers=bootstrap_servers,
            asset_router_url=asset_router_url,
            loglevel=loglevel,
            test_mode=test_mode,
        )

        self.script_path = script_path
        self.log_http_requests = log_http_requests

        # Internal state
        self._uvicorn_server = None
        self._shutdown_requested = False

        # Streamlit application
        self.app = self.create_streamlit_app()

    def create_streamlit_app(self) -> st.App:
        """
        Create and configure the Streamlit application.

        This method is called during initialization to create the
        :class:`streamlit.App` application attached to the OpenFactory runtime.

        Subclasses may override this method to customize the Streamlit application
        before it is served by the embedded Uvicorn server.

        Typical use cases include:
            - Configuring application secrets.
            - Registering additional ASGI routes.
            - Adding ASGI middleware.
            - Configuring application lifespan hooks.
            - Customizing script error handling.
            - Enabling debugging.

        The default implementation creates a Streamlit application using
        the Python script specified by the ``script_path`` constructor parameter.

        Returns:
            streamlit.App: Configured Streamlit application instance.

        .. admonition:: Example

            .. code-block:: python

                class DemoStreamlitApp(OpenFactoryStreamlitApp):

                    def create_streamlit_app(self):
                        return st.App(
                            script_path=self.script_path,
                            debug=True,
                        )

        Note:
            - The returned Streamlit application is automatically exposed through ``self.app``.
            - The resolved Streamlit script path is available via :attr:`script_path`.
            - This method creates the Streamlit ASGI application but does not start its server.
            - The Streamlit runtime is started and stopped through the ASGI lifespan
              mechanism when served by Uvicorn.
        """
        return st.App(self.script_path)

    async def _run_streamlit(self) -> None:
        """
        Run the embedded Streamlit application using Uvicorn.

        This starts an ASGI server that handles HTTP and WebSocket connections
        for the Streamlit application until the server is stopped.

        The server listens on all network interfaces (``0.0.0.0``) and uses the
        port defined by the ``PORT`` environment variable, or ``4000`` if the
        variable is not set.

        The ASGI lifespan protocol is explicitly enabled to ensure proper
        initialization and shutdown of the Streamlit runtime.

        Raises:
            RuntimeError: If the Uvicorn server exits with a ``SystemExit`` exception
                or fails to start without a shutdown having been requested.

        Note:
            - The server log level is aligned with the OpenFactory application logger.
            - Logging of incoming HTTP requests is controlled by the
              ``log_http_requests`` constructor parameter.
            - The Uvicorn server executes asynchronously in the same asyncio event loop
              as the OpenFactory application logic.
            - If application shutdown has already been requested, the server is not started.
            - This method is intended for internal use and is called automatically
              by :meth:`async_run`.
        """

        log_level = logging.getLevelName(self.logger.level).lower()

        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=int(os.getenv("PORT", "4000")),
            log_level=log_level,
            access_log=self.log_http_requests,
            lifespan="on",
        )

        self._uvicorn_server = uvicorn.Server(config)

        if self._shutdown_requested:
            return

        try:
            await self._uvicorn_server.serve()
        except SystemExit as e:
            raise RuntimeError("Streamlit server failed") from e

        if not self._uvicorn_server.started and not self._shutdown_requested:
            raise RuntimeError("Streamlit server failed to start")

    async def _run_openfactory(self) -> None:
        """
        Execute the OpenFactory application logic.

        This method ensures that only asynchronous execution is used.
        If the subclass overrides
        :meth:`OpenFactoryApp.main_loop <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>`,
        a runtime error is raised.

        Behavior:
            - If the subclass overrides :meth:`async_main_loop`, its implementation is awaited.
            - Otherwise, the default :meth:`async_main_loop` keeps the application alive indefinitely.

        Raises:
            RuntimeError: If a synchronous :meth:`main_loop` is defined.

        Note:
            - The OpenFactory application logic executes independently of Streamlit
              script executions and connected browser sessions.
            - This method is intended for internal use and is called automatically
              by :meth:`async_run`.
        """

        if type(self).main_loop is not OpenFactoryApp.main_loop:
            raise RuntimeError(
                "OpenFactoryStreamlitApp does NOT support 'main_loop'. "
                "Use 'async_main_loop' instead."
            )

        await self.async_main_loop()

    def signal_handler(
        self,
        signum: int,
        frame: FrameType | None,
    ) -> None:
        """
        Handle termination signals and request graceful application shutdown.

        Overrides
        :meth:`OpenFactoryApp.signal_handler <openfactory.apps.ofaapp.OpenFactoryApp.signal_handler>`
        to allow the embedded Uvicorn server and the OpenFactory application logic
        to terminate through the asynchronous application lifecycle.

        Rather than immediately deregistering the OpenFactory asset and terminating
        the process, this method marks application shutdown as requested and
        instructs the Uvicorn server to exit, if it has already been initialized.

        Args:
            signum: Signal number received (e.g., ``SIGINT`` or ``SIGTERM``).
            frame: Current execution stack frame when the signal was received, if available.

        Note:
            - This method requests shutdown without directly stopping the asyncio event loop.
            - If the Uvicorn server has not yet started, the shutdown request prevents
              subsequent server execution.
            - Application cleanup and asset deregistration are handled by :meth:`async_run`.
        """

        self._shutdown_requested = True

        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True

    async def async_run(self) -> None:
        """
        Asynchronous entry point of the application.

        Starts both the Streamlit server and the OpenFactory application logic
        concurrently within the same asyncio event loop.

        This method:
            - Displays the welcome banner.
            - Checks whether application shutdown has already been requested.
            - Sets the application availability to ``AVAILABLE`` unless test mode is enabled.
            - Starts the Streamlit server as an asyncio task.
            - Starts the OpenFactory application logic as an asyncio task.
            - Waits until either task terminates.
            - Propagates exceptions raised by completed tasks.
            - Requests graceful shutdown of the Streamlit server.
            - Cancels the OpenFactory background task if it is still running.
            - Waits for both tasks to terminate.
            - Executes the OpenFactory application cleanup.

        Application cleanup is performed through the inherited
        :meth:`OpenFactoryApp.shutdown <openfactory.apps.ofaapp.OpenFactoryApp.shutdown>`
        method, which handles asset deregistration and invokes the
        :meth:`OpenFactoryApp.app_event_loop_stopped <openfactory.apps.ofaapp.OpenFactoryApp.app_event_loop_stopped>`
        cleanup hook.

        Raises:
            RuntimeError: If the embedded Streamlit server fails to start or
                synchronous OpenFactory application logic is defined.
            Exception: Any unhandled exception raised during application execution
                is logged and propagated after application cleanup.
            asyncio.CancelledError: If the asynchronous application execution is cancelled.

        Note:
            - Termination of either task, whether normal or caused by an exception,
              initiates shutdown of the application.
            - Application availability is announced before the embedded HTTP server is started.
            - The termination of an individual Streamlit browser session does not
              terminate the OpenFactory application.
            - The method remains active as long as both the Streamlit server and the
              OpenFactory application logic continue executing.
            - Exceptions raised during the inherited OpenFactory cleanup are logged.
        """

        tasks = set()
        task_streamlit = None
        task_ofa = None

        try:
            self.welcome_banner()

            if self._shutdown_requested:
                return

            if not self._test_mode:
                self.producer.send_asset_attribute(
                    asset_uuid=self.asset_uuid,
                    assetAttribute=AssetAttribute(
                        id="avail",
                        value="AVAILABLE",
                        tag="Availability",
                        type="Events",
                    ),
                )

            self.logger.info("Starting async main loop")

            task_streamlit = asyncio.create_task(
                self._run_streamlit(),
                name="Streamlit",
            )

            task_ofa = asyncio.create_task(
                self._run_openfactory(),
                name="OpenFactory",
            )

            tasks = {task_streamlit, task_ofa}

            done, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                task.result()

        except Exception:
            self.logger.exception("Error in async_run")
            raise

        finally:
            self.logger.info("Shutting down application")

            # Stop Uvicorn gracefully
            if self._uvicorn_server is not None:
                self._uvicorn_server.should_exit = True

            # Cancel OpenFactory background logic
            if task_ofa is not None and not task_ofa.done():
                task_ofa.cancel()

            # Cancel server task if it has not yet initialized
            if task_streamlit is not None and self._uvicorn_server is None:
                task_streamlit.cancel()

            # Wait for both tasks to terminate
            await asyncio.gather(*tasks, return_exceptions=True)

            # OpenFactory cleanup
            try:
                self.shutdown()
            except Exception:
                self.logger.exception("Cleanup failed")

            self.logger.info("Application shutdown complete")

    def run(self) -> None:
        """
        Start the application using a synchronous entry point.

        This method runs :meth:`async_run` inside an asyncio event loop.

        It is the standard entry point for starting an OpenFactory Streamlit
        application from a standalone Python script.

        Note:
            - This method is blocking and returns when the application terminates normally.
            - Applications already running inside an asyncio event loop should
              await :meth:`async_run` instead.
            - The ``streamlit run`` command is not required to start the embedded
              Streamlit server.
        """
        asyncio.run(self.async_run())

    async def async_main_loop(self) -> None:
        """
        Default asynchronous main loop.

        Keeps the application alive when no custom loop is provided.

        Subclasses can override this method to implement background logic,
        such as data acquisition, periodic attribute updates, communication
        with external systems, or data processing.

        The asynchronous main loop executes independently of the Streamlit
        interface and continues running even when no browser session is connected.

        .. admonition:: Example

            .. code-block:: python

                async def async_main_loop(self):
                    while True:
                        await asyncio.sleep(5)
                        self.logger.info("Background task running")

        Note:
            - Background tasks should use asynchronous or non-blocking operations
              to avoid blocking the embedded Uvicorn server.
            - Streamlit rendering functions should be called from Streamlit scripts,
              not from this method.
            - If this method returns or raises an exception, the application
              shutdown sequence is initiated.
            - Subclasses requiring cleanup of background resources should handle
              coroutine cancellation appropriately.
        """
        while True:
            await asyncio.sleep(3600)
