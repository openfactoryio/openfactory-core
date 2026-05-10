try:
    from flask import Flask
except ImportError as e:
    raise ImportError(
        "Flask is required for OpenFactoryFlaskApp. "
        "Install with: pip install openfactory[flask]"
    ) from e

import os
import asyncio
import threading
from werkzeug.serving import make_server
from openfactory.apps import OpenFactoryApp
from openfactory.kafka import KSQLDBClient


class OpenFactoryFlaskApp(OpenFactoryApp):
    """
    OpenFactory application with an embedded Flask web interface.

    Extends :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` by attaching a
    :class:`flask.Flask` application to the OpenFactory runtime.
    This allows exposing HTTP endpoints alongside the standard OpenFactory
    asset, attribute, and method mechanisms.

    The Flask application is available via the :attr:`app` attribute and
    can be used exactly as in a standard Flask project.

    Runtime behavior:
        - Calling :meth:`run` starts both the OpenFactory application and an embedded HTTP server.
        - The Flask server runs in a dedicated background thread using Werkzeug.
        - The OpenFactory logic runs asynchronously inside the asyncio event loop.
        - The server listens on all network interfaces (``0.0.0.0``).
        - The listening port is defined by the ``PORT`` environment variable, or defaults to ``4000`` if unset.
        - Unhandled exceptions in the Flask thread are propagated to the main asyncio runtime.

    Architecture:
        - Flask runs in a dedicated thread
        - OpenFactory logic runs in asyncio
        - User applications implement :meth:`async_main_loop`
        - The synchronous :meth:`OpenFactoryApp.main_loop() <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>`
          is intentionally not supported

    Attributes:
        app (flask.Flask): Flask application instance attached to the OpenFactory app.

    .. admonition:: Usage Example (inline routes)

        .. code-block:: python

            import os
            import asyncio
            from flask import current_app

            from openfactory.apps import OpenFactoryFlaskApp, EventAttribute, ofa_method
            from openfactory.kafka import KSQLDBClient

            class DemoFlaskApp(OpenFactoryFlaskApp):

                status = EventAttribute(value="idle", tag="App.Status")

                def configure_routes(self):

                    @self.app.route("/")
                    def root():
                        ofa_app = current_app.ofa_app
                        return {
                            "status": ofa_app.status.value
                        }

                @ofa_method(description="Move axis")
                def move_axis(self, x: float, y: float):
                    self.logger.info(f"Move to {x},{y}")

                async def async_main_loop(self):
                    while True:
                        await asyncio.sleep(5)
                        self.logger.info("Background task running")

            app = DemoFlaskApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            )

            app.run()

    For larger applications, routes can be split into modules and registered using Blueprints:

    .. admonition:: Using Blueprints (recommended for larger applications)

        .. code-block:: python

            # main.py
            import os
            import asyncio

            from openfactory.apps import OpenFactoryFlaskApp
            from openfactory.kafka import KSQLDBClient

            from routes.root import root_bp

            class DemoFlaskApp(OpenFactoryFlaskApp):

                def configure_routes(self):
                    self.app.register_blueprint(root_bp)

                async def async_main_loop(self):
                    while True:
                        await asyncio.sleep(5)

            app = DemoFlaskApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            )

            app.run()

        .. code-block:: python

            # routes/root.py
            from flask import Blueprint, current_app

            root_bp = Blueprint("root", __name__)

            @root_bp.route("/")
            def root():
                ofa_app = current_app.ofa_app
                return {
                    "availability": ofa_app.avail.value
                }

    Note:
      - The Flask application is accessible via :attr:`app` and behaves like a standard Flask instance.
      - Routes can be registered directly using :meth:`flask.Flask.route` or through Flask Blueprints (:class:`flask.Blueprint`).
      - Only asynchronous execution is supported. Subclasses should implement :meth:`async_main_loop`
        for background tasks.
      - The synchronous :meth:`OpenFactoryApp.main_loop() <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>`
        is not supported in this class.
      - OpenFactory features such as attributes, methods, and asset communication remain unchanged.
      - When deployed on the OpenFactory platform, the ``PORT`` environment variable is set automatically
        by the deployment tool.

    .. seealso::
        - :class:`openfactory.apps.ofaapp.OpenFactoryApp`
        - :class:`flask.Flask`
        - `Flask documentation <https://flask.palletsprojects.com/>`_
    """

    def __init__(
        self,
        ksqlClient: KSQLDBClient,
        bootstrap_servers: str | None = None,
        asset_router_url: str | None = None,
        loglevel: str = "INFO",
        test_mode: bool = False,
    ):
        """
        Initialize the OpenFactory Flask application.

        This constructor forwards all parameters to
        :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`
        and additionally creates a :class:`flask.Flask` instance
        accessible via :attr:`app`.

        The OpenFactory application instance is exposed inside Flask through:

        .. code-block:: python

            current_app.ofa_app

        allowing Flask routes and Blueprints to access the OpenFactory runtime.

        Args:
            ksqlClient: KSQL client instance.
            bootstrap_servers: Kafka bootstrap server address.
            asset_router_url: Asset Router URL.
            loglevel: Logging level (e.g., ``INFO``, ``DEBUG``).
            test_mode: Enables test mode (disables live Kafka/ksql interaction).

        See also:
            :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`
            for full initialization details and environment variable handling.
        """
        super().__init__(
            ksqlClient=ksqlClient,
            bootstrap_servers=bootstrap_servers,
            asset_router_url=asset_router_url,
            loglevel=loglevel,
            test_mode=test_mode
        )

        # Flask application
        self.app = Flask(__name__)

        # expose OpenFactory app inside Flask
        self.app.ofa_app = self

        # internal state
        self._server = None
        self._thread_exception = None

        self.configure_routes()

    def configure_routes(self):
        """
        Configure HTTP routes for the Flask application.

        This method can be overridden by subclasses to define routes directly
        using :attr:`app`.

        .. admonition:: Example

            .. code-block:: python

                def configure_routes(self):

                    @self.app.route("/")
                    def root():
                        return {"status": "ok"}

        Note:
            For larger applications, prefer using Flask Blueprints instead
            of defining all routes inline.
        """
        pass

    def _thread_wrapper(self, target):
        """
        Execute a function inside a managed background thread.

        Any unhandled exception raised by the thread is captured and stored
        so it can be propagated back into the main asyncio runtime.

        Args:
            target: Callable executed inside the thread.
        """
        try:
            target()
        except Exception as e:
            self.logger.exception("Flask thread crashed")
            self._thread_exception = e

    def _run_flask(self):
        """
        Run the embedded Flask application using Werkzeug.

        This starts a blocking WSGI server inside a dedicated background thread.

        The server listens on all network interfaces (``0.0.0.0``) and uses the
        port defined by the ``PORT`` environment variable, or ``4000`` if the
        variable is not set.

        Note:
            This method is intended for internal use and is called automatically
            by :meth:`async_run`.
        """
        port = int(os.getenv("PORT", "4000"))
        self._server = make_server(
            host="0.0.0.0",
            port=port,
            app=self.app
        )

        self.logger.info(f"Starting Flask server on port {port}")
        self._server.serve_forever()

    def _user_defined_async_main(self) -> bool:
        """
        Check whether the subclass defines :meth:`async_main_loop`.

        Returns:
            bool: ``True`` if the subclass overrides :meth:`async_main_loop`,
            otherwise ``False``.
        """
        return type(self).async_main_loop is not OpenFactoryApp.async_main_loop

    def _user_defined_main(self) -> bool:
        """
        Check whether the subclass defines :meth:`main_loop`.

        Returns:
            bool: ``True`` if the subclass overrides :meth:`OpenFactoryApp.main_loop <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>`,
            otherwise ``False``.
        """
        return type(self).main_loop is not OpenFactoryApp.main_loop

    async def _run_openfactory(self):
        """
        Execute the OpenFactory application logic.

        This method ensures that only asynchronous execution is used.
        If :meth:`main_loop` is defined, a runtime error is raised.

        Behavior:
            - If :meth:`async_main_loop` is defined, it is awaited.
            - Otherwise, the coroutine waits indefinitely.

        Raises:
            RuntimeError: If a synchronous :meth:`main_loop` is defined.
        """
        if self._user_defined_main():
            raise RuntimeError(
                "OpenFactoryFlaskApp does NOT support 'main_loop'. "
                "Use 'async_main_loop' instead."
            )

        if self._user_defined_async_main():
            await self.async_main_loop()
        else:
            # keep app alive
            while True:
                await asyncio.sleep(3600)

    async def async_run(self):
        """
        Asynchronous entry point of the application.

        Starts both the Flask server and the OpenFactory logic concurrently.

        This method:
            - Displays the welcome banner
            - Sets the application availability
            - Starts the Flask server thread
            - Runs the OpenFactory async logic
            - Propagates exceptions from the Flask thread
            - Gracefully shuts down the application

        Raises:
            Exception: Any unhandled exception during execution is logged and
                triggers :meth:`OpenFactoryApp.app_event_loop_stopped() <openfactory.apps.ofaapp.OpenFactoryApp.app_event_loop_stopped>`.
        """
        self.welcome_banner()
        self.avail = "AVAILABLE"

        flask_thread = threading.Thread(
            target=lambda: self._thread_wrapper(self._run_flask),
            name="FlaskThread",
            daemon=False
        )

        flask_thread.start()

        task_ofa = asyncio.create_task(
            self._run_openfactory(),
            name="OpenFactoryTask"
        )

        try:
            while True:
                # propagate flask thread exception
                if self._thread_exception:
                    raise self._thread_exception

                # propagate async task exception
                if task_ofa.done():
                    exc = task_ofa.exception()
                    if exc:
                        raise exc
                    break

                await asyncio.sleep(1)

        except asyncio.CancelledError:
            self.logger.info("Application cancelled")
            raise

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received")

        except Exception:
            self.logger.exception("Application crashed")

        finally:
            self.logger.info("Shutting down application")

            # stop flask server
            if self._server:
                self._server.shutdown()

            # cancel async task
            if not task_ofa.done():
                task_ofa.cancel()
                await asyncio.gather(
                    task_ofa,
                    return_exceptions=True
                )

            # cleanup
            try:
                self.app_event_loop_stopped()
            except Exception:
                self.logger.exception("Cleanup failed")

            self.logger.info("Application shutdown complete")

    def run(self):
        """
        Start the application using a synchronous entry point.

        This method runs :meth:`async_run` inside an asyncio event loop.
        """
        asyncio.run(self.async_run())

    async def async_main_loop(self) -> None:
        """
        Default asynchronous main loop.

        Keeps the application alive when no custom loop is provided.

        Subclasses can override this method to implement background logic.

        .. admonition:: Example

            .. code-block:: python

                async def async_main_loop(self):
                    while True:
                        await asyncio.sleep(5)
                        self.logger.info("Background task running")
        """
        while True:
            await asyncio.sleep(3600)
