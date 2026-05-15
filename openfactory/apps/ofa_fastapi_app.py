import asyncio
import os
import uvicorn
import logging
from openfactory.kafka import KSQLDBClient
from openfactory.apps import OpenFactoryApp

try:
    from fastapi import FastAPI
except ImportError as e:
    raise ImportError(
        "FastAPI is required for OpenFactoryFastAPIApp. "
        "Install with: pip install openfactory[fastapi]"
    ) from e


class OpenFactoryFastAPIApp(OpenFactoryApp):
    """
    OpenFactory application with an embedded FastAPI web interface.

    Extends :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` by attaching a
    :class:`fastapi.FastAPI` application to the OpenFactory runtime.
    This allows exposing HTTP endpoints alongside the standard OpenFactory
    asset, attribute, and method mechanisms.

    The FastAPI application is available via the :attr:`api` attribute and
    can be used exactly as in a standard FastAPI project.

    Runtime behavior:
        - Calling :meth:`run` starts both the OpenFactory application and an embedded HTTP server.
        - The HTTP server is powered by Uvicorn and serves the attached :class:`fastapi.FastAPI` application.
        - The server listens on all network interfaces (``0.0.0.0``).
        - The listening port is defined by the ``PORT`` environment variable, or defaults to ``4000`` if unset.
        - The server log level is aligned with the OpenFactory application logger.

    Attributes:
        api (fastapi.FastAPI): FastAPI application instance attached to the OpenFactory app.

    .. admonition:: Usage Example (inline routes)

        .. code-block:: python

            import os
            from openfactory.apps import OpenFactoryFastAPIApp, EventAttribute, ofa_method
            from openfactory.kafka import KSQLDBClient

            class DemoFastAPIApp(OpenFactoryFastAPIApp):

                status = EventAttribute(value="idle", tag="App.Status")

                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)

                    @self.api.get("/")
                    async def root():
                        return {"status": self.status.value}

                @ofa_method(description="Move axis")
                def move_axis(self, x: float, y: float):
                    self.logger.info(f"Move to {x},{y}")

            app = DemoFastAPIApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            )
            app.run()

    For larger applications, routes can be split into modules and included using routers:

    .. admonition:: Using routers (recommended for larger applications)

        .. code-block:: python

            # main.py
            import os
            from openfactory.apps import OpenFactoryFastAPIApp, EventAttribute, ofa_method
            from openfactory.kafka import KSQLDBClient
            from routes import root, move

            class DemoFastAPIApp(OpenFactoryFastAPIApp):

                status = EventAttribute(value="idle", tag="App.Status")

                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)

                    # expose OpenFactory App inside FastAPI
                    self.api.state.ofa_app = self

                    # include routers
                    self.api.include_router(root.router)
                    self.api.include_router(move.router)

                @ofa_method(description="Move axis")
                def move_axis(self, x: float, y: float):
                    self.logger.info(f"Move to {x},{y}")

            app = DemoFastAPIApp(
                ksqlClient=KSQLDBClient(os.getenv("KSQLDB_URL", "http://localhost:8088")),
                bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
            )
            app.run()

        .. code-block:: python

            # routes/root.py
            from fastapi import APIRouter, Request

            router = APIRouter()

            @router.get("/")
            async def root(request: Request):
                ofa_app = request.app.state.ofa_app
                return {"status": ofa_app.status.value}

        .. code-block:: python

            # routes/move.py
            from fastapi import APIRouter, Request

            router = APIRouter()

            @router.post("/move")
            async def move(x: float, y: float, request: Request):
                ofa_app = request.app.state.ofa_app
                ofa_app.move_axis(x, y)
                return {"message": "moving"}

    Note:
      - The FastAPI application is accessible via :attr:`api` and behaves like a standard FastAPI instance.
      - Route definitions can be added either directly using :attr:`api` or via :class:`api.include_router() <fastapi.FastAPI>`.
      - For small applications, routes can be defined inline; for larger applications, using routers is recommended.
      - Only asynchronous execution is supported. Subclasses may optionally implement :meth:`async_main_loop` for background tasks.
      - The synchronous :meth:`OpenFactoryApp.main_loop() <openfactory.apps.ofaapp.OpenFactoryApp.main_loop>` is not supported in this class.
      - OpenFactory features such as attributes, methods, and asset communication remain unchanged.
      - When deployed on the OpenFactory platform, the ``PORT`` environment variable is set automatically by the deployment tool.

    .. seealso::
        - :class:`openfactory.apps.ofaapp.OpenFactoryApp`
        - :class:`fastapi.FastAPI`
        - `FastAPI documentation <https://fastapi.tiangolo.com/>`_
    """

    def __init__(
        self,
        ksqlClient: KSQLDBClient,
        bootstrap_servers: str | None = None,
        asset_router_url: str | None = None,
        loglevel: str = "INFO",
        test_mode: bool = False,
    ) -> None:
        """
        Initialize the OpenFactory FastAPI application.

        This constructor forwards all parameters to
        :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>`
        and additionally creates a :class:`fastapi.FastAPI` instance
        accessible via :attr:`api`.

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
        super().__init__(ksqlClient=ksqlClient, bootstrap_servers=bootstrap_servers,
                         asset_router_url=asset_router_url,
                         loglevel=loglevel,
                         test_mode=test_mode)

        self._shutdown_event = asyncio.Event()

        # OPENFACTORY_ROOT_PATH is set by the OpenFactory deployment tool when the app is
        # exposed behind a path prefix (e.g. via Traefik PathPrefix routing such as
        # http://localhost/<app-name> in devcontainers).
        #
        # In that case, FastAPI must be aware of this prefix to correctly generate
        # URLs for OpenAPI/Swagger and any relative paths. This is done via `root_path`.
        #
        # Example:
        #   External URL: http://localhost/demo-fastapi-app/docs
        #   → OPENFACTORY_ROOT_PATH=/demo-fastapi-app
        #
        # When the app is accessed via host-based routing (e.g.
        # http://myapp.openfactory.local), no prefix is used and this value is empty.
        #
        # `root_path_in_servers=True` ensures that the OpenAPI schema (used by Swagger UI)
        # includes the correct base path automatically.
        #
        # Important:
        # - This must match the Traefik PathPrefix configuration.
        # - It should NOT be set for pure host-based routing.
        #
        root_path = os.getenv("OPENFACTORY_ROOT_PATH", "")
        self.api = FastAPI(
            root_path=root_path,
            root_path_in_servers=True,
            version=self.application_version.value,
            title=self.asset_uuid,
            license_info={"name": self.application_license.value},
        )
        self.configure_routes()

    def configure_routes(self) -> None:
        """
        Configure HTTP routes for the FastAPI application.

        This method can be overridden by subclasses to define routes directly
        using :attr:`api`.

        .. admonition:: Example

            .. code-block:: python

                def configure_routes(self):

                    @self.api.get("/")
                    async def root():
                        return {"status": "ok"}

        Note:
            For larger applications, prefer using :class:`api.include_router() <fastapi.FastAPI>` instead
            of overriding this method.
        """
        pass

    async def _run_fastapi(self) -> None:
        """
        Run the FastAPI application using Uvicorn.

        This starts an ASGI server and serves HTTP requests until the application
        is stopped.

        The server listens on all network interfaces (``0.0.0.0``) and uses the
        port defined by the ``PORT`` environment variable, or ``4000`` if the
        variable is not set.

        Note:
            - The log level of the server is aligned with the OpenFactory application logger.
            - This method is intended for internal use and is called by :meth:`async_run`.
        """
        log_level = logging.getLevelName(self.logger.level).lower()

        config = uvicorn.Config(
            self.api,
            host="0.0.0.0",
            port=int(os.getenv("PORT", "4000")),
            log_level=log_level
        )
        self._uvicorn_server = uvicorn.Server(config)
        await self._uvicorn_server.serve()

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

    async def _run_openfactory(self) -> None:
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
                "OpenFactoryFastAPIApp does NOT support 'main_loop'. "
                "Use 'async_main_loop' instead."
            )

        if self._user_defined_async_main():
            await self.async_main_loop()
        else:
            # do nothing
            await self._shutdown_event.wait()

    async def async_run(self) -> None:
        """
        Asynchronous entry point of the application.

        Starts both the FastAPI server and the OpenFactory logic concurrently.

        This method:
            - Displays the welcome banner
            - Sets the application availability
            - Runs FastAPI and OpenFactory tasks concurrently

        Raises:
            Exception: Any unhandled exception during execution is logged and
               triggers :meth:`OpenFactoryApp.app_event_loop_stopped() <openfactory.apps.ofaapp.OpenFactoryApp.app_event_loop_stopped>`.
        """

        self.welcome_banner()
        self.avail = 'AVAILABLE'
        self.logger.info("Starting async main loop")

        task_fastapi = asyncio.create_task(self._run_fastapi(), name="FastAPI")
        task_ofa = asyncio.create_task(self._run_openfactory(), name="OpenFactory")

        tasks = {task_fastapi, task_ofa}

        try:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION
            )

            # If any task raised → re-raise it
            for task in done:
                if exc := task.exception():
                    self.logger.error(f"Task {task.get_name()} failed")
                    raise exc

        except Exception:
            self.logger.exception("Error in async_run")

        finally:
            self.logger.info("Shutting down application")

            # Stop uvicorn cleanly
            server = getattr(self, "_uvicorn_server", None)
            if server:
                server.should_exit = True

            # Cancel remaining tasks
            for task in tasks:
                if not task.done():
                    task.cancel()

            # Wait for cancellation to complete
            await asyncio.gather(*tasks, return_exceptions=True)

            # OpenFactory cleanup
            try:
                self.app_event_loop_stopped()
            except Exception:
                pass

            self.logger.info("Application shutdown complete")

    def run(self) -> None:
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
