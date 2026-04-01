from typing import Any


class AttributeField:
    """
    Base class for declarative OpenFactory attributes.

    Instances of this class (or its subclasses) are intended to be declared
    as class-level attributes inside an :class:`OpenFactoryApp <openfactory.apps.ofaapp.OpenFactoryApp>` class.
    They are automatically collected by :class:`OpenFactoryAppMeta` into the
    ``_declared_attributes`` dictionary.

    Each attribute represents a typed data field (e.g., ``Events`` or ``Samples``)
    with an associated tag used by OpenFactory.

    Args:
        value (Any, Optional): Default value of the attribute.
            Defaults to ``"UNAVAILABLE"``.
        type (str, Optional): Attribute type (e.g., ``Events``, ``Samples``).
            Defaults to ``"Events"``.
        tag (str): Tag used by OpenFactory.

    Notes:
        - ``name`` is assigned automatically when the class is created.
        - This class is typically not used directly; prefer subclasses
          like :class:`EventAttribute` or :class:`SampleAttribute`.
    """
    def __init__(self, *, value: Any = "UNAVAILABLE", type: str = "Events", tag: str):
        self.value = value
        self.type = type
        self.tag = tag
        self.name = None  # will be set by metaclass


class EventAttribute(AttributeField):
    """
    Represents an ``Events``-type OpenFactory attribute.

    This is a convenience subclass of :class:`AttributeField` that
    automatically sets ``type="Events"``.

    Args:
        value (Any, Optional): Default value of the attribute.
            Defaults to ``"UNAVAILABLE"``.
        tag (str): Tag used by OpenFactory.

    .. admonition:: Example

       .. code-block:: python

            class MyApp(OpenFactoryApp):
                status = EventAttribute(value="idle", tag="App.Status")
    """
    def __init__(self, *, value: Any = "UNAVAILABLE", tag: str):
        super().__init__(value=value, type="Events", tag=tag)


class SampleAttribute(AttributeField):
    """
    Represents a ``Samples``-type OpenFactory attribute.

    This is a convenience subclass of :class:`AttributeField` that
    automatically sets ``type="Samples"``.

    Args:
        value (Any, Optional): Default value of the attribute.
            Defaults to ``"UNAVAILABLE"``.
        tag (str): External identifier used by OpenFactory.

    .. admonition:: Example

       .. code-block:: python

            class MyApp(OpenFactoryApp):
                temperature = SampleAttribute(tag="TEMP_SENSOR")
    """
    def __init__(self, *, value: Any = "UNAVAILABLE", tag: str):
        super().__init__(value=value, type="Samples", tag=tag)


class OpenFactoryAppMeta(type):
    """
    Metaclass that collects declarative :class:`AttributeField` instances.

    When a class is defined with this metaclass, all class-level attributes
    that are instances of :class:`AttributeField` are automatically collected
    into a ``_declared_attributes`` dictionary.

    This enables a declarative style for defining OpenFactory attributes.

    Behavior:
        - Attributes from base classes are inherited.
        - Attributes in subclasses override those from base classes.
        - Each attribute's ``name`` is automatically set to the variable name
          used in the class definition.

    Attributes:
        _declared_attributes (dict[str, AttributeField]):
            Mapping of attribute names to their corresponding instances.

    .. admonition:: Example

       .. code-block:: python

            class BaseApp(metaclass=OpenFactoryAppMeta):
                status = EventAttribute(tag="STATUS")

            class MyApp(BaseApp):
                temperature = SampleAttribute(tag="TEMP")
                status = EventAttribute(tag="OVERRIDE_STATUS")  # overrides base

            MyApp._declared_attributes
            # {
            #     "status": <EventAttribute tag="OVERRIDE_STATUS">,
            #     "temperature": <SampleAttribute tag="TEMP">
            # }

            MyApp._declared_attributes["status"].name
            # "status"

    .. admonition:: Advanced note

       The metaclass walks the MRO (via base classes) and merges attribute
       definitions before processing the current class body. This ensures
       predictable inheritance and override behavior.
    """

    _declared_attributes: dict[str, "AttributeField"]

    def __new__(cls, name: str, bases: tuple[type, ...], attrs: dict[str, object]) -> "OpenFactoryAppMeta":
        """
        Create a new class, collecting any AttributeField instances declared
        on the class or its base classes into a ``_declared_attributes`` dict.

        Args:
            cls (type): The metaclass itself.
            name (str): Name of the class being created.
            bases (tuple[type, ...]): Base classes of the class being created.
            attrs (dict[str, object]): Attributes defined in the class body.

        Returns:
            OpenFactoryAppMeta: A new class object with ``_declared_attributes`` set.

        Notes:
            - Base classes are scanned first, so their declarative attributes
              are inherited by subclasses.
            - Attributes in the current class override any with the same name
              from base classes.
            - ``value.name = key`` ensures the AttributeField knows the name
              it was assigned to, which can be used later for identification
              or serialization.
        """
        declared_attributes: dict[str, "AttributeField"] = {}

        # Collect from base classes first
        for base in bases:
            if hasattr(base, "_declared_attributes"):
                declared_attributes.update(base._declared_attributes)

        # Collect from current class
        for key, value in attrs.items():
            if isinstance(value, AttributeField):
                value.name = key
                declared_attributes[key] = value

        attrs["_declared_attributes"] = declared_attributes
        return super().__new__(cls, name, bases, attrs)
