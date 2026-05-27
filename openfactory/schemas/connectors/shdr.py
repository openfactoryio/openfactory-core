"""
SHDR Connector Schemas

This module provides Pydantic models to define and validate configuration
schemas for SHDR devices within OpenFactory.

Key Models:
-----------
- :class:`SHDRDataPointSchema`:

  Configuration for a single SHDR data point. Contains a ``tag`` used by
  OpenFactory to label incoming SHDR data and a ``type`` defining the type
  of the data point (``Samples`` or ``Events``).

- :class:`SHDRConnectorSchema`:

  Wrapper schema that encapsulates the SHDR server configuration along
  with all configured SHDR data points.

Validation Features:
--------------------
- Validates ``host`` as a valid IPv4 or IPv6 address.
- Validates ``port`` range (1-65535).
- Restricts SHDR data point ``type`` to ``Samples`` or ``Events``.
- Forbids unknown fields to ensure strict schema conformance.
- Supports immutable validated configuration models.

YAML Example:
-------------
.. code-block:: yaml

    type: shdr

    host: 192.168.1.10
    port: 7878

    data:
        temp:
            tag: Temperature
            type: Samples

        humi:
            tag: Humidity
            type: Events

.. seealso::

   The runtime class of the SHDRConnectorSchema schema is
   :class:`openfactory.connectors.shdr.shdr_connector.SHDRConnector`.
"""

from typing import Literal, Dict
from pydantic import BaseModel, ConfigDict, Field, IPvAnyAddress


class SHDRDataPointSchema(BaseModel):
    """ Configuration for a single SHDR data point. """

    tag: str = Field(
        ...,
        description="SHDR tag name."
    )

    type: Literal["Samples", "Events"] = Field(
        ...,
        description="SHDR stream type."
    )

    model_config = ConfigDict(extra="forbid", frozen=True)


class SHDRConnectorSchema(BaseModel):
    """
    SHDR Connector schema wrapping the server configuration.

    The ``type`` field is a discriminator for Pydantic to select this schema.

    .. seealso::

       The runtime class of the :class:`SHDRConnectorSchema` schema is :class:`openfactory.connectors.shdr.shdr_connector.SHDRConnector`.
    """
    type: Literal['shdr'] = Field(
        ...,  # no default, means required
        description="Discriminator field to identify SHDR connector type."
    )

    host: IPvAnyAddress = Field(
        ...,
        description="SHDR server IP address."
    )

    port: int = Field(
        ...,
        ge=1,
        le=65535,
        description="SHDR server port."
    )

    data: Dict[str, SHDRDataPointSchema] = Field(
        default_factory=dict,
        description="Dictionary of SHDR data point definitions."
    )

    model_config = ConfigDict(extra="forbid", frozen=True)
