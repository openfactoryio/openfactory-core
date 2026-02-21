"""
Command envelope schema for OpenFactory command execution.

This module defines the :class:`.CommandEnvelope` and :class:`.CommandHeader`
models used to standardize command messages exchanged between OpenFactory
components (e.g., Core, Gateways, Applications).

The CommandEnvelope represents a structured command request including
metadata (header) and named string-based arguments.

Design Principles:
    - Arguments are string-based.
    - Argument ordering, if needed, is handled by the receiving component (e.g., OPC UA gateway).
    - Identity and correlation metadata are carried in the header.
    - Strict schema validation is enforced (extra fields are forbidden).

Note:
    - This schema defines the protocol structure only.
    - It does not implement authentication or signature validation.
    - Signature validation (if used) must be implemented at runtime level.

Warning:
    Signature validation is not yet implemented in OpenFactory and will be added in future.
    The presence of the ``signature`` field does not imply authentication
    or integrity verification at this stage.

.. admonition:: Usage Example

  Example JSON message:

  .. code-block:: json

     {
       "header": {
         "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
         "sender_uuid": "HMI-CNC-9207",
         "timestamp": "2026-02-21T12:00:00Z",
         "signature": null
       },
       "arguments": {
         "x": "10",
         "y": "20",
         "z": "30"
       }
     }
"""

from datetime import datetime, timezone
from typing import Dict, Optional
from uuid import UUID
from pydantic import BaseModel, Field


class CommandHeader(BaseModel):
    """
    Metadata header for an OpenFactory command message.

    Arguments:
        correlation_id (:class:`~uuid.UUID`):
            Unique identifier used to correlate request and response.
        sender_uuid (:class:`~uuid.UUID`):
            UUID of the requesting asset.
        timestamp (:class:`~datetime.datetime`):
            Time at which the command was issued (UTC).
        signature (Optional[str]):
            Optional cryptographic signature for authentication purposes.
            Validation is performed at runtime, not at schema level.
    """

    correlation_id: UUID = Field(
        description="Unique identifier to correlate request and response"
    )

    sender_uuid: str = Field(
        description="Asset UUID of the sending asset"
    )

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="UTC timestamp when the command was created"
    )

    signature: Optional[str] = Field(
        default=None,
        description="Optional cryptographic signature"
    )

    model_config = {
        "extra": "forbid",
        "str_strip_whitespace": True
    }


class CommandEnvelope(BaseModel):
    """
    OpenFactory command request envelope.

    The envelope contains a metadata header and a dictionary
    of named string arguments.

    Arguments:
        header (CommandHeader):
            Metadata associated with the command.
        arguments (Dict[str, str]):
            Named string arguments passed to the receiving asset.

    Note:
        - All argument values are strings.
        - Argument ordering is resolved by the receiving gateway if required.
        - Unknown fields are rejected.
    """

    header: CommandHeader = Field(
        description="Command metadata header"
    )

    arguments: Dict[str, str] = Field(
        default_factory=dict,
        description="Named string arguments for the command"
    )

    model_config = {
        "extra": "forbid",
        "str_strip_whitespace": True
    }
