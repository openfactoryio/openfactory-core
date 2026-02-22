"""
Shared Deployment Configuration Schemas for OpenFactory

This module defines reusable Pydantic models that represent container
deployment settings and resource constraints. These models are intended
to be shared across device and connector configuration schemas.

Components:
-----------
- `ResourcesDefinition`: Represents CPU and memory values for a container.
- `Resources`: Groups `reservations` and `limits` for container resources.
- `Placement`: Defines constraints for container placement on specific nodes.
- `Deploy`: Complete deployment configuration including replicas, resource configs, and placement rules.

Key Features:
-------------
- Express CPU and memory constraints using common formats (e.g., 0.5 CPUs, "1Gi" memory)
- Define both resource requests and limits for containers
- Support placement constraints for scheduling on labeled nodes
- Modular, reusable across multiple OpenFactory schema modules

Example Usage:
--------------
Define a deployment configuration:

    >>> Deploy(
    ...     replicas=2,
    ...     resources=Resources(
    ...         reservations=ResourcesDefinition(cpus=0.5, memory="512Mi"),
    ...         limits=ResourcesDefinition(cpus=1.0, memory="1Gi")
    ...     ),
    ...     placement=Placement(
    ...         constraints=["node.labels.zone == eu-west"]
    ...     )
    ... )

This module is typically used as part of device, connector, or application
schemas that involve resource scheduling or orchestration.
"""

import re
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any


class ResourcesDefinition(BaseModel):
    """ Defines resource limits or reservations such as CPU and memory. """
    cpus: Optional[float] = Field(
        default=None,
        description="Amount of CPU to allocate, expressed as a fractional number (e.g., 0.25 = one quarter of a CPU core)."
    )
    memory: Optional[str] = Field(
        default=None,
        description="Amount of memory to allocate (e.g., '512Mi', '1Gi')."
    )


class Resources(BaseModel):
    """ Specifies resource requests and limits for a container deployment. """
    reservations: Optional[ResourcesDefinition] = Field(
        default=None,
        description="Minimum required resources guaranteed for the container."
    )
    limits: Optional[ResourcesDefinition] = Field(
        default=None,
        description="Maximum resources the container is allowed to consume."
    )


class Placement(BaseModel):
    """ Defines placement constraints for scheduling containers. """
    constraints: Optional[List[str]] = Field(
        default=None,
        description="List of placement constraint expressions (e.g., ['node.labels.region == can-west'])."
    )


class Deploy(BaseModel):
    """ Defines deployment configuration such as replicas, resources, and placement. """
    replicas: Optional[int] = Field(
        default=1,
        description="Number of container instances (replicas) to run."
    )
    resources: Optional[Resources] = Field(
        default=None,
        description="Resource requests and limits for the container."
    )
    placement: Optional[Placement] = Field(
        default=None,
        description="Constraints for container placement on specific nodes."
    )


def cpus_reservation(deploy: Optional[Deploy], default: float = 0.5) -> float:
    """
    Retrieve the CPU reservation value from a Deploy object, returning a default if unavailable.

    Args:
        deploy (Optional[Deploy]): The deployment configuration object.
        default (float): The default CPU reservation value to return if not set. Defaults to 0.5.

    Returns:
        float: The CPU reservation value from the deployment or the default if not specified.
    """
    if deploy is None:
        return default
    resources = getattr(deploy, 'resources', None)
    reservations = resources.reservations if resources else None
    return reservations.cpus if reservations and reservations.cpus is not None else default


def cpus_limit(deploy: Optional[Deploy], default: float = 1.0) -> float:
    """
    Retrieve the CPU limit value from a Deploy object, returning a default if unavailable.

    Args:
        deploy (Optional[Deploy]): The deployment configuration object.
        default (float): The default CPU limit value to return if not set. Defaults to 1.0.

    Returns:
        float: The CPU limit value from the deployment or the default if not specified.
    """
    if deploy is None:
        return default
    resources = getattr(deploy, 'resources', None)
    limits = resources.limits if resources else None
    return limits.cpus if limits and limits.cpus is not None else default


def parse_memory_to_bytes(mem_str: str) -> int:
    """
    Convert a memory size string into an integer number of bytes.

    This function accepts memory strings using common units like bytes (``B``), kilobytes (``K``, ``KB``),
    megabytes (``M``, ``MB``, ``Mi``), and gigabytes (``G``, ``GB``, ``Gi``), case-insensitive.
    It also handles fractional values (e.g., ``0.5Gi``).

    If no unit is provided, the string is interpreted as bytes.

    .. admonition:: Usage example

      .. code-block:: python

        >>> parse_memory_to_bytes("512Mi")
        536870912
        >>> parse_memory_to_bytes("1Gi")
        1073741824
        >>> parse_memory_to_bytes("0.5Gi")
        536870912
        >>> parse_memory_to_bytes("1024")
        1024

    Args:
        mem_str (str): Memory size string, e.g., ``512Mi``, ``1Gi``, ``1024``.

    Returns:
        int: Memory size in bytes.

    Raises:
        ValueError: If the string cannot be parsed into a valid number.
    """
    mem_str = mem_str.strip().lower()  # normalize
    units = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024 ** 2,
        "mb": 1024 ** 2,
        "mi": 1024 ** 2,
        "g": 1024 ** 3,
        "gb": 1024 ** 3,
        "gi": 1024 ** 3,
    }

    # check longest units first
    for unit in sorted(units.keys(), key=len, reverse=True):
        if mem_str.endswith(unit):
            factor = units[unit]
            number_part = mem_str[:-len(unit)].strip()
            return int(float(number_part) * factor)

    # fallback: assume raw bytes
    return int(mem_str)


def resources(deploy: Optional[Deploy]) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Retrieve and normalize resources from a Deploy object for Docker deployments.

    Extracts CPU and memory settings from ``deploy.resources`` and converts them
    into the dictionary format expected by Docker.

    Args:
        deploy (Optional[Deploy]): Deployment configuration for an application.

    Returns:
        Optional[Dict[str, Dict[str, Any]]]: Dictionary with ``Limits`` and ``Reservations`` keys,
        containing CPU (NanoCPUs) and Memory (MemoryBytes) in Docker format.
        Returns None if no resource info is provided.
    """
    if deploy is None or deploy.resources is None:
        return None

    res = deploy.resources
    limits: Dict[str, Any] = {}
    reservations: Dict[str, Any] = {}

    # Limits
    if res.limits:
        if res.limits.cpus is not None:
            limits["NanoCPUs"] = int(1e9 * res.limits.cpus)
        if res.limits.memory:
            limits["MemoryBytes"] = parse_memory_to_bytes(res.limits.memory)

    # Reservations
    if res.reservations:
        if res.reservations.cpus is not None:
            reservations["NanoCPUs"] = int(1e9 * res.reservations.cpus)
        if res.reservations.memory:
            reservations["MemoryBytes"] = parse_memory_to_bytes(res.reservations.memory)

    if not limits and not reservations:
        return None

    return {
        "Limits": limits,
        "Reservations": reservations
    }


def constraints(deploy: Optional[Deploy]) -> Optional[List[str]]:
    """
    Retrieve and normalize placement constraints from a Deploy object for Docker deployments.

    This function:
     - Extracts the ``constraints`` list from ``deploy.placement`` if available.
     - Converts single ``'='`` into ``' == '`` for Docker/Docker Swarm API compatibility.
     - Normalizes spacing around ``'=='``.
     - Returns None if no constraints are defined.

    Args:
        deploy (Optional[Deploy]): Deployment configuration containing optional placement info.

    Returns:
        Optional[List[str]]: List of normalized constraint strings, or None if empty or undefined.
    """
    if deploy is None:
        return None

    placement = deploy.placement
    if placement is None or placement.constraints is None:
        return None

    placement = deploy.placement
    if placement is None:
        return None

    raw_constraints = placement.constraints
    if not raw_constraints:  # catches None and []
        return None

    normalized = []

    for c in raw_constraints:
        # Replace single '=' not part of '=='
        c = re.sub(r'(?<![=])=(?!=)', ' == ', c)

        # Normalize spacing around ==
        c = re.sub(r'\s*==\s*', ' == ', c)

        normalized.append(c.strip())

    return normalized
