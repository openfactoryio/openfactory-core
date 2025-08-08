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

from pydantic import BaseModel, Field
from typing import List, Optional


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
