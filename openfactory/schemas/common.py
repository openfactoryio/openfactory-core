"""
Shared Pydantic schemas used across device and connector configurations.

This module includes reusable components like resource definitions,
deployment configuration, and container placement constraints.
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
