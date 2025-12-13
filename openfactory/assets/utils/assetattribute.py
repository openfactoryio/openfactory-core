""" AssetAttribute """

from dataclasses import dataclass, field
from typing import Literal
from .time_methods import current_timestamp


@dataclass
class AssetAttribute:
    """
    Represents a single attribute of an asset, including its value, type, tag, and timestamp.

    Attributes:
        id (str): the ID of the AssetAttribute
        value (str | float): The actual value of the attribute. Can be a string or float.
        type (typing.Literal['Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE']):
            The category/type of the attribute, must be one of the allowed literal strings.
        tag (str): The tag or identifier associated with this attribute.
        timestamp (str): Timestamp when the attribute was recorded, in OpenFactory format.
                         Defaults to the current timestamp if not provided.
    """

    id: str
    value: str | float
    type: Literal['Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE']
    tag: str
    timestamp: str = field(default_factory=current_timestamp)

    def __post_init__(self) -> None:
        """
        Validates the type of the attribute after initialization.

        Raises:
            ValueError: If the type is not one of the allowed literal strings.
        """
        ALLOWED_TYPES = {'Samples', 'Condition', 'Events', 'Method', 'OpenFactory', 'UNAVAILABLE'}
        if self.type not in ALLOWED_TYPES:
            raise ValueError(f"Invalid type '{self.type}'. Allowed values are: {', '.join(ALLOWED_TYPES)}")
