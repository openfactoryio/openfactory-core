""" Provides a case-insensitive dictionary implementation. """

from collections import UserDict
from typing import Optional, Any


class CaseInsensitiveDict(UserDict):
    """
    Dictionary with case insensitive keys.

    Example:
        >>> d = CaseInsensitiveDict({'Content-Type': 'application/json'})
        >>> d['content-type']
        'application/json'
        >>> d['CONTENT-TYPE']
        'application/json'
    """

    def __getitem__(self, key: str):
        """
        Retrieve the value associated with a key, case-insensitively.

        Args:
            key (str): The key to look up.

        Returns:
            The value corresponding to the key, if found.

        Raises:
            KeyError: If the key is not found in any case.
        """
        key_lower = key.lower()
        for k in self.data.keys():
            if k.lower() == key_lower:
                return self.data[k]
        raise KeyError(key)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve the value associated with a key, case-insensitively, with a default.

        Args:
            key (str): The key to look up.
            default (Any, optional): Value to return if key is not found.

        Returns:
            Any: The value for the key, or default if not found.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __contains__(self, key: str) -> bool:
        """
        Check if a key exists in the dictionary, case-insensitively.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if key exists, False otherwise.
        """
        key_lower = key.lower()
        return any(k.lower() == key_lower for k in self.data.keys())

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set the value for a key, case-insensitively.

        If the key exists in any casing, it will overwrite the existing value.
        Otherwise, a new key is added preserving the original casing.

        Args:
            key (str): The key to set.
            value (Any): The value to assign.
        """
        key_lower = key.lower()
        for k in self.data.keys():
            if k.lower() == key_lower:
                self.data[k] = value
                return
        self.data[key] = value

    def __delitem__(self, key: str) -> None:
        """
        Delete a key, case-insensitively.

        Args:
            key (str): The key to delete.

        Raises:
            KeyError: If the key is not found in any case.
        """
        key_lower = key.lower()
        for k in list(self.data.keys()):
            if k.lower() == key_lower:
                del self.data[k]
                return
        raise KeyError(key)
