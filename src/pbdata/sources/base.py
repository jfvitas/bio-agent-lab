"""Abstract base class for all pbdata source adapters."""

from abc import ABC, abstractmethod
from typing import Any

from pbdata.schemas.canonical_sample import CanonicalBindingSample


class BaseAdapter(ABC):
    """Contract that every source adapter must satisfy.

    Each adapter is responsible for:
    - fetching raw records from its source
    - normalizing them toward the canonical schema fields
    - preserving provenance on every returned record
    """

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Canonical name of the data source (e.g. 'RCSB', 'BindingDB')."""
        ...

    @abstractmethod
    def fetch_metadata(self, record_id: str) -> dict[str, Any]:
        """Fetch raw metadata for a given record identifier.

        Args:
            record_id: Source-native identifier for the record.

        Returns:
            Raw metadata dict as returned by the source.
        """
        ...

    @abstractmethod
    def normalize_record(self, raw: dict[str, Any]) -> CanonicalBindingSample:
        """Map a raw source record to canonical internal fields.

        Args:
            raw: Raw record dict from fetch_metadata.

        Returns:
            Validated canonical record. Implementations must preserve
            source provenance and instantiate CanonicalBindingSample.
        """
        ...
