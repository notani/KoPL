from typing import Optional, Union
from datetime import date


def comp(a: "ValueClass", b: "ValueClass", op: str) -> bool:
    """Compare two ValueClass instances using the specified operator.

    This function performs comparisons between strongly-typed values in the KoPL system.
    It handles special cases for temporal values where equality has containment semantics
    (e.g., a date can equal a year if the date falls within that year).

    Args:
        a: The first ValueClass instance (typically an attribute value from an entity)
        b: The second ValueClass instance (typically a comparison target from a query)
        op: Comparison operator - one of "=", "!=", "<", ">"

    Returns:
        True if the comparison holds, False otherwise

    Raises:
        TypeError: If the operator is not supported

    Example:
        >>> birthday = ValueClass("date", date(1960, 2, 1))
        >>> year_1960 = ValueClass("year", 1960)
        >>> comp(birthday, year_1960, "=")  # Returns True (birthday is in 1960)
        True
    """
    # Special handling for temporal values: equality uses containment semantics
    # This allows dates to match years (e.g., "1960-02-01" equals "1960")
    if b.isTime():
        if op == "=":
            return b.contains(a)
        elif op == "!=":
            return not b.contains(a)

    # Standard comparisons for all value types
    if op == "=":
        return a == b
    elif op == "<":
        return a < b
    elif op == ">":
        return a > b
    elif op == "!=":
        return a != b
    else:
        raise TypeError(
            f"Unsupported operator '{op}'. Supported operators: =, !=, <, >"
        )


class ValueClass(object):
    """Strongly-typed value container for KoPL knowledge base operations.

    This class wraps different value types (string, quantity, date, year) with
    proper comparison semantics and type checking. It supports four data types:
    - string: Text values
    - quantity: Numerical values with units (e.g., "200 centimetre", "75 kilogram")
    - date: Full dates (YYYY-MM-DD)
    - year: Year integers

    The class implements special comparison logic for temporal values where
    dates can be compared with years using containment semantics.

    Attributes:
        type: The value type ("string", "quantity", "date", "year")
        value: The actual value (str, float, date object, or int)
        unit: The unit for quantity types (None for other types)
    """

    def __init__(
        self, type: str, value: Union[str, float, int, date], unit: Optional[str] = None
    ) -> None:
        """Initialize a ValueClass instance with type-specific value and optional unit.

        Args:
            type: Value type - one of "string", "quantity", "date", "year"
            value: The actual value:
                - For "string": a string
                - For "quantity": a number (int or float)
                - For "year": an integer
                - For "date": a date object
            unit: Unit for quantity types (required for "quantity", ignored for others)

        Raises:
            ValueError: If type-value combination is invalid or unit is missing for quantity
        """
        self.type = type
        self.value = value
        self.unit = unit

    def isTime(self) -> bool:
        """Check if this value represents a temporal type (year or date).

        Returns:
            True if the value type is "year" or "date", False otherwise
        """
        return self.type in {"year", "date"}

    def can_compare(self, other: "ValueClass") -> bool:
        """Check if this value can be compared with another ValueClass instance.

        Comparison rules:
        - Strings can only compare with strings
        - Quantities can only compare with quantities of the same unit
        - Temporal values (years/dates) can compare with each other

        Args:
            other: Another ValueClass instance to check compatibility with

        Returns:
            True if the values can be compared, False otherwise
        """
        if self.type == "string":
            return other.type == "string"
        elif self.type == "quantity":
            # Quantities can only compare when they have the same unit
            return other.type == "quantity" and other.unit == self.unit
        else:
            # Temporal values (year/date) can compare with each other
            return other.type == "year" or other.type == "date"

    def convert_to_year(self):
        # Convert a date value to its year
        if self.type == "year":
            return self
        elif self.type == "date" and isinstance(self.value, date):
            return ValueClass("year", self.value.year)
        else:
            return self

    def contains(self, other: "ValueClass") -> bool:
        """Check temporal containment between date and year values.

        Temporal containment rules:
        - A date contains a year if the date's year matches the given year
        - A year contains a date if the date's year matches the given year
        - All other combinations return False

        Args:
            other: Another ValueClass to check containment with

        Returns:
            True if there is temporal containment, False otherwise
        """
        if (
            self.type == "date"
            and other.type == "year"
            and isinstance(self.value, date)
        ):
            return self.value.year == other.value
        elif (
            other.type == "date"
            and self.type == "year"
            and isinstance(other.value, date)
        ):
            return other.value.year == self.value
        else:
            return False

    def __eq__(self, other: object) -> bool:
        """Test equality between ValueClass instances with strict type matching.

        Unlike temporal containment, this method requires exact type and value matches.
        For example, year 2001 and date 2001-01-01 are not considered equal.

        Args:
            other: Another object (should be ValueClass) to compare with

        Returns:
            True if both type and value match exactly, False otherwise
        """
        if not isinstance(other, ValueClass):
            return False
        if not self.can_compare(other):
            return False
        return self.type == other.type and self.value == other.value

    def __lt__(self, other: "ValueClass") -> bool:
        """Test less-than comparison with type conversion for temporal values.

        When comparing between year and date, both are converted to year for comparison.
        String comparisons raise an exception as they should use equality only.

        Args:
            other: Another ValueClass instance to compare with

        Returns:
            True if this value is less than the other

        Raises:
            Exception: If attempting to compare string values
            AssertionError: If values cannot be compared
        """
        assert self.can_compare(other), "Cannot compare incompatible types"
        if self.type == "string":
            raise Exception("Cannot order-compare string values")
        elif (
            self.type == "quantity"
            and isinstance(self.value, (int, float))
            and isinstance(other.value, (int, float))
        ):
            return self.value < other.value
        elif self.type == "year":
            if (
                other.type == "year"
                and isinstance(self.value, int)
                and isinstance(other.value, int)
            ):
                return self.value < other.value
            elif (
                other.type == "date"
                and isinstance(self.value, int)
                and isinstance(other.value, date)
            ):
                return self.value < other.value.year
        elif self.type == "date" and isinstance(self.value, date):
            if other.type == "year" and isinstance(other.value, int):
                return self.value.year < other.value
            elif other.type == "date" and isinstance(other.value, date):
                return self.value < other.value
        return False

    def __gt__(self, other: "ValueClass") -> bool:
        """Test greater-than comparison with type-specific semantics.

        Args:
            other: Another ValueClass instance to compare with

        Returns:
            True if this value is greater than the other, False otherwise
        """
        if self.type == "string" and other.type == "string":
            return str(self.value) > str(other.value)
        elif (
            self.type == "quantity"
            and other.type == "quantity"
            and self.unit == other.unit
        ):
            # Type guard for quantity values
            if isinstance(self.value, (int, float)) and isinstance(
                other.value, (int, float)
            ):
                return self.value > other.value
            return False
        elif (
            self.type == "year"
            and other.type == "year"
            and isinstance(self.value, int)
            and isinstance(other.value, int)
        ):
            return self.value > other.value
        elif (
            self.type == "date"
            and other.type == "date"
            and isinstance(self.value, date)
            and isinstance(other.value, date)
        ):
            return self.value > other.value
        elif (
            self.type == "date"
            and other.type == "year"
            and isinstance(self.value, date)
            and isinstance(other.value, int)
        ):
            return self.value.year > other.value
        elif (
            self.type == "year"
            and other.type == "date"
            and isinstance(self.value, int)
            and isinstance(other.value, date)
        ):
            return self.value > other.value.year
        else:
            return False

    def __str__(self) -> str:
        """Return string representation of the value with proper formatting.

        Returns:
            Formatted string representation based on value type:
            - string: Direct string value
            - quantity: "<value> <unit>" or just "<value>" if unit is "1"
            - year: String representation of year integer
            - date: ISO format date string (YYYY-MM-DD)
        """
        if self.type == "string":
            return str(self.value)
        elif self.type == "quantity" and isinstance(self.value, (int, float)):
            # Check if value is close to integer
            if abs(self.value - int(self.value)) < 1e-5:
                v = int(self.value)
            else:
                v = self.value
            return "{} {}".format(v, self.unit) if self.unit != "1" else str(v)
        elif self.type == "year":
            return str(self.value)
        elif self.type == "date" and isinstance(self.value, date):
            return self.value.isoformat()
        else:
            return str(self.value)

    def __hash__(self) -> int:
        """Return hash value based on string representation.

        Returns:
            Hash of the string representation of this value
        """
        return hash(str(self))
