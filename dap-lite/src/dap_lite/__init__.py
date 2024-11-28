from .driver import BNPDriver as DB_BNPDriver
from .mock_driver import BNPDriver as MOCK_BNPDriver

from .driver_protocol import BNPDriverProtocol

from enum import Enum


class DriverType(Enum):
    DB = "db"
    MOCK = "mock"


def get_driver(
    driver_type: DriverType = DriverType.MOCK,  processor_id:int = 1, **kwargs 
) -> BNPDriverProtocol:
    if driver_type == DriverType.DB:
        return DB_BNPDriver(processor_id=processor_id)  # Return the actual DB driver
    elif driver_type == DriverType.MOCK:
        return MOCK_BNPDriver(processor_id=processor_id)  # Return the mock driver
    else:
        raise ValueError(f"Unsupported driver type: {driver_type}")


__all__ = [
    "BNPDriverProtocol",  # Protocol for type hinting
    "DriverType",  # Enum for driver types
    "get_driver",  # Factory function for getting drivers
]
