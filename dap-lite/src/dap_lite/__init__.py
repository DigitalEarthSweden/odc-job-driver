from .driver import BNPDriver as DB_BNPDriver
from .mock_driver import BNPDriver as MOCK_BNPDriver

from .driver_protocol import BNPDriverProtocol

from enum import Enum


class DriverType(Enum):
    DB = "db"
    MOCK = "mock"


def get_driver(driver_type: DriverType = DriverType.MOCK) -> BNPDriverProtocol:
    if driver_type == DriverType.DB:
        return DB_BNPDriver()  # Return the actual DB driver
    elif driver_type == DriverType.MOCK:
        return MOCK_BNPDriver()  # Return the mock driver
    else:
        raise ValueError(f"Unsupported driver type: {driver_type}")
