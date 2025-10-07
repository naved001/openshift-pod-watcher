import re
from datetime import datetime, timezone


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_openshift_resource(resource_value):
    """
    Converts the resource value and returns it without any unit.
    For example, ram is returned in bytes, CPU is returned without mili.
    """
    PATTERN = r"([0-9]+)(m|Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E)?"

    suffix = {
        "Ki": 2**10,
        "Mi": 2**20,
        "Gi": 2**30,
        "Ti": 2**40,
        "Pi": 2**50,
        "Ei": 2**60,
        "m": 10**-3,
        "K": 10**3,
        "M": 10**6,
        "G": 10**9,
        "T": 10**12,
        "P": 10**15,
        "E": 10**18,
    }

    if resource_value and resource_value != "0":
        result = re.search(PATTERN, resource_value)

        if result is None:
            raise ValueError(f"Unable to parse resource_value = '{resource_value}'")

        value = int(result.groups()[0])
        unit = result.groups()[1]
        # Convert to number i.e. without any unit suffix
        if unit is not None:
            return value * suffix[unit]
        return value
    return 0
