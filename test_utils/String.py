from uuid import uuid4


def unique_str() -> str:
    return str(uuid4())
