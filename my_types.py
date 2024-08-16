from enum import Enum


class SchemaEnum(Enum):
    ADDRESS = 1
    CITIZENSHIP = 2


class CitizenshipTypes(Enum):
    BIRTH = 1
    DESCENT = 2
    CONFERRAL = 3
    ADOPTION = 4
