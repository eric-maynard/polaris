# coding: utf-8

"""
    Apache Iceberg REST Catalog API

    Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import pprint
import re  # noqa: F401
import json

from pydantic import ConfigDict, Field, StrictStr, field_validator
from typing import Any, ClassVar, Dict, List, Optional
from polaris.catalog.models.content_file import ContentFile
from polaris.catalog.models.count_map import CountMap
from polaris.catalog.models.file_format import FileFormat
from polaris.catalog.models.primitive_type_value import PrimitiveTypeValue
from polaris.catalog.models.value_map import ValueMap
from typing import Optional, Set
from typing_extensions import Self

class DataFile(ContentFile):
    """
    DataFile
    """ # noqa: E501
    content: StrictStr
    column_sizes: Optional[CountMap] = Field(default=None, description="Map of column id to total count, including null and NaN", alias="column-sizes")
    value_counts: Optional[CountMap] = Field(default=None, description="Map of column id to null value count", alias="value-counts")
    null_value_counts: Optional[CountMap] = Field(default=None, description="Map of column id to null value count", alias="null-value-counts")
    nan_value_counts: Optional[CountMap] = Field(default=None, description="Map of column id to number of NaN values in the column", alias="nan-value-counts")
    lower_bounds: Optional[ValueMap] = Field(default=None, description="Map of column id to lower bound primitive type values", alias="lower-bounds")
    upper_bounds: Optional[ValueMap] = Field(default=None, description="Map of column id to upper bound primitive type values", alias="upper-bounds")
    __properties: ClassVar[List[str]] = ["content", "file-path", "file-format", "spec-id", "partition", "file-size-in-bytes", "record-count", "key-metadata", "split-offsets", "sort-order-id"]

    @field_validator('content')
    def content_validate_enum(cls, value):
        """Validates the enum"""
        if value not in set(['data']):
            raise ValueError("must be one of enum values ('data')")
        return value

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )


    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of DataFile from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([
        ])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of each item in partition (list)
        _items = []
        if self.partition:
            for _item in self.partition:
                if _item:
                    _items.append(_item.to_dict())
            _dict['partition'] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of DataFile from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate({
            "content": obj.get("content"),
            "file-path": obj.get("file-path"),
            "file-format": obj.get("file-format"),
            "spec-id": obj.get("spec-id"),
            "partition": [PrimitiveTypeValue.from_dict(_item) for _item in obj["partition"]] if obj.get("partition") is not None else None,
            "file-size-in-bytes": obj.get("file-size-in-bytes"),
            "record-count": obj.get("record-count"),
            "key-metadata": obj.get("key-metadata"),
            "split-offsets": obj.get("split-offsets"),
            "sort-order-id": obj.get("sort-order-id")
        })
        return _obj


