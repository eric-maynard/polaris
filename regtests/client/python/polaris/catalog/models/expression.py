#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# coding: utf-8

"""
    Apache Iceberg REST Catalog API

    Defines the specification for the first version of the REST Catalog API. Implementations should ideally support both Iceberg table specs v1 and v2, with priority given to v2.

    The version of the OpenAPI document: 0.0.1
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations
import json
import pprint
from pydantic import BaseModel, ConfigDict, Field, StrictStr, ValidationError, field_validator
from typing import Any, List, Optional
from polaris.catalog.models.literal_expression import LiteralExpression
from polaris.catalog.models.set_expression import SetExpression
from polaris.catalog.models.unary_expression import UnaryExpression
from pydantic import StrictStr, Field
from typing import Union, List, Set, Optional, Dict
from typing_extensions import Literal, Self

EXPRESSION_ONE_OF_SCHEMAS = ["AndOrExpression", "LiteralExpression", "NotExpression", "SetExpression", "UnaryExpression"]

class Expression(BaseModel):
    """
    Expression
    """
    # data type: AndOrExpression
    oneof_schema_1_validator: Optional[AndOrExpression] = None
    # data type: NotExpression
    oneof_schema_2_validator: Optional[NotExpression] = None
    # data type: SetExpression
    oneof_schema_3_validator: Optional[SetExpression] = None
    # data type: LiteralExpression
    oneof_schema_4_validator: Optional[LiteralExpression] = None
    # data type: UnaryExpression
    oneof_schema_5_validator: Optional[UnaryExpression] = None
    actual_instance: Optional[Union[AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression]] = None
    one_of_schemas: Set[str] = { "AndOrExpression", "LiteralExpression", "NotExpression", "SetExpression", "UnaryExpression" }

    model_config = ConfigDict(
        validate_assignment=True,
        protected_namespaces=(),
    )


    def __init__(self, *args, **kwargs) -> None:
        if args:
            if len(args) > 1:
                raise ValueError("If a position argument is used, only 1 is allowed to set `actual_instance`")
            if kwargs:
                raise ValueError("If a position argument is used, keyword arguments cannot be used.")
            super().__init__(actual_instance=args[0])
        else:
            super().__init__(**kwargs)

    @field_validator('actual_instance')
    def actual_instance_must_validate_oneof(cls, v):
        instance = Expression.model_construct()
        error_messages = []
        match = 0
        # validate data type: AndOrExpression
        if not isinstance(v, AndOrExpression):
            error_messages.append(f"Error! Input type `{type(v)}` is not `AndOrExpression`")
        else:
            match += 1
        # validate data type: NotExpression
        if not isinstance(v, NotExpression):
            error_messages.append(f"Error! Input type `{type(v)}` is not `NotExpression`")
        else:
            match += 1
        # validate data type: SetExpression
        if not isinstance(v, SetExpression):
            error_messages.append(f"Error! Input type `{type(v)}` is not `SetExpression`")
        else:
            match += 1
        # validate data type: LiteralExpression
        if not isinstance(v, LiteralExpression):
            error_messages.append(f"Error! Input type `{type(v)}` is not `LiteralExpression`")
        else:
            match += 1
        # validate data type: UnaryExpression
        if not isinstance(v, UnaryExpression):
            error_messages.append(f"Error! Input type `{type(v)}` is not `UnaryExpression`")
        else:
            match += 1
        if match > 1:
            # more than 1 match
            raise ValueError("Multiple matches found when setting `actual_instance` in Expression with oneOf schemas: AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression. Details: " + ", ".join(error_messages))
        elif match == 0:
            # no match
            raise ValueError("No match found when setting `actual_instance` in Expression with oneOf schemas: AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression. Details: " + ", ".join(error_messages))
        else:
            return v

    @classmethod
    def from_dict(cls, obj: Union[str, Dict[str, Any]]) -> Self:
        return cls.from_json(json.dumps(obj))

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Returns the object represented by the json string"""
        instance = cls.model_construct()
        error_messages = []
        match = 0

        # deserialize data into AndOrExpression
        try:
            instance.actual_instance = AndOrExpression.from_json(json_str)
            match += 1
        except (ValidationError, ValueError) as e:
            error_messages.append(str(e))
        # deserialize data into NotExpression
        try:
            instance.actual_instance = NotExpression.from_json(json_str)
            match += 1
        except (ValidationError, ValueError) as e:
            error_messages.append(str(e))
        # deserialize data into SetExpression
        try:
            instance.actual_instance = SetExpression.from_json(json_str)
            match += 1
        except (ValidationError, ValueError) as e:
            error_messages.append(str(e))
        # deserialize data into LiteralExpression
        try:
            instance.actual_instance = LiteralExpression.from_json(json_str)
            match += 1
        except (ValidationError, ValueError) as e:
            error_messages.append(str(e))
        # deserialize data into UnaryExpression
        try:
            instance.actual_instance = UnaryExpression.from_json(json_str)
            match += 1
        except (ValidationError, ValueError) as e:
            error_messages.append(str(e))

        if match > 1:
            # more than 1 match
            raise ValueError("Multiple matches found when deserializing the JSON string into Expression with oneOf schemas: AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression. Details: " + ", ".join(error_messages))
        elif match == 0:
            # no match
            raise ValueError("No match found when deserializing the JSON string into Expression with oneOf schemas: AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression. Details: " + ", ".join(error_messages))
        else:
            return instance

    def to_json(self) -> str:
        """Returns the JSON representation of the actual instance"""
        if self.actual_instance is None:
            return "null"

        if hasattr(self.actual_instance, "to_json") and callable(self.actual_instance.to_json):
            return self.actual_instance.to_json()
        else:
            return json.dumps(self.actual_instance)

    def to_dict(self) -> Optional[Union[Dict[str, Any], AndOrExpression, LiteralExpression, NotExpression, SetExpression, UnaryExpression]]:
        """Returns the dict representation of the actual instance"""
        if self.actual_instance is None:
            return None

        if hasattr(self.actual_instance, "to_dict") and callable(self.actual_instance.to_dict):
            return self.actual_instance.to_dict()
        else:
            # primitive type
            return self.actual_instance

    def to_str(self) -> str:
        """Returns the string representation of the actual instance"""
        return pprint.pformat(self.model_dump())

from polaris.catalog.models.and_or_expression import AndOrExpression
from polaris.catalog.models.not_expression import NotExpression
# TODO: Rewrite to not use raise_errors
Expression.model_rebuild(raise_errors=False)

