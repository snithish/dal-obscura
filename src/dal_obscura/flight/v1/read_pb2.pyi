from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FieldPathSegment(_message.Message):
    __slots__ = ("kind", "name", "field_id")
    class Kind(metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        KIND_UNSPECIFIED: _ClassVar[FieldPathSegment.Kind]
        FIELD: _ClassVar[FieldPathSegment.Kind]
        LIST_ELEMENT: _ClassVar[FieldPathSegment.Kind]
        MAP_KEY: _ClassVar[FieldPathSegment.Kind]
        MAP_VALUE: _ClassVar[FieldPathSegment.Kind]
    KIND_UNSPECIFIED: FieldPathSegment.Kind
    FIELD: FieldPathSegment.Kind
    LIST_ELEMENT: FieldPathSegment.Kind
    MAP_KEY: FieldPathSegment.Kind
    MAP_VALUE: FieldPathSegment.Kind
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    FIELD_ID_FIELD_NUMBER: _ClassVar[int]
    kind: FieldPathSegment.Kind
    name: str
    field_id: int
    def __init__(self, kind: _Optional[_Union[FieldPathSegment.Kind, str]] = ..., name: _Optional[str] = ..., field_id: _Optional[int] = ...) -> None: ...

class FieldPath(_message.Message):
    __slots__ = ("version", "segments")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    version: int
    segments: _containers.RepeatedCompositeFieldContainer[FieldPathSegment]
    def __init__(self, version: _Optional[int] = ..., segments: _Optional[_Iterable[_Union[FieldPathSegment, _Mapping]]] = ...) -> None: ...

class PlanRequest(_message.Message):
    __slots__ = ("protocol_version", "catalog", "target", "columns", "row_filter", "column_paths")
    PROTOCOL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CATALOG_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ROW_FILTER_FIELD_NUMBER: _ClassVar[int]
    COLUMN_PATHS_FIELD_NUMBER: _ClassVar[int]
    protocol_version: int
    catalog: str
    target: str
    columns: _containers.RepeatedScalarFieldContainer[str]
    row_filter: str
    column_paths: _containers.RepeatedCompositeFieldContainer[FieldPath]
    def __init__(self, protocol_version: _Optional[int] = ..., catalog: _Optional[str] = ..., target: _Optional[str] = ..., columns: _Optional[_Iterable[str]] = ..., row_filter: _Optional[str] = ..., column_paths: _Optional[_Iterable[_Union[FieldPath, _Mapping]]] = ...) -> None: ...
