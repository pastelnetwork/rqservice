# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: service.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rservice.proto\x12\x07raptorq\"b\n\x15\x45ncodeMetaDataRequest\x12\x0c\n\x04path\x18\x01 \x01(\t\x12\x14\n\x0c\x66iles_number\x18\x02 \x01(\r\x12\x12\n\nblock_hash\x18\x03 \x01(\t\x12\x11\n\tpastel_id\x18\x04 \x01(\t\"V\n\x13\x45ncodeMetaDataReply\x12\x1a\n\x12\x65ncoder_parameters\x18\x01 \x01(\x0c\x12\x15\n\rsymbols_count\x18\x02 \x01(\r\x12\x0c\n\x04path\x18\x03 \x01(\t\"\x1d\n\rEncodeRequest\x12\x0c\n\x04path\x18\x01 \x01(\t\"N\n\x0b\x45ncodeReply\x12\x1a\n\x12\x65ncoder_parameters\x18\x01 \x01(\x0c\x12\x15\n\rsymbols_count\x18\x02 \x01(\r\x12\x0c\n\x04path\x18\x03 \x01(\t\"9\n\rDecodeRequest\x12\x1a\n\x12\x65ncoder_parameters\x18\x01 \x01(\x0c\x12\x0c\n\x04path\x18\x02 \x01(\t\"\x1b\n\x0b\x44\x65\x63odeReply\x12\x0c\n\x04path\x18\x01 \x01(\t2\xc9\x01\n\x07RaptorQ\x12N\n\x0e\x45ncodeMetaData\x12\x1e.raptorq.EncodeMetaDataRequest\x1a\x1c.raptorq.EncodeMetaDataReply\x12\x36\n\x06\x45ncode\x12\x16.raptorq.EncodeRequest\x1a\x14.raptorq.EncodeReply\x12\x36\n\x06\x44\x65\x63ode\x12\x16.raptorq.DecodeRequest\x1a\x14.raptorq.DecodeReplyb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'service_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _ENCODEMETADATAREQUEST._serialized_start=26
  _ENCODEMETADATAREQUEST._serialized_end=124
  _ENCODEMETADATAREPLY._serialized_start=126
  _ENCODEMETADATAREPLY._serialized_end=212
  _ENCODEREQUEST._serialized_start=214
  _ENCODEREQUEST._serialized_end=243
  _ENCODEREPLY._serialized_start=245
  _ENCODEREPLY._serialized_end=323
  _DECODEREQUEST._serialized_start=325
  _DECODEREQUEST._serialized_end=382
  _DECODEREPLY._serialized_start=384
  _DECODEREPLY._serialized_end=411
  _RAPTORQ._serialized_start=414
  _RAPTORQ._serialized_end=615
# @@protoc_insertion_point(module_scope)
