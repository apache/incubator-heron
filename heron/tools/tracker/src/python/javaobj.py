#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""Provides functions for reading and writing (writing is WIP currently) Java objects
serialized or will be deserialized by ObjectOutputStream. This form of object
representation is a standard data interchange format in Java world.

javaobj module exposes an API familiar to users of the standard
library marshal, pickle and json modules.

See: http://download.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
"""

import struct
import six

from heron.common.src.python.utils.log import Log

def log_debug(message, ident=0):
  """log debugging info"""
  Log.debug(" " * (ident * 2) + str(message))

def log_error(message, ident=0):
  """log error info"""
  Log.error(" " * (ident * 2) + str(message))

__version__ = "$Revision: 20 $"

def load(file_object):
  """
  Deserializes Java primitive data and objects serialized by ObjectOutputStream
  from a file-like object.
  """
  marshaller = JavaObjectUnmarshaller(file_object)
  marshaller.add_transformer(DefaultObjectTransformer())
  return marshaller.readObject()


# pylint: disable=undefined-variable
def loads(value):
  """
  Deserializes Java objects and primitive data serialized by ObjectOutputStream
  from a string.
  """
  f = six.StringIO(value)
  marshaller = JavaObjectUnmarshaller(f)
  marshaller.add_transformer(DefaultObjectTransformer())
  return marshaller.readObject()


def dumps(obj):
  """
  Serializes Java primitive data and objects unmarshaled by load(s) before into string.
  """
  marshaller = JavaObjectMarshaller()
  return marshaller.dump(obj)

_java_primitives = set([
    "java.lang.Double",
    "java.lang.Float",
    "java.lang.Integer",
    "java.lang.Long"])

class JavaClass(object):
  """Java class representation"""
  def __init__(self):
    self.name = None
    self.serialVersionUID = None
    self.flags = None
    self.fields_names = []
    self.fields_types = []
    self.superclass = None

  def __str__(self):
    return self.__repr__()

  def __repr__(self):
    return "[%s:0x%X]" % (self.name, self.serialVersionUID)


class JavaObject(object):
  """Java object representation"""
  def __init__(self):
    self.classdesc = None
    self.annotations = []

  def get_class(self):
    """get class"""
    return self.classdesc

  def __str__(self):
    """get reprensentation in string"""
    return self.__repr__()

  def __repr__(self):
    """get reprensentation"""
    name = "UNKNOWN"
    if self.classdesc:
      name = self.classdesc.name
    return "<javaobj:%s>" % name

  def classname(self):
    name = "UNKNOWN"
    if self.classdesc:
      name = self.classdesc.name
    return name

  def is_primitive(self):
    return self.classname() in _java_primitives

  def copy(self, new_object):
    """copy an object"""
    new_object.classdesc = self.classdesc

    for name in self.classdesc.fields_names:
      new_object.__setattr__(name, getattr(self, name))

class JavaObjectConstants(object):
  """class about Java object constants"""

  STREAM_MAGIC = 0xaced
  STREAM_VERSION = 0x05

  TC_NULL = 0x70
  TC_REFERENCE = 0x71
  TC_CLASSDESC = 0x72
  TC_OBJECT = 0x73
  TC_STRING = 0x74
  TC_ARRAY = 0x75
  TC_CLASS = 0x76
  TC_BLOCKDATA = 0x77
  TC_ENDBLOCKDATA = 0x78
  TC_RESET = 0x79
  TC_BLOCKDATALONG = 0x7A
  TC_EXCEPTION = 0x7B
  TC_LONGSTRING = 0x7C
  TC_PROXYCLASSDESC = 0x7D
  TC_ENUM = 0x7E
  TC_MAX = 0x7E

  # classDescFlags
  SC_WRITE_METHOD = 0x01 # if SC_SERIALIZABLE
  SC_BLOCK_DATA = 0x08   # if SC_EXTERNALIZABLE
  SC_SERIALIZABLE = 0x02
  SC_EXTERNALIZABLE = 0x04
  SC_ENUM = 0x10

  # type definition chars (typecode)
  TYPE_BYTE = 'B'     # 0x42
  TYPE_CHAR = 'C'
  TYPE_DOUBLE = 'D'   # 0x44
  TYPE_FLOAT = 'F'    # 0x46
  TYPE_INTEGER = 'I'  # 0x49
  TYPE_LONG = 'J'     # 0x4A
  TYPE_SHORT = 'S'    # 0x53
  TYPE_BOOLEAN = 'Z'  # 0x5A
  TYPE_OBJECT = 'L'   # 0x4C
  TYPE_ARRAY = '['    # 0x5B

  # list of supported typecodes listed above
  TYPECODES_LIST = [
      # primitive types
      TYPE_BYTE,
      TYPE_CHAR,
      TYPE_DOUBLE,
      TYPE_FLOAT,
      TYPE_INTEGER,
      TYPE_LONG,
      TYPE_SHORT,
      TYPE_BOOLEAN,
      # object types
      TYPE_OBJECT,
      TYPE_ARRAY]

  BASE_REFERENCE_IDX = 0x7E0000

# pylint: disable=missing-docstring
class JavaObjectUnmarshaller(JavaObjectConstants):
  """Java object unmarshaller"""

  def __init__(self, stream=None):
    self.opmap = {
        self.TC_NULL: self.do_null,
        self.TC_CLASSDESC: self.do_classdesc,
        self.TC_OBJECT: self.do_object,
        self.TC_STRING: self.do_string,
        self.TC_ARRAY: self.do_array,
        self.TC_CLASS: self.do_class,
        self.TC_BLOCKDATA: self.do_blockdata,
        self.TC_REFERENCE: self.do_reference,
        self.TC_ENUM: self.do_enum,
        self.TC_ENDBLOCKDATA: self.do_null, # note that we are reusing of do_null
    }
    self.current_object = None
    self.reference_counter = 0
    self.references = []
    self.object_stream = stream
    self._readStreamHeader()
    self.object_transformers = []

  def readObject(self):
    """read object"""
    try:
      _, res = self._read_and_exec_opcode(ident=0)

      position_bak = self.object_stream.tell()
      the_rest = self.object_stream.read()
      if len(the_rest):
        log_error("Warning!!!!: Stream still has %s bytes left.\
Enable debug mode of logging to see the hexdump." % len(the_rest))
        log_debug(self._create_hexdump(the_rest))
      else:
        log_debug("Java Object unmarshalled succesfully!")
      self.object_stream.seek(position_bak)

      return res
    except Exception:
      self._oops_dump_state()
      raise

  def add_transformer(self, transformer):
    """add to object transformer"""
    self.object_transformers.append(transformer)

  def _readStreamHeader(self):
    (magic, version) = self._readStruct(">HH")
    if magic != self.STREAM_MAGIC or version != self.STREAM_VERSION:
      raise IOError("The stream is not java serialized object.\
Invalid stream header: %04X%04X" % (magic, version))

  def _read_and_exec_opcode(self, ident=0, expect=None):
    (opid, ) = self._readStruct(">B")
    log_debug("OpCode: 0x%X" % opid, ident)
    if expect and opid not in expect:
      raise IOError("Unexpected opcode 0x%X" % opid)
    handler = self.opmap.get(opid)
    if not handler:
      raise RuntimeError("Unknown OpCode in the stream: 0x%x" % opid)
    return (opid, handler(ident=ident))

  def _readStruct(self, unpack):
    length = struct.calcsize(unpack)
    ba = self.object_stream.read(length)
    if len(ba) != length:
      raise RuntimeError("Stream has been ended unexpectedly while unmarshaling.")
    return struct.unpack(unpack, ba)

  def _readString(self):
    (length, ) = self._readStruct(">H")
    ba = self.object_stream.read(length)
    return ba

  def do_classdesc(self, parent=None, ident=0):
    """do_classdesc"""
    # TC_CLASSDESC className serialVersionUID newHandle classDescInfo
    # classDescInfo:
    #   classDescFlags fields classAnnotation superClassDesc
    # classDescFlags:
    #   (byte)                  // Defined in Terminal Symbols and Constants
    # fields:
    #   (short)<count>  fieldDesc[count]

    # fieldDesc:
    #   primitiveDesc
    #   objectDesc
    # primitiveDesc:
    #   prim_typecode fieldName
    # objectDesc:
    #   obj_typecode fieldName className1
    clazz = JavaClass()
    log_debug("[classdesc]", ident)
    ba = self._readString()
    clazz.name = ba
    log_debug("Class name: %s" % ba, ident)
    (serialVersionUID, newHandle, classDescFlags) = self._readStruct(">LLB")
    clazz.serialVersionUID = serialVersionUID
    clazz.flags = classDescFlags

    self._add_reference(clazz)

    log_debug("Serial: 0x%X newHandle: 0x%X.\
classDescFlags: 0x%X" % (serialVersionUID, newHandle, classDescFlags), ident)
    (length, ) = self._readStruct(">H")
    log_debug("Fields num: 0x%X" % length, ident)

    clazz.fields_names = []
    clazz.fields_types = []
    for _ in range(length):
      (typecode, ) = self._readStruct(">B")
      field_name = self._readString()
      field_type = None
      field_type = self._convert_char_to_type(typecode)

      if field_type == self.TYPE_ARRAY:
        _, field_type = self._read_and_exec_opcode(
            ident=ident+1, expect=[self.TC_STRING, self.TC_REFERENCE])
        assert isinstance(field_type, str)
#              if field_type is not None:
#                  field_type = "array of " + field_type
#              else:
#                  field_type = "array of None"
      elif field_type == self.TYPE_OBJECT:
        _, field_type = self._read_and_exec_opcode(
            ident=ident+1, expect=[self.TC_STRING, self.TC_REFERENCE])
        assert isinstance(field_type, str)

      log_debug("FieldName: 0x%X" % typecode + " " + str(field_name) + " " + str(field_type), ident)
      assert field_name is not None
      assert field_type is not None

      clazz.fields_names.append(field_name)
      clazz.fields_types.append(field_type)
    # pylint: disable=protected-access
    if parent:
      parent.__fields = clazz.fields_names
      parent.__types = clazz.fields_types
    # classAnnotation
    (opid, ) = self._readStruct(">B")
    log_debug("OpCode: 0x%X" % opid, ident)
    if opid != self.TC_ENDBLOCKDATA:
      raise NotImplementedError("classAnnotation isn't implemented yet")
    # superClassDesc
    _, superclassdesc = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_CLASSDESC, self.TC_NULL, self.TC_REFERENCE])
    log_debug(str(superclassdesc), ident)
    clazz.superclass = superclassdesc

    return clazz

  # pylint: disable=unused-argument
  def do_blockdata(self, parent=None, ident=0):
    # TC_BLOCKDATA (unsigned byte)<size> (byte)[size]
    log_debug("[blockdata]", ident)
    (length, ) = self._readStruct(">B")
    ba = self.object_stream.read(length)
    return ba

  def do_class(self, parent=None, ident=0):
    # TC_CLASS classDesc newHandle
    log_debug("[class]", ident)

    _, classdesc = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_CLASSDESC, self.TC_PROXYCLASSDESC,
                               self.TC_NULL, self.TC_REFERENCE])
    log_debug("Classdesc: %s" % classdesc, ident)
    self._add_reference(classdesc)
    return classdesc

  def do_object(self, parent=None, ident=0):
    # TC_OBJECT classDesc newHandle classdata[]  // data for each class
    java_object = JavaObject()
    log_debug("[object]", ident)
    log_debug("java_object.annotations just after instantination: " +
              str(java_object.annotations), ident)

    opcode, classdesc = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_CLASSDESC, self.TC_PROXYCLASSDESC,
                               self.TC_NULL, self.TC_REFERENCE])
    # self.TC_REFERENCE hasn't shown in spec, but actually is here

    self._add_reference(java_object)

    # classdata[]

    # Store classdesc of this object
    java_object.classdesc = classdesc

    if classdesc.flags & self.SC_EXTERNALIZABLE and not classdesc.flags & self.SC_BLOCK_DATA:
      raise NotImplementedError("externalContents isn't implemented yet")

    if classdesc.flags & self.SC_SERIALIZABLE:
      # create megalist
      tempclass = classdesc
      megalist = []
      megatypes = []
      while tempclass:
        log_debug(">>> " + str(tempclass.fields_names) + " " + str(tempclass), ident)
        log_debug(">>> " + str(tempclass.fields_types), ident)
        fieldscopy = tempclass.fields_names[:]
        fieldscopy.extend(megalist)
        megalist = fieldscopy

        fieldscopy = tempclass.fields_types[:]
        fieldscopy.extend(megatypes)
        megatypes = fieldscopy

        tempclass = tempclass.superclass

      log_debug("Values count: %s" % str(len(megalist)), ident)
      log_debug("Prepared list of values: %s" % str(megalist), ident)
      log_debug("Prepared list of types: %s" % str(megatypes), ident)

      for field_name, field_type in zip(megalist, megatypes):
        res = self._read_value(field_type, ident, name=field_name)
        java_object.__setattr__(field_name, res)

    if classdesc.flags & self.SC_SERIALIZABLE and classdesc.flags & \
       self.SC_WRITE_METHOD or classdesc.flags & self.SC_EXTERNALIZABLE \
       and classdesc.flags & self.SC_BLOCK_DATA:
        # objectAnnotation
      log_debug("java_object.annotations before: " + str(java_object.annotations), ident)
      while opcode != self.TC_ENDBLOCKDATA:
        opcode, obj = self._read_and_exec_opcode(ident=ident+1)
        if opcode != self.TC_ENDBLOCKDATA:
          java_object.annotations.append(obj)
        log_debug("objectAnnotation value: " + str(obj), ident)
      log_debug("java_object.annotations after: " + str(java_object.annotations), ident)

    # Transform object
    for transformer in self.object_transformers:
      tmp_object = transformer.transform(java_object)
      if tmp_object != java_object:
        java_object = tmp_object
        break

    log_debug(">>> java_object: " + str(java_object), ident)
    return java_object

  def do_string(self, parent=None, ident=0):
    log_debug("[string]", ident)
    ba = self._readString()
    self._add_reference(str(ba))
    return str(ba)

  def do_array(self, parent=None, ident=0):
    # TC_ARRAY classDesc newHandle (int)<size> values[size]
    log_debug("[array]", ident)
    _, classdesc = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_CLASSDESC,
                               self.TC_PROXYCLASSDESC, self.TC_NULL, self.TC_REFERENCE])

    array = []

    self._add_reference(array)

    (size, ) = self._readStruct(">i")
    log_debug("size: " + str(size), ident)

    type_char = classdesc.name[0]
    assert type_char == self.TYPE_ARRAY
    type_char = classdesc.name[1]

    if type_char == self.TYPE_OBJECT or type_char == self.TYPE_ARRAY:
      for _ in range(size):
        _, res = self._read_and_exec_opcode(ident=ident+1)
        log_debug("Object value: %s" % str(res), ident)
        array.append(res)
    else:
      for _ in range(size):
        res = self._read_value(type_char, ident)
        log_debug("Native value: %s" % str(res), ident)
        array.append(res)

    return array

  def do_reference(self, parent=None, ident=0):
    (handle, ) = self._readStruct(">L")
    log_debug("## Reference handle: 0x%x" % (handle), ident)
    return self.references[handle - self.BASE_REFERENCE_IDX]

  # pylint: disable=no-self-use
  def do_null(self, parent=None, ident=0):
    return None

  def do_enum(self, parent=None, ident=0):
    # TC_ENUM classDesc newHandle enumConstantName
    enum = JavaObject()
    _ = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_CLASSDESC,
                               self.TC_PROXYCLASSDESC, self.TC_NULL, self.TC_REFERENCE])
    self._add_reference(enum)
    _, enumConstantName = self._read_and_exec_opcode(
        ident=ident+1, expect=[self.TC_STRING, self.TC_REFERENCE])
    return enumConstantName

  def _create_hexdump(self, src, length=16):
    FILTER = ''.join([(len(repr(chr(x))) == 3) and chr(x) or '.' for x in range(256)])
    result = []
    for i in range(0, len(src), length):
      s = src[i:i+length]
      hexa = ' '.join(["%02X"%ord(x) for x in s])
      printable = s.translate(FILTER)
      result.append("%04X   %-*s  %s\n" % (i, length*3, hexa, printable))
    return ''.join(result)

  def _read_value(self, field_type, ident, name=""):
    if len(field_type) > 1:
      field_type = field_type[0]  # We don't need details for arrays and objects

    if field_type == self.TYPE_BOOLEAN:
      (val, ) = self._readStruct(">B")
      res = bool(val)
    elif field_type == self.TYPE_BYTE:
      (res, ) = self._readStruct(">b")
    elif field_type == self.TYPE_SHORT:
      (res, ) = self._readStruct(">h")
    elif field_type == self.TYPE_INTEGER:
      (res, ) = self._readStruct(">i")
    elif field_type == self.TYPE_LONG:
      (res, ) = self._readStruct(">q")
    elif field_type == self.TYPE_FLOAT:
      (res, ) = self._readStruct(">f")
    elif field_type == self.TYPE_DOUBLE:
      (res, ) = self._readStruct(">d")
    elif field_type == self.TYPE_OBJECT or field_type == self.TYPE_ARRAY:
      _, res = self._read_and_exec_opcode(ident=ident+1)
    else:
      raise RuntimeError("Unknown typecode: %s" % field_type)
    log_debug("* %s %s: " % (field_type, name) + str(res), ident)
    return res

  def _convert_char_to_type(self, type_char):
    typecode = type_char
    if isinstance(type_char, int):
      typecode = chr(type_char)

    if typecode in self.TYPECODES_LIST:
      return typecode
    else:
      raise RuntimeError("Typecode %s (%s) isn't supported." % (type_char, typecode))

  def _add_reference(self, obj):
    self.references.append(obj)

  def _oops_dump_state(self):
    log_error("==Oops state dump" + "=" * (30 - 17))
    log_error("References: %s" % str(self.references))
    log_error("Stream seeking back at -16 byte (2nd line is an actual position!):")
    self.object_stream.seek(-16, mode=1)
    the_rest = self.object_stream.read()
    if len(the_rest):
      log_error("Warning!!!!: Stream still has %s bytes left." % len(the_rest))
      log_error(self._create_hexdump(the_rest))
    log_error("=" * 30)


class JavaObjectMarshaller(JavaObjectConstants):

  def __init__(self, stream=None):
    self.object_stream = stream

  # pylint: disable=attribute-defined-outside-init
  def dump(self, obj):
    self.object_obj = obj
    self.object_stream = six.StringIO()
    self._writeStreamHeader()
    self.writeObject(obj)
    return self.object_stream.getvalue()

  def _writeStreamHeader(self):
    self._writeStruct(">HH", 4, (self.STREAM_MAGIC, self.STREAM_VERSION))

  def writeObject(self, obj):
    log_debug("Writing object of type " + str(type(obj)))
    if isinstance(obj, JavaObject):
      self.write_object(obj)
    elif isinstance(obj, str):
      self.write_blockdata(obj)
    else:
      raise RuntimeError("Object serialization of type %s is not supported." % str(type(obj)))

  def _writeStruct(self, unpack, _, args):
    ba = struct.pack(unpack, *args)
    self.object_stream.write(ba)

  def _writeString(self, string):
    l = len(string)
    self._writeStruct(">H", 2, (l, ))
    self.object_stream.write(string)

  # pylint: disable=unused-argument
  def write_blockdata(self, obj, parent=None):
    # TC_BLOCKDATA (unsigned byte)<size> (byte)[size]
    self._writeStruct(">B", 1, (self.TC_BLOCKDATA, ))
    if isinstance(obj, str):
      self._writeStruct(">B", 1, (len(obj), ))
      self.object_stream.write(obj)

  def write_object(self, obj, parent=None):
    self._writeStruct(">B", 1, (self.TC_OBJECT, ))
    self._writeStruct(">B", 1, (self.TC_CLASSDESC, ))

class DefaultObjectTransformer(object):

  class JavaList(list, JavaObject):
    pass

  class JavaMap(dict, JavaObject):
    pass

  def transform(self, obj):
    if obj.get_class().name == "java.util.ArrayList":
      #    * @serialData The length of the array backing the <tt>ArrayList</tt>
      #    *             instance is emitted (int), followed by all of its elements
      #    *             (each an <tt>Object</tt>) in the proper order.
      new_object = self.JavaList()
      obj.copy(new_object)
      new_object.extend(obj.annotations[1:])
      return new_object
    if obj.get_class().name == "java.util.LinkedList":
      new_object = self.JavaList()
      obj.copy(new_object)
      new_object.extend(obj.annotations[1:])
      return new_object
    # pylint: disable=redefined-variable-type
    if obj.get_class().name == "java.util.HashMap":
      new_object = self.JavaMap()
      obj.copy(new_object)

      for i in range((len(obj.annotations)-1)/2):
        new_object[obj.annotations[i*2+1]] = obj.annotations[i*2+2]

      return new_object

    return obj
