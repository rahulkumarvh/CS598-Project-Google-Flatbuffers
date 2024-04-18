# automatically generated by the FlatBuffers compiler, do not modify

# namespace: CS598

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Column(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Column()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsColumn(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Column
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Column
    def Name(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # Column
    def DataType(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Uint8Flags, o + self._tab.Pos)
        return 0

    # Column
    def IntData(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int64Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 8))
        return 0

    # Column
    def IntDataAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int64Flags, o)
        return 0

    # Column
    def IntDataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Column
    def IntDataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        return o == 0

    # Column
    def FloatData(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Float32Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return 0

    # Column
    def FloatDataAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Float32Flags, o)
        return 0

    # Column
    def FloatDataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Column
    def FloatDataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        return o == 0

    # Column
    def StringData(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.String(a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return ""

    # Column
    def StringDataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Column
    def StringDataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(12))
        return o == 0

def ColumnStart(builder):
    builder.StartObject(5)

def Start(builder):
    ColumnStart(builder)

def ColumnAddName(builder, name):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(name), 0)

def AddName(builder, name):
    ColumnAddName(builder, name)

def ColumnAddDataType(builder, dataType):
    builder.PrependUint8Slot(1, dataType, 0)

def AddDataType(builder, dataType):
    ColumnAddDataType(builder, dataType)

def ColumnAddIntData(builder, intData):
    builder.PrependUOffsetTRelativeSlot(2, flatbuffers.number_types.UOffsetTFlags.py_type(intData), 0)

def AddIntData(builder, intData):
    ColumnAddIntData(builder, intData)

def ColumnStartIntDataVector(builder, numElems):
    return builder.StartVector(8, numElems, 8)

def StartIntDataVector(builder, numElems):
    return ColumnStartIntDataVector(builder, numElems)

def ColumnAddFloatData(builder, floatData):
    builder.PrependUOffsetTRelativeSlot(3, flatbuffers.number_types.UOffsetTFlags.py_type(floatData), 0)

def AddFloatData(builder, floatData):
    ColumnAddFloatData(builder, floatData)

def ColumnStartFloatDataVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartFloatDataVector(builder, numElems):
    return ColumnStartFloatDataVector(builder, numElems)

def ColumnAddStringData(builder, stringData):
    builder.PrependUOffsetTRelativeSlot(4, flatbuffers.number_types.UOffsetTFlags.py_type(stringData), 0)

def AddStringData(builder, stringData):
    ColumnAddStringData(builder, stringData)

def ColumnStartStringDataVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartStringDataVector(builder, numElems):
    return ColumnStartStringDataVector(builder, numElems)

def ColumnEnd(builder):
    return builder.EndObject()

def End(builder):
    return ColumnEnd(builder)
