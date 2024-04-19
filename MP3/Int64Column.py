# automatically generated by the FlatBuffers compiler, do not modify

# namespace: MP3

import flatbuffers
from flatbuffers.compat import import_numpy
np = import_numpy()

class Int64Column(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAs(cls, buf, offset=0):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = Int64Column()
        x.Init(buf, n + offset)
        return x

    @classmethod
    def GetRootAsInt64Column(cls, buf, offset=0):
        """This method is deprecated. Please switch to GetRootAs."""
        return cls.GetRootAs(buf, offset)
    # Int64Column
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # Int64Column
    def Data(self, j):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            a = self._tab.Vector(o)
            return self._tab.Get(flatbuffers.number_types.Int32Flags, a + flatbuffers.number_types.UOffsetTFlags.py_type(j * 4))
        return 0

    # Int64Column
    def DataAsNumpy(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.GetVectorAsNumpy(flatbuffers.number_types.Int32Flags, o)
        return 0

    # Int64Column
    def DataLength(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.VectorLen(o)
        return 0

    # Int64Column
    def DataIsNone(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        return o == 0

def Int64ColumnStart(builder):
    builder.StartObject(1)

def Start(builder):
    Int64ColumnStart(builder)

def Int64ColumnAddData(builder, data):
    builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(data), 0)

def AddData(builder, data):
    Int64ColumnAddData(builder, data)

def Int64ColumnStartDataVector(builder, numElems):
    return builder.StartVector(4, numElems, 4)

def StartDataVector(builder, numElems):
    return Int64ColumnStartDataVector(builder, numElems)

def Int64ColumnEnd(builder):
    return builder.EndObject()

def End(builder):
    return Int64ColumnEnd(builder)
