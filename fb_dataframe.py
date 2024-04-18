import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np
from CS598.Column import ColumnAddFloatData, ColumnAddIntData, ColumnAddName, ColumnAddStringData, ColumnEnd, ColumnStart
from CS598.Column import ColumnAddDataType
import CS598.DataFrame as df_fb

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

import CS598.DataFrame as DataFrame
import CS598.Column as Column

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytearray of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    builder = flatbuffers.Builder(1024)
    
    # Start building the Flatbuffer
    col_offsets = []
    for col_name, col_data in df.items():
        col_type = col_data.dtype
        if col_type == 'int64':
            # Rest of the code...
            col_offsets.append(builder.EndCol())
        elif col_type == 'float64':
            # Add float64 column data
            df_fb.DataFrame.StartFloatdataVector(builder, len(col_data))
            for val in col_data:
                builder.PrependFloat64(val)
            floatdata = builder.EndVector(len(col_data))
            df_fb.DataFrame.StartCol(builder)
            df_fb.DataFrame.AddName(builder, builder.CreateString(col_name))
            df_fb.DataFrame.AddType(builder, df_fb.DataType.Float)
            df_fb.DataFrame.AddData(builder, floatdata)
            col_offsets.append(builder.EndCol())
        elif col_type == 'object':
            # Add object (string) column data
            string_offsets = [builder.CreateString(str(val)) for val in col_data]
            df_fb.DataFrame.StartStringdataVector(builder, len(string_offsets))
            for string_offset in reversed(string_offsets):
                builder.PrependUOffsetTRelative(string_offset)
            stringdata = builder.EndVector(len(string_offsets))
            df_fb.DataFrame.StartCol(builder)
            df_fb.DataFrame.AddName(builder, builder.CreateString(col_name))
            df_fb.DataFrame.AddType(builder, df_fb.DataType.String)
            df_fb.DataFrame.AddData(builder, stringdata)
            col_offsets.append(builder.EndCol())

    # Add column metadata
    df_fb.DataFrame.StartColsVector(builder, len(col_offsets))
    for col_offset in reversed(col_offsets):
        builder.PrependUOffsetTRelative(col_offset)
    cols = builder.EndVector(len(col_offsets))

    # Add dataframe metadata
    df_fb.DataFrame.Start(builder)
    df_fb.DataFrame.AddCols(builder, cols)
    fb_df = df_fb.DataFrame.End(builder)

    builder.Finish(fb_df)
    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    columns = []
    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        column_name = column.Name().decode('utf-8')
        data_type = column.DataType()
        if data_type == 0:  # int64
            column_data = column.IntDataAsNumpy()[:rows]
        elif data_type == 1:  # float64
            column_data = column.FloatDataAsNumpy()[:rows]
        else:  # string
            column_data = [column.StringData(j).decode('utf-8') for j in range(column.StringDataLength())][:rows]
        columns.append(pd.Series(column_data, name=column_name))

    return pd.DataFrame({column.name: column for column in columns})

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    group_col_idx = None
    sum_col_idx = None
    column_names = []
    column_data = []

    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        column_name = column.Name().decode('utf-8')
        column_names.append(column_name)
        data_type = column.DataType()
        if column_name == grouping_col_name:
            group_col_idx = i
            if data_type == 0:  # int64
                column_data.append(list(column.IntDataAsNumpy()))
            elif data_type == 1:  # float64
                column_data.append(list(column.FloatDataAsNumpy()))
            else:  # string
                column_data.append([column.StringData(j).decode('utf-8') for j in range(column.StringDataLength())])
        elif column_name == sum_col_name:
            sum_col_idx = i
            if data_type == 0:  # int64
                column_data.append(list(column.IntDataAsNumpy()))
            elif data_type == 1:  # float64
                column_data.append(list(column.FloatDataAsNumpy()))
            else:  # string
                column_data.append([column.StringData(j).decode('utf-8') for j in range(column.StringDataLength())])

    if group_col_idx is None or sum_col_idx is None:
        return pd.DataFrame()

    result_df = pd.DataFrame({column_names[i]: column_data[i] for i in range(len(column_names))})
    grouped = result_df.groupby(column_names[group_col_idx])
    agg_result = grouped[column_names[sum_col_idx]].sum()

    return agg_result

def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    df = DataFrame.DataFrame.GetRootAsDataFrame(fb_buf, 0)
    map_col_idx = None

    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        column_name = column.Name().decode('utf-8')
        if column_name == col_name:
            map_col_idx = i
            data_type = column.DataType()
            if data_type == 0:  # int64
                data_start = column.IntDataAsNumpy().ctypes.data
                for j in range(len(column.IntDataAsNumpy())):
                    value = struct.unpack('<q', fb_buf[data_start:data_start+8])[0]
                    new_value = map_func(value)
                    fb_buf[data_start:data_start+8] = struct.pack('<q', new_value)
                    data_start += 8
            elif data_type == 1:  # float64
                data_start = column.FloatDataAsNumpy().ctypes.data
                for j in range(len(column.FloatDataAsNumpy())):
                    value = struct.unpack('<d', fb_buf[data_start:data_start+8])[0]
                    new_value = map_func(value)
                    fb_buf[data_start:data_start+8] = struct.pack('<d', new_value)
                    data_start += 8
            break