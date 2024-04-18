import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

import CS598.DataFrame as fbDataFrame

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    builder = flatbuffers.Builder(0)
    column_offsets = []

    for column_name, column_data in df.items():
        column_name_offset = builder.CreateString(column_name)
        data_type = 2  # Default to string
        int_data_offset = float_data_offset = string_data_offset = None

        if column_data.dtype == np.int64:
            data_type = 0
            int_data_offset = builder.CreateNumpyVector(column_data.values)
        elif column_data.dtype == np.float64:
            data_type = 1
            float_data_offset = builder.CreateNumpyVector(column_data.values)
        else:
            string_data_offset = builder.CreateNumpyVectorOfUOffsets(
                [builder.CreateString(str(val)).Value for val in column_data.values])

        fbColumn = fbDataFrame.Column.CreateColumn(builder, column_name_offset, data_type,
                                                    int_data_offset, float_data_offset, string_data_offset)
        column_offsets.append(fbColumn)

    fbDataFrame.DataFrame.ColumnStartColumnsVector(builder, len(column_offsets))
    for offset in column_offsets:
        builder.PrependUOffsetTRelative(offset)
    columns_offset = builder.EndVector(len(column_offsets))

    fbDataFrame.DataFrame.ColumnStart(builder)
    fbDataFrame.DataFrame.ColumnAddColumns(builder, columns_offset)
    df_offset = fbDataFrame.DataFrame.ColumnEnd(builder)

    builder.Finish(df_offset)
    return builder.Output()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    df = fbDataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
    columns = []
    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        column_name = column.Name().decode('utf-8')
        data_type = column.DataType()
        if data_type == 0:  # int64
            column_data = column.IntData().AsNumpy()[:rows]
        elif data_type == 1:  # float64
            column_data = column.FloatData().AsNumpy()[:rows]
        else:  # string
            column_data = [column.StringData(j).decode('utf-8') for j in range(rows)]
        columns.append(pd.Series(column_data, name=column_name))

    return pd.DataFrame({column.name: column for column in columns})  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    df = fbDataFrame.DataFrame.GetRootAsDataFrame(fb_bytes, 0)
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
                column_data.append(list(column.IntData().AsNumpy()))
            elif data_type == 1:  # float64
                column_data.append(list(column.FloatData().AsNumpy()))
            else:  # string
                column_data.append([column.StringData(j).decode('utf-8') for j in range(column.StringDataLength())])
        elif column_name == sum_col_name:
            sum_col_idx = i
            if data_type == 0:  # int64
                column_data.append(list(column.IntData().AsNumpy()))
            elif data_type == 1:  # float64
                column_data.append(list(column.FloatData().AsNumpy()))
            else:  # string
                column_data.append([column.StringData(j).decode('utf-8') for j in range(column.StringDataLength())])
        else:
            column_data.append([])

    if group_col_idx is None or sum_col_idx is None:
        return pd.DataFrame()

    result_df = pd.DataFrame({column_names[i]: column_data[i] for i in range(len(column_names))})
    grouped = result_df.groupby(column_names[group_col_idx])
    agg_result = grouped[column_names[sum_col_idx]].sum()

    return agg_result  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    df = fbDataFrame.DataFrame.GetRootAsDataFrame(fb_buf, 0)
    map_col_idx = None

    for i in range(df.ColumnsLength()):
        column = df.Columns(i)
        column_name = column.Name().decode('utf-8')
        if column_name == col_name:
            map_col_idx = i
            data_type = column.DataType()
            if data_type == 0:  # int64
                data_start = column.IntData().DataAsNumpy().ctypes.data
                for j in range(len(column.IntData().AsNumpy())):
                    value = struct.unpack('<q', fb_buf[data_start:data_start+8])[0]
                    new_value = map_func(value)
                    fb_buf[data_start:data_start+8] = struct.pack('<q', new_value)
                    data_start += 8
            elif data_type == 1:  # float64
                data_start = column.FloatData().DataAsNumpy().ctypes.data
                for j in range(len(column.FloatData().AsNumpy())):
                    value = struct.unpack('<d', fb_buf[data_start:data_start+8])[0]
                    new_value = map_func(value)
                    fb_buf[data_start:data_start+8] = struct.pack('<d', new_value)
                    data_start += 8
            break
    
