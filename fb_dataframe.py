import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here
from Dataframe import Column, ColumnMetadata, Dataframe, DataType
from Dataframe import DataType


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

    # Step 1: Serialize metadata for each column
    col_metadata_offsets = []
    for col_name, col_dtype in zip(df.columns, df.dtypes):
        dtype = None
        if col_dtype == 'int64':
            dtype = DataType.Int64
        elif col_dtype == 'float64':
            dtype = DataType.Float
        elif col_dtype == 'object':
            dtype = DataType.String

        if dtype is not None:
            col_metadata = ColumnMetadata.CreateColumnMetadata(builder, builder.CreateString(col_name), dtype)
            col_metadata_offsets.append(col_metadata)

    # Step 2: Serialize column data
    column_offsets = []
    for col_name, col_data in df.iteritems():
        dtype = None
        if col_data.dtype == 'int64':
            dtype = DataType.Int64
            col_data = col_data.astype(int)
            col_data = col_data.values.tolist()
        elif col_data.dtype == 'float64':
            dtype = DataType.Float
            col_data = col_data.astype(float)
            col_data = col_data.values.tolist()
        elif col_data.dtype == 'object':
            dtype = DataType.String
            col_data = col_data.astype(str)
            col_data = [builder.CreateString(val) for val in col_data.values.tolist()]

        if dtype is not None:
            if dtype == DataType.Int64:
                Column.ColumnStartInt64DataVector(builder, len(col_data))
                for val in reversed(col_data):
                    builder.PrependInt64(val)
                int64_data = builder.EndVector(len(col_data))
                Column.ColumnAddInt64Data(builder, int64_data)
            elif dtype == DataType.Float:
                Column.ColumnStartFloatDataVector(builder, len(col_data))
                for val in reversed(col_data):
                    builder.PrependFloat64(val)
                float_data = builder.EndVector(len(col_data))
                Column.ColumnAddFloatData(builder, float_data)
            elif dtype == DataType.String:
                Column.ColumnStartStringDataVector(builder, len(col_data))
                for val in reversed(col_data):
                    builder.PrependUOffsetTRelative(builder.CreateString(val))
                string_data = builder.EndVector(len(col_data))
                Column.ColumnAddStringData(builder, string_data)

            Column.ColumnStart(builder)
            Column.ColumnAddDtype(builder, dtype)
            column_offset = Column.ColumnEnd(builder)
            column_offsets.append(column_offset)

    # Step 3: Serialize DataFrame
    Dataframe.DataframeStartMetadataVector(builder, len(col_metadata_offsets))
    for offset in reversed(col_metadata_offsets):
        builder.PrependUOffsetTRelative(offset)
    metadata_vector = builder.EndVector(len(col_metadata_offsets))

    Dataframe.DataframeStartColumnsVector(builder, len(column_offsets))
    for offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(offset)
    columns_vector = builder.EndVector(len(column_offsets))

    Dataframe.DataframeStart(builder)
    Dataframe.DataframeAddMetadata(builder, metadata_vector)
    Dataframe.DataframeAddColumns(builder, columns_vector)
    df_offset = Dataframe.DataframeEnd(builder)

    builder.Finish(df_offset)
    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
    similar to df.head(). If there are less than n rows, return the entire Dataframe.

    @param fb_bytes: bytes of the Flatbuffer Dataframe.
    @param rows: number of rows to return.
    """

    return pd.DataFrame()


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    # YOUR CODE HERE...
    pass
    