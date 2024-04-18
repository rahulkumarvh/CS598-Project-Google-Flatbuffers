import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here
from DataFrame import Column, ColumnMetadata, Dataframe, DataType
from DataFrame import DataType


def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    builder = flatbuffers.Builder(1024)

    # Step 1: Serialize metadata for each column
    col_metadata_offsets = []
    for col_name, col_dtype in zip(df.columns, df.dtypes):
        dtype = None
        if col_dtype == 'int64':
            dtype = DataFrame.DataType.Int64
        elif col_dtype == 'float64':
            dtype = DataFrame.DataType.Float
        elif col_dtype == 'object':
            dtype = DataFrame.DataType.String

        name_offset = builder.CreateString(col_name)
        DataFrame.ColumnMetadataStart(builder)
        DataFrame.ColumnMetadataAddName(builder, name_offset)
        DataFrame.ColumnMetadataAddDtype(builder, dtype)
        col_metadata_offsets.append(DataFrame.ColumnMetadataEnd(builder))

    DataFrame.DataframeStartMetadataVector(builder, len(col_metadata_offsets))
    for offset in reversed(col_metadata_offsets):
        builder.PrependUOffsetTRelative(offset)
    metadata_offset = builder.EndVector()

    # Step 2: Serialize data for each column
    column_offsets = []
    for col_name, col_data in df.iteritems():
        col_data = col_data.dropna()  # Drop NaN values
        col_dtype = df[col_name].dtype
        col_data = col_data.values  # Convert to numpy array for performance

        # Convert data to appropriate FlatBuffer vector type
        if col_dtype == 'int64':
            DataFrame.ColumnStartInt64DataVector(builder, len(col_data))
            for val in reversed(col_data):
                builder.PrependInt64(val)
            int64_data_offset = builder.EndVector()
            DataFrame.ColumnStart(builder)
            DataFrame.ColumnAddDtype(builder, DataFrame.DataType.Int64)
            DataFrame.ColumnAddInt64Data(builder, int64_data_offset)
            column_offsets.append(DataFrame.ColumnEnd(builder))
        elif col_dtype == 'float64':
            DataFrame.ColumnStartFloatDataVector(builder, len(col_data))
            for val in reversed(col_data):
                builder.PrependFloat64(val)
            float_data_offset = builder.EndVector()
            DataFrame.ColumnStart(builder)
            DataFrame.ColumnAddDtype(builder, DataFrame.DataType.Float)
            DataFrame.ColumnAddFloatData(builder, float_data_offset)
            column_offsets.append(DataFrame.ColumnEnd(builder))
        elif col_dtype == 'object':
            string_data_offsets = [builder.CreateString(str(val)) for val in reversed(col_data)]
            DataFrame.ColumnStartStringDataVector(builder, len(string_data_offsets))
            for offset in string_data_offsets:
                builder.PrependUOffsetTRelative(offset)
            string_data_offset = builder.EndVector()
            DataFrame.ColumnStart(builder)
            DataFrame.ColumnAddDtype(builder, DataFrame.DataType.String)
            DataFrame.ColumnAddStringData(builder, string_data_offset)
            column_offsets.append(DataFrame.ColumnEnd(builder))

    DataFrame.DataframeStartColumnsVector(builder, len(column_offsets))
    for offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(offset)
    columns_offset = builder.EndVector()

    # Step 3: Serialize the Dataframe
    DataFrame.DataframeStart(builder)
    DataFrame.DataframeAddMetadata(builder, metadata_offset)
    DataFrame.DataframeAddColumns(builder, columns_offset)
    fb_dataframe = DataFrame.DataframeEnd(builder)

    builder.Finish(fb_dataframe)
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
    