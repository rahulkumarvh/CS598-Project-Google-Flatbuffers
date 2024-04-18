import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here

from CS598 import Column, ColumnMetadata, DataType, Dataframe

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
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

        name_offset = builder.CreateString(col_name)
        ColumnMetadata.ColumnMetadataStart(builder)
        ColumnMetadata.ColumnMetadataAddName(builder, name_offset)
        ColumnMetadata.ColumnMetadataAddDtype(builder, dtype)
        col_metadata_offsets.append(ColumnMetadata.ColumnMetadataEnd(builder))

    ColumnMetadata.DataframeStartMetadataVector(builder, len(col_metadata_offsets))
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
            Column.ColumnStartInt64DataVector(builder, len(col_data))
            for val in reversed(col_data):
                builder.PrependInt64(val)
            int64_data_offset = builder.EndVector()
            Column.ColumnStart(builder)
            Column.ColumnAddDtype(builder, DataType.Int64)
            Column.ColumnAddInt64Data(builder, int64_data_offset)
            column_offsets.append(Column.ColumnEnd(builder))
        elif col_dtype == 'float64':
            Column.ColumnStartFloatDataVector(builder, len(col_data))
            for val in reversed(col_data):
                builder.PrependFloat64(val)
            float_data_offset = builder.EndVector()
            Column.ColumnStart(builder)
            Column.ColumnAddDtype(builder, DataType.Float)
            Column.ColumnAddFloatData(builder, float_data_offset)
            column_offsets.append(Column.ColumnEnd(builder))
        elif col_dtype == 'object':
            string_data_offsets = [builder.CreateString(str(val)) for val in reversed(col_data)]
            Column.ColumnStartStringDataVector(builder, len(string_data_offsets))
            for offset in string_data_offsets:
                builder.PrependUOffsetTRelative(offset)
            string_data_offset = builder.EndVector()
            Column.ColumnStart(builder)
            Column.ColumnAddDtype(builder, Column.DataType.String)
            Column.ColumnAddStringData(builder, string_data_offset)
            column_offsets.append(Column.ColumnEnd(builder))
    # Your code here
    columns_offset = builder.EndVector()

    # Step 3: Serialize the Dataframe
    CS598.Dataframe.DataframeStart(builder)
    CS598.Dataframe.DataframeAddMetadata(builder, metadata_offset)
    CS598.Dataframe.DataframeAddColumns(builder, columns_offset)
    fb_dataframe = CS598.Dataframe.DataframeEnd(builder)

    builder.Finish(fb_dataframe)
    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...



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
    