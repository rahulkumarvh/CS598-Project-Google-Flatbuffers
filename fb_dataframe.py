import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np
# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

import CS598.DataFrame
import CS598.FloatColumn
import CS598.IntColumn
import CS598.StringColumn

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    # Initialize the Flatbuffer builder
    builder = flatbuffers.Builder(0)

    # Create a list to hold the column offsets
    column_offsets = []

    # Iterate over the columns in the DataFrame
    for column_name, series in df.items():
        # Create the column name
        name_offset = builder.CreateString(column_name)

        # Create a list to hold the value offsets
        value_offsets = []

        # Iterate over the values in the series
        for value in series:
            # Create the value based on its type
            if pd.api.types.is_string_dtype(series):
                value_offset = CS598.StringValue.CreateStringValue(builder, builder.CreateString(value))
            elif pd.api.types.is_integer_dtype(series):
                value_offset = CS598.IntValue.CreateIntValue(builder, value)
            elif pd.api.types.is_float_dtype(series):
                value_offset = CS598.FloatValue.CreateFloatValue(builder, value)

            # Add the value offset to the list
            value_offsets.append(value_offset)

        # Create the values vector
        CS598.ColumnStartValuesVector(builder, len(value_offsets))
        for value_offset in reversed(value_offsets):
            builder.PrependUOffsetTRelative(value_offset)
        values_offset = builder.EndVector(len(value_offsets))

        # Create the column
        CS598.ColumnStart(builder)
        CS598.ColumnAddName(builder, name_offset)
        CS598.ColumnAddValues(builder, values_offset)
        column_offset = CS598.ColumnEnd(builder)

        # Add the column offset to the list
        column_offsets.append(column_offset)

    # Create the columns vector
    CS598.DataFrameStartColumnsVector(builder, len(column_offsets))
    for column_offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(column_offset)
    columns_offset = builder.EndVector(len(column_offsets))

    # Create the DataFrame
    CS598.DataFrameStart(builder)
    CS598.DataFrameAddColumns(builder, columns_offset)
    dataframe_offset = CS598.DataFrameEnd(builder)

    # Finish the Flatbuffer
    builder.Finish(dataframe_offset)

    # Return the bytearray of the Flatbuffer
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
    