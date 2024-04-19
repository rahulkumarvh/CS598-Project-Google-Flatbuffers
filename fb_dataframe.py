import pandas as pd
import flatbuffers
import types
import MP3.Dataframe
import MP3.ColumnMetadata
import MP3.Column
import MP3.DataType

import flatbuffers
import pandas as pd
import struct
import time
import types

# Import the Flatbuffer definitions

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
                value_offset = builder.CreateString(value)
                value_offsets.append(value_offset)
            elif pd.api.types.is_integer_dtype(series):
                value_offset = builder.PrependInt64(value)
                value_offsets.append(value_offset)
            elif pd.api.types.is_float_dtype(series):
                value_offset = builder.PrependFloat64(value)
                value_offsets.append(value_offset)

        # Create the values vector

        for value_offset in reversed(value_offsets):
            builder.PrependUOffsetTRelative(value_offset)
        values_offset = builder.EndVector(len(value_offsets))

        # Create the column
        MP3.Dataframe.ColumnStart(builder)
        MP3.Dataframe.ColumnAddName(builder, name_offset)
        if pd.api.types.is_string_dtype(series):
            MP3.Dataframe.ColumnAddStringData(builder, values_offset)
            MP3.Dataframe.ColumnAddDtype(builder, MP3.Dataframe.DataType.String)
        elif pd.api.types.is_integer_dtype(series):
            MP3.Dataframe.ColumnAddInt64Data(builder, values_offset)
            MP3.Dataframe.ColumnAddDtype(builder, MP3.Dataframe.DataType.Int64)
        elif pd.api.types.is_float_dtype(series):
            MP3.Dataframe.ColumnAddFloatData(builder, values_offset)
            MP3.Dataframe.ColumnAddDtype(builder, MP3.Dataframe.DataType.Float)
        column_offset = MP3.Dataframe.ColumnEnd(builder)

        # Add the column offset to the list
        column_offsets.append(column_offset)

    # Create the columns vector
    MP3.Dataframe.DataframeStartColumnsVector(builder, len(column_offsets))
    for column_offset in reversed(column_offsets):
        builder.PrependUOffsetTRelative(column_offset)
    columns_offset = builder.EndVector(len(column_offsets))

    # Create the DataFrame
    MP3.Dataframe.DataframeStart(builder)
    MP3.Dataframe.DataframeAddColumns(builder, columns_offset)
    dataframe_offset = MP3.Dataframe.DataframeEnd(builder)

    # Finish the Flatbuffer
    builder.Finish(dataframe_offset)

    # Return the bytearray of the Flatbuffer
    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer MP3.Dataframe as a Pandas MP3.Dataframe
        similar to df.head(). If there are less than n rows, return the entire MP3.Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer MP3.Dataframe.
        @param rows: number of rows to return.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer MP3.Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
    return pd.DataFrame()  # REPLACE THIS WITH YOUR CODE...


def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
        Apply map_func to elements in a numeric column in the Flatbuffer MP3.Dataframe in place.
        This function shouldn't do anything if col_name doesn't exist or the specified
        column is a string column.

        @param fb_buf: buffer containing bytes of the Flatbuffer MP3.Dataframe.
        @param col_name: name of the numeric column to apply map_func to.
        @param map_func: function to apply to elements in the numeric column.
    """
    # YOUR CODE HERE...
    pass