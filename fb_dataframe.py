import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np

# Your Flatbuffer imports here
from Dataframe.Column import Column
from Dataframe.ColumnMetadata import ColumnMetadata
from Dataframe.DataType import DataType
from Dataframe.Dataframe import Dataframe  # Assuming this is the actual class name for Dataframe


from Dataframe.ColumnMetadata import Start, AddName, AddDtype, End, CreateString

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    """
    Converts a Pandas DataFrame to a Flatbuffer. Returns the bytearray of the Flatbuffer.

    @param df: the dataframe.
    """
    builder = flatbuffers.Builder()

    # Create a list to hold column metadata
    column_metadata_offsets = []

    for col_name, col_data in df.items():
        # Start the creation of ColumnMetadata
        Start(builder)
        
        # Add the name of the column
        name_offset = builder.CreateString(col_name)
        AddName(builder, name_offset)
        
        # Determine the data type of the column and add it
        if pd.api.types.is_string_dtype(col_data):
            dtype = DataType.String
        else:
            dtype = DataType.Int64
        AddDtype(builder, dtype)

        # End the creation of ColumnMetadata and add it to the list
        column_metadata_offset = End(builder)
        column_metadata_offsets.append(column_metadata_offset)

    # Create a vector of column metadata
    Dataframe.ColumnMetadataStart(builder)
    Dataframe.ColumnMetadataAddMetadata(builder, builder.CreateVector(column_metadata_offsets))
    column_metadata_vector = Dataframe.ColumnMetadataEnd(builder)

    # Create the Dataframe object
    Dataframe.DataframeStart(builder)
    Dataframe.AddMetadata(builder, column_metadata_vector)
    dataframe_offset = Dataframe.End(builder)

    # Finish the buffer
    builder.Finish(dataframe_offset)

    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
    similar to df.head(). If there are less than n rows, return the entire Dataframe.

    @param fb_bytes: bytes of the Flatbuffer Dataframe.
    @param rows: number of rows to return.
    """
    df = Dataframe.GetRootAsDataframe(fb_bytes)
    num_rows = min(rows, df.rows())

    # Get column names and data
    column_names = [m.name() for m in df.metadata()]
    column_data = []
    for i in range(num_rows):
        row = []
        for col in df.columns():
            if col.dtype() == DataType.Int64:
                row.append(col.Int64Data(i))
            elif col.dtype() == DataType.Float:
                row.append(col.FloatData(i))
            else:
                row.append(col.StringData(i))
        column_data.append(row)

    return pd.DataFrame(data=column_data, columns=column_names)


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
    