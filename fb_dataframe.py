import pandas as pd
from typing import Callable, Any
import flatbuffers

import CSCS598.Dataframe
import CSCS598.ColumnMetadata
import CSCS598.Column
import CSCS598.DataType

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    """
    Converts a DataFrame to a flatbuffer. Returns the bytearray of the flatbuffer.

    The flatbuffer follows a columnar format as follows:
    +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
    | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
    +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
    """
    builder = flatbuffers.Builder(0)

    # Create column metadata
    column_metadata = []
    for col_name, col_dtype in df.dtypes.iteritems():
        name = builder.CreateString(col_name)
        if col_dtype == 'int64':
            dtype = CSCS598.DataType.Int64
        elif col_dtype == 'float64':
            dtype = CSCS598.DataType.Float
        else:
            dtype = CS598.DataType.String
        CS598.ColumnMetadata.ColumnMetadataStart(builder)
        CS598.ColumnMetadata.ColumnMetadataAddName(builder, name)
        CS598.ColumnMetadata.ColumnMetadataAddDtype(builder, dtype)
        CS598.column_metadata.append(CS598.ColumnMetadata.ColumnMetadataEnd(builder))

    # Create columns
    columns = []
    for col_name, col_data in df.iteritems():
        col_dtype = df[col_name].dtype
        CS598.Column.ColumnStart(builder)
        if col_dtype == 'int64':
            CS598.Column.ColumnAddDtype(builder, CS598.DataType.Int64)
            int_data = builder.CreateNumpyVector(col_data.values)
            CS598.Column.ColumnAddInt64Data(builder, int_data)
        elif col_dtype == 'float64':
            CS598.Column.ColumnAddDtype(builder, CS598.DataType.Float)
            float_data = builder.CreateNumpyVector(col_data.values)
            CS598.Column.ColumnAddFloatData(builder, float_data)
        else:
            CS598.Column.ColumnAddDtype(builder, CS598.DataType.String)
            string_data = [builder.CreateString(str(val)) for val in col_data.values]
            CS598.Column.ColumnStartStringDataVector(builder, len(string_data))
            for s in reversed(string_data):
                builder.PrependUOffsetTRelative(s)
            string_data = builder.EndVector(len(string_data))
            CS598.Column.ColumnAddStringData(builder, string_data)
        columns.append(CS598.Column.ColumnEnd(builder))

    # Create Dataframe
    CS598.Dataframe.DataframeStartMetadataVector(builder, len(column_metadata))
    for meta in reversed(column_metadata):
        builder.PrependUOffsetTRelative(meta)
    metadata = builder.EndVector(len(column_metadata))

    CS598.Dataframe.DataframeStartColumnsVector(builder, len(columns))
    for col in reversed(columns):
        builder.PrependUOffsetTRelative(col)
    columns_data = builder.EndVector(len(columns))

    CS598.Dataframe.DataframeStart(builder)
    CS598.Dataframe.DataframeAddMetadata(builder, metadata)
    CS598.Dataframe.DataframeAddColumns(builder, columns_data)
    df_obj = CS598.Dataframe.DataframeEnd(builder)
    builder.Finish(df_obj)

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