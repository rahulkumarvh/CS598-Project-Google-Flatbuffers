import flatbuffers
import pandas as pd
import struct
import time
import types

import MP3.Column
import MP3.ColumnMeta
import MP3.DataFrame
import MP3.Float64Column
import MP3.Int64Column
import MP3.StringColumn

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
    builder = flatbuffers.Builder(1024)

    # Start serializing the dataframe
    columns = []
    for col_name, col_data in df.items():
        # For each column, serialize the metadata
        col_meta = builder.CreateString(col_name)  # MP3.Column name as metadata
        col_type = col_data.dtype  # Data type of the column
        col_size = len(col_data)  # Number of elements in the column

        # Serialize metadata
        MP3.ColumnMeta.ColumnMetaStart(builder)
        MP3.ColumnMeta.ColumnMetaAddName(builder, col_meta)
        MP3.ColumnMeta.ColumnMetaAddType(builder, col_type)
        MP3.ColumnMeta.ColumnMetaAddSize(builder, col_size)
        col_meta_offset = MP3.ColumnMeta.ColumnMetaEnd(builder)

        # Serialize column values based on type
        if col_type == 'int64':
            data = col_data.values.tolist()
            MP3.Int64Column.Int64ColumnStartDataVector(builder, len(data))
            for val in reversed(data):  # Reversed for efficiency
                builder.PrependInt64(val)
            col_data_offset = builder.EndVector(len(data))
            MP3.Int64Column.Int64ColumnStart(builder)
            MP3.Int64Column.Int64ColumnAddData(builder, col_data_offset)
            int64_col = MP3.Int64Column.Int64ColumnEnd(builder)
            MP3.Column.ColumnStart(builder)
            MP3.Column.ColumnAddMeta(builder, col_meta_offset)
            MP3.Column.ColumnAddInt64Col(builder, int64_col)
            col_offset = MP3.Column.ColumnEnd(builder)
            columns.append(col_offset)
        elif col_type == 'float64':
            data = col_data.values.tolist()
            MP3.Float64Column.Float64ColumnStartDataVector(builder, len(data))
            for val in reversed(data):  # Reversed for efficiency
                builder.PrependFloat64(val)
            col_data_offset = builder.EndVector(len(data))
            MP3.Float64Column.Float64ColumnStart(builder)
            MP3.Float64Column.Float64ColumnAddData(builder, col_data_offset)
            float64_col = MP3.Float64Column.Float64ColumnEnd(builder)
            MP3.Column.ColumnStart(builder)
            MP3.Column.ColumnAddMeta(builder, col_meta_offset)
            MP3.Column.ColumnAddFloat64Col(builder, float64_col)
            col_offset = MP3.Column.ColumnEnd(builder)
            columns.append(col_offset)
        elif col_type == 'object':
            data = [builder.CreateString(str(val)) for val in col_data.values]
            StringColumn.StringColumnStartDataVector(builder, len(data))
            for val in reversed(data):  # Reversed for efficiency
                builder.PrependUOffsetTRelative(val)
            col_data_offset = builder.EndVector(len(data))
            StringColumn.StringColumnStart(builder)
            StringColumn.StringColumnAddData(builder, col_data_offset)
            str_col = StringColumn.StringColumnEnd(builder)
            MP3.Column.ColumnStart(builder)
            MP3.Column.ColumnAddMeta(builder, col_meta_offset)
            MP3.Column.ColumnAddStringCol(builder, str_col)
            col_offset = MP3.Column.ColumnEnd(builder)
            columns.append(col_offset)

    # Serialize the dataframe
    MP3.DataFrame.DataFrameStartColsVector(builder, len(columns))
    for col_offset in reversed(columns):  # Reversed for efficiency
        builder.PrependUOffsetTRelative(col_offset)
    cols_vector = builder.EndVector(len(columns))
    MP3.DataFrame.DataFrameStart(builder)
    MP3.DataFrame.DataFrameAddCols(builder, cols_vector)
    fb_data = MP3.DataFrame.DataFrameEnd(builder)

    builder.Finish(fb_data)
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