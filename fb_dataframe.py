import pandas as pd
import flatbuffers
import types
import MP3.Dataframe
import MP3.ColumnMetadata
import MP3.Column
import MP3.DataType

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
    for col_name, col_dtype in df.dtypes.items():
        name = builder.CreateString(col_name)
        if col_dtype == 'int64':
            dtype = MP3.DataType.Int64
        elif col_dtype == 'float64':
            dtype = MP3.DataType.Float
        else:
            dtype = MP3.DataType.String
        MP3.ColumnMetadata.ColumnMetadataStart(builder)
        MP3.ColumnMetadata.ColumnMetadataAddName(builder, name)
        MP3.ColumnMetadata.ColumnMetadataAddDtype(builder, dtype)
        column_metadata.append(MP3.ColumnMetadata.ColumnMetadataEnd(builder))

    # Create columns
    columns = []
    for col_name, col_data in df.items():
        col_dtype = df[col_name].dtype
        MP3.Column.ColumnStart(builder)
        if col_dtype == 'int64':
            MP3.Column.ColumnAddDtype(builder, MP3.DataType.Int64)
            int_data = builder.CreateNumpyVector(col_data.values)
            MP3.Column.ColumnAddInt64Data(builder, int_data)
        elif col_dtype == 'float64':
            MP3.Column.ColumnAddDtype(builder, MP3.DataType.Float)
            float_data = builder.CreateNumpyVector(col_data.values)
            MP3.Column.ColumnAddFloatData(builder, float_data)
        else:
            MP3.Column.ColumnAddDtype(builder, MP3.DataType.String)
            string_data = [builder.CreateString(str(val)) for val in col_data.values]
            MP3.Column.ColumnStartStringDataVector(builder, len(string_data))
            for s in reversed(string_data):
                builder.PrependUOffsetTRelative(s)
            string_data = builder.EndVector(len(string_data))
            MP3.Column.ColumnAddStringData(builder, string_data)
        columns.append(MP3.Column.ColumnEnd(builder))

    # Create Dataframe
    MP3.Dataframe.DataframeStartMetadataVector(builder, len(column_metadata))
    for meta in reversed(column_metadata):
        builder.PrependUOffsetTRelative(meta)
    metadata = builder.EndVector(len(column_metadata))

    MP3.Dataframe.DataframeStartColumnsVector(builder, len(columns))
    for col in reversed(columns):
        builder.PrependUOffsetTRelative(col)
    columns_data = builder.EndVector(len(columns))

    MP3.Dataframe.DataframeStart(builder)
    MP3.Dataframe.DataframeAddMetadata(builder, metadata)
    MP3.Dataframe.DataframeAddColumns(builder, columns_data)
    df_obj = MP3.Dataframe.DataframeEnd(builder)
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