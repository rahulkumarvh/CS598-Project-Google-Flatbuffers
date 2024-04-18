import pandas as pd
from typing import Callable, Any
import flatbuffers
from CS598 import Dataframe, Column, ColumnMetadata, DataType

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
            dtype = DataType.Int64
        elif col_dtype == 'float64':
            dtype = DataType.Float
        else:
            dtype = DataType.String
        ColumnMetadata.ColumnMetadataStart(builder)
        ColumnMetadata.ColumnMetadataAddName(builder, name)
        ColumnMetadata.ColumnMetadataAddDtype(builder, dtype)
        column_metadata.append(ColumnMetadata.ColumnMetadataEnd(builder))

    # Create columns
    columns = []
    for col_name, col_data in df.iteritems():
        col_dtype = df[col_name].dtype
        Column.ColumnStart(builder)
        if col_dtype == 'int64':
            Column.ColumnAddDtype(builder, DataType.Int64)
            int_data = builder.CreateNumpyVector(col_data.values)
            Column.ColumnAddInt64Data(builder, int_data)
        elif col_dtype == 'float64':
            Column.ColumnAddDtype(builder, DataType.Float)
            float_data = builder.CreateNumpyVector(col_data.values)
            Column.ColumnAddFloatData(builder, float_data)
        else:
            Column.ColumnAddDtype(builder, DataType.String)
            string_data = [builder.CreateString(str(val)) for val in col_data.values]
            Column.ColumnStartStringDataVector(builder, len(string_data))
            for s in reversed(string_data):
                builder.PrependUOffsetTRelative(s)
            string_data = builder.EndVector(len(string_data))
            Column.ColumnAddStringData(builder, string_data)
        columns.append(Column.ColumnEnd(builder))

    # Create Dataframe
    Dataframe.DataframeStartMetadataVector(builder, len(column_metadata))
    for meta in reversed(column_metadata):
        builder.PrependUOffsetTRelative(meta)
    metadata = builder.EndVector(len(column_metadata))

    Dataframe.DataframeStartColumnsVector(builder, len(columns))
    for col in reversed(columns):
        builder.PrependUOffsetTRelative(col)
    columns_data = builder.EndVector(len(columns))

    Dataframe.DataframeStart(builder)
    Dataframe.DataframeAddMetadata(builder, metadata)
    Dataframe.DataframeAddColumns(builder, columns_data)
    df_obj = Dataframe.DataframeEnd(builder)
    builder.Finish(df_obj)

    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
    similar to df.head(). If there are less than n rows, return the entire Dataframe.
    """
    df = Dataframe.Dataframe.GetRootAs(fb_bytes, 0)

    columns = []
    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        column_name = df.Metadata(i).Name().decode('utf-8')
        column_type = df.Metadata(i).Dtype()
        if column_type == DataType.Int64:
            column_data = list(col.Int64DataAsNumpy())[:rows]
        elif column_type == DataType.Float:
            column_data = list(col.FloatDataAsNumpy())[:rows]
        else:
            column_data = [col.StringData(j).decode('utf-8') for j in range(min(rows, col.StringDataLength()))]
        columns.append(pd.Series(column_data, name=column_name))

    return pd.DataFrame({col.name: col for col in columns})

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
    Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
    and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.
    """
    df = Dataframe.Dataframe.GetRootAs(fb_bytes, 0)

    # Find the column indexes for grouping and summing
    grouping_col_idx = None
    sum_col_idx = None
    for i in range(df.MetadataLength()):
        col_meta = df.Metadata(i)
        col_name = col_meta.Name().decode('utf-8')
        if col_name == grouping_col_name:
            grouping_col_idx = i
        elif col_name == sum_col_name:
            sum_col_idx = i

    if grouping_col_idx is None or sum_col_idx is None:
        raise ValueError(f"Column '{grouping_col_name}' or '{sum_col_name}' not found in the dataframe.")

    # Extract the grouping and summing columns
    grouping_col = df.Columns(grouping_col_idx)
    sum_col = df.Columns(sum_col_idx)

    # Perform groupby sum
    groupby_dict = {}
    if grouping_col.Dtype() == DataType.Int64:
        grouping_values = grouping_col.Int64DataAsNumpy()
    elif grouping_col.Dtype() == DataType.Float:
        grouping_values = grouping_col.FloatDataAsNumpy()
    else:
        grouping_values = [grouping_col.StringData(j).decode('utf-8') for j in range(grouping_col.StringDataLength())]

    if sum_col.Dtype() == DataType.Int64:
        sum_values = sum_col.Int64DataAsNumpy()
    elif sum_col.Dtype() == DataType.Float:
        sum_values = sum_col.FloatDataAsNumpy()
    else:
        raise ValueError("The sum column must be numeric.")

    for group, value in zip(grouping_values, sum_values):
        groupby_dict[group] = groupby_dict.get(group, 0) + value

    return pd.DataFrame({'GroupBy': groupby_dict.keys(), sum_col_name: groupby_dict.values()})


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
    