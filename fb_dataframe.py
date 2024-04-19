import flatbuffers
import pandas as pd
import types
from Dataframe import DataFrame
from Dataframe import Column
from Dataframe import ColumnMetadata
from Dataframe import ValueType

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
            dtype = ValueType.ValueType().Int
        elif col_dtype == 'float64':
            dtype = ValueType.ValueType().Float
        else:
            dtype = ValueType.ValueType().String
        ColumnMetadata.Start(builder)
        ColumnMetadata.AddName(builder, name)
        ColumnMetadata.AddDtype(builder, dtype)
        column_metadata.append(ColumnMetadata.End(builder))

    # Create columns
    columns = []
    for col_name, col_data in df.iteritems():
        col_dtype = df[col_name].dtype
        Column.Start(builder)
        if col_dtype == 'int64':
            Column.AddDtype(builder, ValueType.ValueType().Int)
            int_data = builder.CreateNumpyVector(col_data.values)
            Column.AddIntValues(builder, int_data)
        elif col_dtype == 'float64':
            Column.AddDtype(builder, ValueType.ValueType().Float)
            float_data = builder.CreateNumpyVector(col_data.values)
            Column.AddFloatValues(builder, float_data)
        else:
            Column.AddDtype(builder, ValueType.ValueType().String)
            string_data = [builder.CreateString(str(val)) for val in col_data.values]
            Column.StartStringValuesVector(builder, len(string_data))
            for s in reversed(string_data):
                builder.PrependUOffsetTRelative(s)
            string_data = builder.EndVector(len(string_data))
            Column.AddStringValues(builder, string_data)
        metadata = column_metadata.pop()
        Column.AddMetadata(builder, metadata)
        columns.append(Column.End(builder))

    # Create Dataframe
    DataFrame.StartColumnsVector(builder, len(columns))
    for col in reversed(columns):
        builder.PrependUOffsetTRelative(col)
    columns_data = builder.EndVector(len(columns))

    metadata_string = builder.CreateString("DataFrame Metadata")
    DataFrame.Start(builder)
    DataFrame.AddMetadata(builder, metadata_string)
    DataFrame.AddColumns(builder, columns_data)
    df_obj = DataFrame.End(builder)
    builder.Finish(df_obj)

    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
    similar to df.head(). If there are less than n rows, return the entire Dataframe.
    """
    df = DataFrame.GetRootAs(fb_bytes, 0)

    columns = []
    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        metadata = col.Metadata()
        col_name = metadata.Name().decode('utf-8')
        if metadata.Dtype() == ValueType.ValueType().Int:
            column_data = list(col.IntValuesAsNumpy())[:rows]
        elif metadata.Dtype() == ValueType.ValueType().Float:
            column_data = list(col.FloatValuesAsNumpy())[:rows]
        else:
            column_data = [col.StringValues(j).decode('utf-8') for j in range(min(rows, col.StringValuesLength()))]
        columns.append(pd.Series(column_data, name=col_name))

    return pd.DataFrame({col.name: col for col in columns})

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
    Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
    and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.
    """
    df = DataFrame.GetRootAs(fb_bytes, 0)

    # Find the column indexes for grouping and summing
    grouping_col_idx = None
    sum_col_idx = None
    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        metadata = col.Metadata()
        col_name = metadata.Name().decode('utf-8')
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
    if grouping_col.Metadata().Dtype() == ValueType.ValueType().Int:
        grouping_values = grouping_col.IntValuesAsNumpy()
    elif grouping_col.Metadata().Dtype() == ValueType.ValueType().Float:
        grouping_values = grouping_col.FloatValuesAsNumpy()
    else:
        grouping_values = [grouping_col.StringValues(j).decode('utf-8') for j in range(grouping_col.StringValuesLength())]

    if sum_col.Metadata().Dtype() == ValueType.ValueType().Int:
        sum_values = sum_col.IntValuesAsNumpy()
    elif sum_col.Metadata().Dtype() == ValueType.ValueType().Float:
        sum_values = sum_col.FloatValuesAsNumpy()
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
    """
    df = DataFrame.GetRootAs(fb_buf, 0)

    # Find the column index
    col_idx = None
    for i in range(df.ColumnsLength()):
        col = df.Columns(i)
        metadata = col.Metadata()
        if metadata.Name().decode('utf-8') == col_name:
            col_idx = i
            break

    if col_idx is None:
        return  # Column not found, do nothing

    col = df.Columns(col_idx)
    col_dtype = col.Metadata