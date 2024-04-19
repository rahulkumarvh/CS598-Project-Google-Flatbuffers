import flatbuffers
import pandas as pd
from DataFrame import DataFrame
from DataFrame import Column
from DataFrame import Metadata
from DataFrame import ValueType
import types


def to_flatbuffer(df: pd.DataFrame) -> bytes:
    """
    Converts a DataFrame to a FlatBuffer. Returns the bytes of the FlatBuffer.
    """
    builder = flatbuffers.Builder(1024)

    # Create metadata string for the DataFrame
    metadata_string = builder.CreateString("DataFrame Metadata")

    # Create a list to store column data
    columns = []

    # Iterate over DataFrame columns
    for column_name, dtype in df.dtypes.items():
        # Create Metadata for the column
        metadata = Metadata.Metadata.CreateMetadata(builder, builder.CreateString(column_name), 
                                                    ValueType.ValueType().Int if dtype == 'int64' else 
                                                    ValueType.ValueType().Float if dtype == 'float64' else 
                                                    ValueType.ValueType().String)

        # Create values for the column based on data type
        values = None
        if dtype == 'int64':
            values = builder.CreateNumpyVector(df[column_name].astype(int).values)
        elif dtype == 'float64':
            values = builder.CreateNumpyVector(df[column_name].astype(float).values)
        elif dtype == 'object':
            values = [builder.CreateString(str(val)) for val in df[column_name].values]
            values = builder.CreateStringVector(values)

        # Create Column and add it to the list
        columns.append(Column.Column.CreateColumn(builder, metadata, values))

    # Create a vector of Column objects
    DataFrame.DataFrameStartColumnsVector(builder, len(columns))
    for column in reversed(columns):
        builder.PrependUOffsetTRelative(column)
    columns_vector = builder.EndVector(len(columns))

    # Create the DataFrame object
    DataFrame.DataFrameStart(builder)
    DataFrame.DataFrameAddMetadata(builder, metadata_string)
    DataFrame.DataFrameAddColumns(builder, columns_vector)
    df_data = DataFrame.DataFrameEnd(builder)

    # Finish building the FlatBuffer
    builder.Finish(df_data)

    # Get the bytes from the builder
    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
    Returns the first n rows of the FlatBuffer DataFrame as a Pandas DataFrame.
    """
    df = DataFrame.DataFrame.GetRootAs(fb_bytes, 0)
    num_columns = df.ColumnsLength()
    data = {}

    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()

        if metadata.Dtype() == ValueType.ValueType().Int:
            values = [column.IntValues(j) for j in range(min(rows, column.IntValuesLength()))]
        elif metadata.Dtype() == ValueType.ValueType().Float:
            values = [column.FloatValues(j) for j in range(min(rows, column.FloatValuesLength()))]
        elif metadata.Dtype() == ValueType.ValueType().String:
            values = [column.StringValues(j).decode() for j in range(min(rows, column.StringValuesLength()))]
        else:
            continue  # Skip unsupported column types

        data[col_name] = values

    return pd.DataFrame(data)

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
    