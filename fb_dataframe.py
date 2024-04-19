import flatbuffers
import pandas as pd
import struct
import time
import types
from CS598 import DataFrame
from CS598 import Column
from CS598 import Metadata
from CS598 import DataType  

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    builder = flatbuffers.Builder(1024)
    metadata_string = builder.CreateString("DataFrame Metadata")
    column_metadata_list = []
    value_vectors = []
    value_vectors_dtype = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'int64':
            data_type = DataType.DataType().Int
        elif dtype == 'float64':
            data_type = DataType.DataType().Float
        elif dtype == 'object':
            data_type = DataType.DataType().String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        column_metadata_list.append((column_name, data_type))

        # Convert column values to FlatBuffer values
        column_values = df[column_name]
        value_vectors.append(column_values.tolist())
        value_vectors_dtype.append(dtype)
    columns = []
    for dtype, metadata, value_vector in reversed(list(zip(value_vectors_dtype ,column_metadata_list, value_vectors))):
        if dtype == 'int64':
            Column.StartIntValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependInt64(value)
            values = builder.EndVector(len(value_vector))

            col_name = builder.CreateString(metadata[0])
            data_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, data_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddIntValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'float64':
            Column.StartFloatValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependFloat64(value)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            data_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, data_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddFloatValues(builder, values)
            columns.append(Column.End(builder))
        elif dtype == 'object':
            str_offsets = [builder.CreateString(str(value)) for value in value_vector]
            Column.StartStringValuesVector(builder, len(value_vector))
            for offset in reversed(str_offsets):
                builder.PrependUOffsetTRelative(offset)
            values = builder.EndVector(len(value_vector))
            
            col_name = builder.CreateString(metadata[0])
            data_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, data_type)
            meta = Metadata.End(builder)
            Column.Start(builder)            
            Column.AddMetadata(builder, meta)
            Column.AddStringValues(builder, values)
            columns.append(Column.End(builder))

    # Create a vector of Column objects
    DataFrame.StartColumnsVector(builder, len(columns))
    for column in columns:
        builder.PrependUOffsetTRelative(column)
    columns_vector = builder.EndVector(len(columns))
    

    # Create the DataFrame object
    DataFrame.Start(builder)
    DataFrame.AddMetadata(builder, metadata_string)
    DataFrame.AddColumns(builder, columns_vector)
    df_data = DataFrame.End(builder)

    # Finish building the FlatBuffer
    builder.Finish(df_data)
    # Get the bytes from the builder
    return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    df = DataFrame.DataFrame.GetRootAs(fb_bytes,0)

    num_columns = df.ColumnsLength()
    data = {}

    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()
        
        if metadata.Dtype() == DataType.DataType.Int:
            values = [column.IntValues(j) for j in range(min(rows, column.IntValuesLength()))]
        elif metadata.Dtype() == DataType.DataType.Float:
            values = [column.FloatValues(j) for j in range(min(rows, column.FloatValuesLength()))]
        elif metadata.Dtype() == DataType.DataType.String:
            values = [column.StringValues(j).decode() for j in range(min(rows, column.StringValuesLength()))]
        else:
            continue  # Skip unsupported column types

        data[col_name] = values

    # Construct and return a Pandas DataFrame
    return pd.DataFrame(data)

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    df = DataFrame.DataFrame.GetRootAs(fb_bytes,0)
    num_columns = df.ColumnsLength()
    data = {}

    # Variables to hold column data for group by and sum operations
    grouping_data = None
    summing_data = None

    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()

        if col_name == grouping_col_name:
            if metadata.Dtype() == DataType.DataType.Int:
                grouping_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == DataType.DataType.Float:
                grouping_data = [column.FloatValues(j) for j in range(column.FloatValuesLength())]
            elif metadata.Dtype() == DataType.DataType.String:
                grouping_data = [column.StringValues(j).decode() for j in range(column.StringValuesLength())]

        elif col_name == sum_col_name:
            if metadata.Dtype() == DataType.DataType.Int:
                summing_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == DataType.DataType.Float:
                summing_data = [column.FloatValues(j) for j in range(column.FloatValuesLength())]

        if grouping_data and summing_data:
            break

    if grouping_data is None or summing_data is None:
        raise ValueError("Grouping column or summing column not found")

    # Create a temporary DataFrame for performing the groupby sum
    temp_df = pd.DataFrame({
        grouping_col_name: grouping_data,
        sum_col_name: summing_data
    })

    result_df = temp_df.groupby(grouping_col_name).agg({sum_col_name: 'sum'})
    return result_df

def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    """
    Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.
    This function shouldn't do anything if col_name doesn't exist or the specified
    column is a string column.

    @param fb_buf: buffer containing bytes of the Flatbuffer Dataframe.
    @param col_name: name of the numeric column to apply map_func to.
    @param map_func: function to apply to elements in the numeric column.
    """
    dataframe = DataFrame.DataFrame.GetRootAs(fb_buf, 0)
    num_columns = dataframe.ColumnsLength()

    for i in range(num_columns):
        column = dataframe.Columns(i)
        metadata = column.Metadata()
        col_name_fb = metadata.Name().decode()

        if col_name_fb == col_name:
            dtype = metadata.Dtype()
            if dtype == DataFrame.DataType.Int:
                for j in range(column.IntValuesLength()):
                    value = column.IntValues(j)
                    new_value = map_func(value)
                    column.IntValues(j, new_value)
            elif dtype == DataFrame.DataType.Float:
                for j in range(column.FloatValuesLength()):
                    value = column.FloatValues(j)
                    new_value = map_func(value)
                    column.FloatValues(j, new_value)
            else:
                raise ValueError("Column is not numeric")