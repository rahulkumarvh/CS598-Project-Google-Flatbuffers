import flatbuffers
import pandas as pd
import struct
import types
from CS598 import DataFrame
from CS598 import Column
from CS598 import Meta
from CS598 import DataType

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    builder = flatbuffers.Builder(1024)
    metadata_string = builder.CreateString("DataFrame Metadata")
    column_metadata_list = []
    value_vectors = []
    value_vectors_dtype = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'int64':
            value_type = DataType.DataType().Int
        elif dtype == 'float64':
            value_type = DataType.DataType().Float
        elif dtype == 'object':
            value_type = DataType.DataType().String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        column_metadata_list.append((column_name, value_type))

        # Convert column values to FlatBuffer values
        column_values = df[column_name]
        value_vectors.append(column_values.tolist())
        value_vectors_dtype.append(dtype)

    columns = []
    for dtype, metadata, value_vector in reversed(list(zip(value_vectors_dtype, column_metadata_list, value_vectors))):
        if dtype == 'int64':
            Column.StartIntValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependInt64(value)
            values = builder.EndVector(len(value_vector))

        elif dtype == 'float64':
            Column.StartFloatValuesVector(builder, len(value_vector))
            for value in reversed(value_vector):
                builder.PrependFloat64(value)
            values = builder.EndVector(len(value_vector))

        elif dtype == 'object':
            str_offsets = [builder.CreateString(str(value)) for value in value_vector]
            Column.StartStringValuesVector(builder, len(value_vector))
            for offset in reversed(str_offsets):
                builder.PrependUOffsetTRelative(offset)
            values = builder.EndVector(len(value_vector))

        col_name = builder.CreateString(metadata[0])
        value_type = metadata[1]
        Meta.MetaStart(builder)
        Meta.MetaAddName(builder, col_name)
        Meta.MetaAddDtype(builder, value_type)
        meta = Meta.MetaEnd(builder)
        Column.ColumnStart(builder)            
        Column.ColumnAddMetadata(builder, meta)
        Column.ColumnAddIntValues(builder, values)  # Int values here. Change as needed.
        columns.append(Column.ColumnEnd(builder))

    DataFrame.DataFrameStartColumnsVector(builder, len(columns))
    for column in columns:
        builder.PrependUOffsetTRelative(column)
    columns_vector = builder.EndVector(len(columns))

    DataFrame.DataFrameStart(builder)
    DataFrame.DataFrameAddMetadata(builder, metadata_string)
    DataFrame.DataFrameAddColumns(builder, columns_vector)
    df_data = DataFrame.DataFrameEnd(builder)

    builder.Finish(df_data)
    return builder.Output()

def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
    """
        Returns the first n rows of the Flatbuffer DataFrame as a Pandas DataFrame
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer DataFrame.
        @param rows: number of rows to return.
    """
    df = CS598.DataFrame.GetRootAs(fb_bytes, 0)
    num_columns = df.ColumnsLength()
    data = {}

    # Extract data for each column
    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()
        dtype = metadata.Dtype()

        values = []

        # Extract values based on data type
        if dtype == CS598.DataType.Int:
            values = [column.IntValues(j) for j in range(min(rows, column.IntValuesLength()))]
        elif dtype == CS598.DataType.Float:
            values = [column.FloatValues(j) for j in range(min(rows, column.FloatValuesLength()))]
        elif dtype == CS598.DataType.String:
            values = [column.StringValues(j).decode() for j in range(min(rows, column.StringValuesLength()))]

        # Add values to the data dictionary
        data[col_name] = values

    # Construct and return a Pandas DataFrame
    return pd.DataFrame(data)

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    df = DataFrame.GetRootAs(fb_bytes, 0)
    num_columns = df.ColumnsLength()
    data = {}

    grouping_data = None
    summing_data = None

    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()

        if col_name == grouping_col_name:
            if metadata.Dtype() == DataType.Int:
                grouping_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == DataType.Float:
                grouping_data = [column.FloatValues(j) for j in range(column.FloatValuesLength())]
            elif metadata.Dtype() == DataType.String:
                grouping_data = [column.StringValues(j).decode() for j in range(column.StringValuesLength())]

        elif col_name == sum_col_name:
            if metadata.Dtype() == DataType.Int:
                summing_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == DataType.Float:
                summing_data = [column.FloatValues(j) for j in range(column.FloatValuesLength())]

        if grouping_data and summing_data:
            break

    if grouping_data is None or summing_data is None:
        raise ValueError("Grouping column or summing column not found")

    temp_df = pd.DataFrame({
        grouping_col_name: grouping_data,
        sum_col_name: summing_data
    })

    result_df = temp_df.groupby(grouping_col_name).agg({sum_col_name: 'sum'})
    return result_df

def fb_dataframe_map_numeric_column(fb_buf: memoryview, col_name: str, map_func: types.FunctionType) -> None:
    dataframe = DataFrame.GetRootAs(fb_buf, 0)
    num_elements = dataframe.Columns(0).IntValuesLength()  # Assuming all columns have same length
    element_size = 8  # Size of int64 or float64

    start_offset = 0
    for i in range(dataframe.ColumnsLength()):
        column = dataframe.Columns(i)
        metadata = column.Metadata()
        col_name_fb = metadata.Name().decode()
        dtype = metadata.Dtype()

        if col_name_fb == col_name:
            if dtype == DataType.Int:
                start_offset = dataframe.Columns(i).IntValues(0).value  # Int values
            elif dtype == DataType.Float:
                start_offset = dataframe.Columns(i).FloatValues(0).value  # Float values
            else:
                return  # Not a numeric column, do nothing

    if start_offset == 0:
        return  # Column not found or not numeric

    for i in range(num_elements):
        offset = start_offset + i * element_size
        if dtype == DataType.Int:
            original_value = int.from_bytes(fb_buf[offset:offset + element_size], 'little', signed=True)
        elif dtype == DataType.Float:
            original_value = struct.unpack_from('<d', fb_buf, offset)[0]

        modified_value = map_func(original_value)

        if dtype == DataType.Int:
            fb_buf[offset:offset + element_size] = modified_value.to_bytes(element_size, 'little', signed=True)
        elif dtype == DataType.Float:
            struct.pack_into('<d', fb_buf, offset, modified_value)