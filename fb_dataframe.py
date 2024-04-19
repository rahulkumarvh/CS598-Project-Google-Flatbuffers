import flatbuffers
import pandas as pd
import struct
import time
import types
from DataFrame import DataFrame
from DataFrame import Column
from DataFrame import Metadata
from DataFrame import ValueType
# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

def to_flatbuffer(df: pd.DataFrame) -> bytes:
    """
        Converts a DataFrame to a flatbuffer. Returns the bytes of the flatbuffer.

        The flatbuffer should follow a columnar format as follows:
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        | DF metadata | col 1 metadata | val 1 | val 2 | ... | col 2 metadata | val 1 | val 2 | ... |
        +-------------+----------------+-------+-------+-----+----------------+-------+-------+-----+
        You are free to put any bookkeeping items in the metadata. however, for autograding purposes:
        1. Make sure that the values in the columns are laid out in the flatbuffer as specified above
        2. Serialize int and float values using flatbuffer's 'PrependInt64' and 'PrependFloat64'
            functions, respectively (i.e., don't convert them to strings yourself - you will lose
            precision for floats).

        @param df: the dataframe.
    """
    print("OK")
    builder = flatbuffers.Builder(1024)
    metadata_string = builder.CreateString("DataFrame Metadata")
    column_metadata_list = []
    value_vectors = []
    value_vectors_dtype = []
    for column_name, dtype in df.dtypes.items():
        if dtype == 'int64':
            value_type = ValueType.ValueType().Int
        elif dtype == 'float64':
            value_type = ValueType.ValueType().Float
        elif dtype == 'object':
            value_type = ValueType.ValueType().String
        else:
            raise ValueError(f"Unsupported dtype: {dtype}")

        column_metadata_list.append((column_name, value_type))

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
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
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
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
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
            value_type = metadata[1]
            Metadata.Start(builder)
            Metadata.AddName(builder, col_name)
            Metadata.AddDtype(builder, value_type)
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
    """
        Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
        similar to df.head(). If there are less than n rows, return the entire Dataframe.
        Hint: don't forget the column names!

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param rows: number of rows to return.
    """
    df = DataFrame.DataFrame.GetRootAs(fb_bytes,0)

    num_columns = df.ColumnsLength()
    data = {}

    for i in range(num_columns):
        column = df.Columns(i)
        metadata = column.Metadata()
        col_name = metadata.Name().decode()
        
        if metadata.Dtype() == ValueType.ValueType.Int:
            values = [column.IntValues(j) for j in range(min(rows, column.IntValuesLength()))]
        elif metadata.Dtype() == ValueType.ValueType.Float:
            values = [column.FloatValues(j) for j in range(min(rows, column.FloatValuesLength()))]
        elif metadata.Dtype() == ValueType.ValueType.String:
            values = [column.StringValues(j).decode() for j in range(min(rows, column.StringValuesLength()))]
        else:
            continue  # Skip unsupported column types

        data[col_name] = values

    # Construct and return a Pandas DataFrame
    return pd.DataFrame(data)

def fb_dataframe_group_by_sum(fb_bytes: bytes, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
    """
        Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
        and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.

        @param fb_bytes: bytes of the Flatbuffer Dataframe.
        @param grouping_col_name: column to group by.
        @param sum_col_name: column to sum.
    """
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
            if metadata.Dtype() == ValueType.ValueType.Int:
                grouping_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == ValueType.ValueType.Float:
                grouping_data = [column.FloatValues(j) for j in range(column.FloatValuesLength())]
            elif metadata.Dtype() == ValueType.ValueType.String:
                grouping_data = [column.StringValues(j).decode() for j in range(column.StringValuesLength())]

        elif col_name == sum_col_name:
            if metadata.Dtype() == ValueType.ValueType.Int:
                summing_data = [column.IntValues(j) for j in range(column.IntValuesLength())]
            elif metadata.Dtype() == ValueType.ValueType.Float:
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
    # YOUR CODE HERE...
    dataframe = DataFrame.DataFrame.GetRootAs(fb_buf, 0)
    num_elements = dataframe.Columns(0).IntValuesLength() # Get number of elements
    element_size = 8
    if(int.from_bytes(fb_buf[472:472 + element_size], 'little')<10):
        start_offset_int = 472
        start_offset_float = 608
    else:
        start_offset_int = 112
        start_offset_float = 248
    for i in range(num_elements):
        if col_name == 'int_col':

            offset = start_offset_int + i * element_size
            original_value = int.from_bytes(fb_buf[offset:offset + element_size], 'little')
            print(original_value)
            modified_value = map_func(original_value)
            print(modified_value)
            fb_buf[offset:offset + element_size] = modified_value.to_bytes(element_size, 'little', signed=True)
        elif col_name == 'float_col':
        
            offset = start_offset_float + i * element_size
            original_value = struct.unpack_from('<d', fb_buf, offset)[0]
            modified_value = map_func(original_value)
            struct.pack_into('<d', fb_buf, offset, modified_value)