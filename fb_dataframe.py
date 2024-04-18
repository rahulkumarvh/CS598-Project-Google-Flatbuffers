import flatbuffers
import pandas as pd
import struct
import time
import types
import numpy as np
# Your Flatbuffer imports here (i.e. the files generated from running ./flatc with your Flatbuffer definition)...

from Dataframe import (  # Assuming this is the generated filename
    Dataframe as RootTable,  # Update with the actual class name for Dataframe
    ColumnMetadata,
    DataType,
    Column,
)

def to_flatbuffer(df: pd.DataFrame) -> bytearray:
  """
  Converts a Pandas DataFrame to a Flatbuffer. Returns the bytearray of the Flatbuffer.

  @param df: the dataframe.
  """
  builder = flatbuffers.Builder()

  # Create ColumnMetadata and Column data based on data types
  column_metadata = []
  columns = []
  for col_name, col_data in df.items():
    data_type = DataType.String if pd.api.types.is_string_dtype(col_data) else DataType.Int64
    column_metadata.append(ColumnMetadata.CreateColumnMetadata(builder, col_name, data_type))

    if data_type == DataType.Int64:
      columns.append(Column.CreateColumn(builder, dtype=data_type, int64_data=col_data.tolist()))
    elif data_type == DataType.Float:
      columns.append(Column.CreateColumn(builder, dtype=data_type, float_data=col_data.tolist()))
    else:
      columns.append(Column.CreateColumn(builder, dtype=data_type, string_data=[str(val) for val in col_data.tolist()]))

  # Create Dataframe object
  dataframe = RootTable.CreateDataframe(builder, metadata=builder.CreateVector(column_metadata), columns=builder.CreateVector(columns))

  return builder.Output()


def fb_dataframe_head(fb_bytes: bytes, rows: int = 5) -> pd.DataFrame:
  """
  Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
  similar to df.head(). If there are less than n rows, return the entire Dataframe.

  @param fb_bytes: bytes of the Flatbuffer Dataframe.
  @param rows: number of rows to return.
  """
  df = RootTable.GetRootAsDataframe(fb_bytes)  # Update with the actual class name
  num_rows = min(rows, df.rows())

  # Get column names and data
  column_names = [m.name() for m in df.metadata()]
  column_data = []
  for i in range(num_rows):
    row = []
    for col in df.columns():
      if col.dtype() == DataType.Int64:
        row.append(col.int64_data()[i])
      elif col.dtype() == DataType.Float:
        row.append(col.float_data()[i])
      else:
        row.append(col.string_data()[i])
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
    