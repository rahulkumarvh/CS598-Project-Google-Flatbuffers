import dill
import hashlib
import pandas as pd
import types
import json


from multiprocessing import shared_memory

from fb_dataframe import to_flatbuffer, fb_dataframe_head, fb_dataframe_group_by_sum, fb_dataframe_map_numeric_column


class FbSharedMemory:
    """
        Class for managing the shared memory for holding flatbuffer dataframes.
    """
    def __init__(self):
        try:
            self.df_shared_memory = shared_memory.SharedMemory(name = "CS598")
            self.offsets_mem = shared_memory.SharedMemory(name="CS598_offsets")
            self.offsets = self.load_offsets()
        except FileNotFoundError:
            # Shared memory is not created yet, create it with size 200M.
            self.df_shared_memory = shared_memory.SharedMemory(name = "CS598", create=True, size=200000000)
            self.offsets_mem = shared_memory.SharedMemory(name="CS598_offsets", create=True, size=4096)
            self.offsets = {}

            # Add more initialization steps if needed here...

        # Add other class members you need here...
        self.current_offset = max((end for start, end in self.offsets.values()), default=0)

    def save_offsets(self):
        # Serialize and save the offsets dictionary to shared memory
        offsets_data = json.dumps(self.offsets).encode('utf-8')
        # Clear buffer before saving new data
        self.offsets_mem.buf[:len(offsets_data)] = offsets_data
        self.offsets_mem.buf[len(offsets_data):] = b'\x00' * (self.offsets_mem.size - len(offsets_data))

    def load_offsets(self):
        # Read until the first null byte, assuming null byte indicates end of valid JSON data
        raw_data = bytes(self.offsets_mem.buf)
        end_of_json = raw_data.find(b'\x00')
        valid_json = raw_data[:end_of_json].decode('utf-8')
        return json.loads(valid_json)

    def add_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """
            Adds a dataframe into the shared memory. Does nothing if a dataframe with 'name' already exists.

            @param name: name of the dataframe.
            @param df: the dataframe to add to shared memory.
        """
        if name in self.offsets:
            return

        fb_bytes = to_flatbuffer(df)
        if self.current_offset + len(fb_bytes) > self.df_shared_memory.size:
            raise MemoryError("Not enough space in shared memory")

        self.df_shared_memory.buf[self.current_offset:self.current_offset + len(fb_bytes)] = fb_bytes
        self.offsets[name] = (self.current_offset, self.current_offset + len(fb_bytes))
        self.current_offset += len(fb_bytes)

        self.save_offsets() 


    def _get_fb_buf(self, df_name: str) -> memoryview:
        """
            Returns the section of the buffer corresponding to the dataframe with df_name.
            Hint: get buffer section (fb_buf) holding the flatbuffer from shared memory.

            @param df_name: name of the Dataframe.
        """
        if df_name not in self.offsets:
            raise ValueError("Dataframe not found in shared memory")
        start, end = self.offsets[df_name]
        return memoryview(self.df_shared_memory.buf)[start:end]


    def dataframe_head(self, df_name: str, rows: int = 5) -> pd.DataFrame:
        """
            Returns the first n rows of the Flatbuffer Dataframe as a Pandas Dataframe
            similar to df.head(). If there are less than n rows, returns the entire Dataframe.

            @param df_name: name of the Dataframe.
            @param rows: number of rows to return.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_head(fb_bytes, rows)

    def dataframe_group_by_sum(self, df_name: str, grouping_col_name: str, sum_col_name: str) -> pd.DataFrame:
        """
            Applies GROUP BY SUM operation on the flatbuffer dataframe grouping by grouping_col_name
            and summing sum_col_name. Returns the aggregate result as a Pandas dataframe.
    
            @param df_name: name of the Dataframe.
            @param grouping_col_name: column to group by.
            @param sum_col_name: column to sum.
        """
        fb_bytes = bytes(self._get_fb_buf(df_name))
        return fb_dataframe_group_by_sum(fb_bytes, grouping_col_name, sum_col_name)

    def dataframe_map_numeric_column(self, df_name: str, col_name: str, map_func: types.FunctionType) -> None:
        """
            Apply map_func to elements in a numeric column in the Flatbuffer Dataframe in place.

            @param df_name: name of the Dataframe.
            @param col_name: name of the numeric column to apply map_func to.
            @param map_func: function to apply to elements in the numeric column.
        """
        fb_dataframe_map_numeric_column(self._get_fb_buf(df_name), col_name, map_func)

    def close(self) -> None:
        """
            Closes the managed shared memory.
        """
        try:
            self.df_shared_memory.close()
            self.offsets_mem.close()
        except:
            pass