"""Implementation of the local storage plugin"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import zarr

import shutil

from scixtracer.models import URI
from scixtracer.models import Dataset
from scixtracer.models import StorageTypes
from scixtracer.storage import SxStorage


class SxStorageLocal(SxStorage):
    """Interface for storage interactions"""

    def __init__(self):
        self.__root = None

    def connect(self, workspace: str = None, **kwargs):
        self.__root = Path(workspace).resolve()

    def init_dataset(self, dataset: Dataset):
        """Initialize the storage for a new dataset

        :param dataset: Dataset information
        """
        dataset_path = self.__root / dataset.uri.value
        dataset_path.mkdir(exist_ok=True)
        Path(dataset_path / "data" / "array").mkdir(parents=True)
        Path(dataset_path / "data" / "table").mkdir(parents=True)
        with open(dataset_path / "data" / "value.json",
                  "w", encoding='utf-8') as json_file:
            json.dump({}, json_file)
        with open(dataset_path / "data" / "label.json",
                  "w", encoding='utf-8') as json_file:
            json.dump({}, json_file)

    @staticmethod
    def array_types() -> tuple | type:
        """Array data types that the plugin can store

        :return: The list of types
        """
        return np.ndarray

    @staticmethod
    def table_types() -> tuple | type:
        """Table data types that the plugin can store

        :return: The list of types
        """
        return pd.Series, pd.DataFrame

    @staticmethod
    def value_types() -> tuple | type:
        """Value data types that the plugin can store

        :return: The list of types
        """
        return float, int, bool

    @staticmethod
    def label_types() -> tuple | type:
        """Label data types that the plugin can store

        :return: The list of types
        """
        return str

    @staticmethod
    def __make_tensor_uri(root: Path):
        """Build a local URI

        :param root: Directory of the array storage
        :return: The created URI
        """
        files = sorted(list(root.glob('*.zarr')))
        uuid = 1
        if len(files) > 0:
            last_id = int(str(files[-1].name).replace(".zarr", ""))
            uuid = last_id + 1
        return root / f"{str(uuid).zfill(9)}.zarr"

    @staticmethod
    def __make_table_uri(root: Path):
        """Build a local URI

        :param root: Directory of the array storage
        :return: The created URI
        """
        files = sorted(list(root.glob('*.csv')))
        uuid = 1
        if len(files) > 0:
            last_id = int(str(files[-1].name).replace(".csv", ""))
            uuid = last_id + 1
        return root / f"{str(uuid).zfill(9)}.csv"

    def create_tensor(self,
                      dataset: Dataset,
                      array: np.ndarray = None,
                      shape: tuple[int, ...] = None
                      ) -> URI:
        """Create a new tensor

        :param dataset: Destination dataset,
        :param array: Data content,
        :param shape: Shape of the tensor,
        :return: The new data URI
        """
        filename = self.__make_tensor_uri(self.__root /
                                          dataset.uri.value /
                                          "data" / "array")
        tensor_uri = str(filename).replace(str(self.__root), "")
        if shape is not None and array is None:
            chunks = shape
            dtype = 'f4'
            zarr.open(str(filename), mode='w', shape=shape,
                      chunks=chunks, dtype=dtype)
        if array is not None and shape is None:
            z_array = zarr.open(str(filename), mode='w')
            z_array[:] = array
        return URI(value=tensor_uri)

    def write_tensor(self,
                     uri: URI,
                     array: np.ndarray
                     ):
        """Write new tensor data

        :param uri: Unique identifier of the data,
        :param array: Data content
        """
        filename = str(Path(str(self.__root) + str(uri.value)).resolve())
        z_array = zarr.open(filename, mode='w')
        z_array[:] = array

    def read_tensor(self,
                    uri: URI
                    ) -> np.ndarray:
        """Read a tensor from the dataset storage

        :param uri: Unique identifier of the data,
        :return: the read array
        """
        filename = str(Path(str(self.__root) + str(uri.value)).resolve())
        z_array = zarr.open(filename, mode='r')
        return np.array(z_array[:])

    def create_table(self, dataset: Dataset, table: pd.DataFrame):
        """Write table data into storage

        :param dataset: Destination dataset,
        :param table: Data table to write
        """
        filename = self.__make_table_uri(
            self.__root / dataset.uri.value / "data" / "table")

        if table is None:
            with open(str(filename), 'w') as fp:
                pass
        else:
            table.to_csv(filename)
        table_uri = URI(value=str(filename).replace(str(self.__root), ""))
        return table_uri

    def write_table(self, uri: URI, table: pd.DataFrame):
        """Write table data into storage

        :param uri: Unique identifier of the data,
        :param table: Data table to write
        """
        filename = str(Path(str(self.__root) + str(uri.value)).resolve())
        table.to_csv(filename)

    def read_table(self, uri: URI, ) -> pd.DataFrame:
        """Read a table from the dataset storage

        :param uri: Unique identifier of the data,
        :return: the read table
        """
        filename = str(Path(str(self.__root) + str(uri.value)).resolve())
        return pd.read_csv(filename)

    def create_value(self, dataset: Dataset, value: float) -> URI:
        """Write a value into storage

        :param dataset: Destination dataset,
        :param value: Value to write
        """
        filename = self.__root / dataset.uri.value / "data" / "value.json"
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)

        if len(data.keys()) > 0:
            keys = list(map(int, data.keys()))
            new_id = max(keys) + 1
        else:
            new_id = 1
        data[new_id] = value

        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file)

        uri_value = f"{str(filename)}.{new_id}".replace(str(self.__root), "")
        return URI(value=uri_value)

    def write_value(self, uri: URI, value: float):
        """Write a value into storage

        :param uri: Unique identifier of the data,
        :param value: Value to write
        """
        filename, uuid = uri.value.rsplit('.', 1)
        filename = str(Path(str(self.__root) + filename).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)

        data[str(uuid)] = value

        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file)

    def read_value(self, uri: URI) -> float:
        """Read a value from the dataset storage

        :param uri: Unique identifier of the data,
        :return: the read value
        """
        filename, uuid = uri.value.rsplit('.', 1)
        filename = str(Path(str(self.__root) + filename).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)
            return data[str(uuid)]

    def create_label(self, dataset: Dataset, value: str):
        """Write a value into storage

        :param dataset: Destination dataset,
        :param value: Value to write
        """
        filename = self.__root / dataset.uri.value / "data" / "label.json"
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)

        if len(data.keys()) > 0:
            keys = list(map(int, data.keys()))
            new_id = max(keys) + 1
        else:
            new_id = 1
        data[new_id] = value

        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file)

        uri_value = f"{str(filename)}.{new_id}".replace(str(self.__root), "")
        return URI(value=uri_value)

    def write_label(self, uri: URI, value: str):
        """Write a label into storage

        :param uri: Unique identifier of the data,
        :param value: Value to write
        """
        filename, uuid = uri.value.rsplit('.', 1)
        filename = str(Path(str(self.__root) + filename).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)

        data[str(uuid)] = value

        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file)

    def read_label(self, uri: URI) -> str:
        """Read a label from the dataset storage

        :param uri: Unique identifier of the data,
        :return: the read value
        """
        filename, uuid = uri.value.rsplit('.', 1)
        filename = str(Path(str(self.__root) + filename).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)
            return data[str(uuid)]

    def delete(self, storage_type: StorageTypes, uri: URI):
        """Delete a data

        :param storage_type: Data storage type
        :param uri: Unique identifier of the data,
        """
        if storage_type == StorageTypes.ARRAY \
                or storage_type == StorageTypes.TABLE:
            filename = str(Path(str(self.__root) + str(uri.value)).resolve())
            if Path(filename).is_dir():
                shutil.rmtree(filename)
            elif Path(filename).is_file():
                Path(filename).unlink()
        else:
            self.__remove_item(uri)

    def __remove_item(self, uri: URI):
        """Remove an item from a json file

        :param uri: Unique identifier of the file and item
        """
        filename, uuid = uri.value.rsplit('.', 1)
        filename = str(Path(str(self.__root) + filename).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            data = json.load(json_file)

        data.pop(str(uuid), None)

        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(data, json_file)
