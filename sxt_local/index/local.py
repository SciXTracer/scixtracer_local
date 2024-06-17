"""Implementation of the local index plugin"""
from pathlib import Path
import json
from sqlite3 import connect, Connection

import pandas as pd

from scixtracer.models import URI
from scixtracer.models import Dataset
from scixtracer.models import Location
from scixtracer.models import DataInfo
from scixtracer.index import SxIndex

from .queries import init_database
from .queries import insert_location
from .queries import insert_location_annotation
from .queries import insert_data_annotation
from .queries import insert_data
from .queries import query_locations
from .queries import query_data_annotations
from .queries import query_locations_annotations
from .queries import query_data_with_annotations
from .queries import query_data_with_locations
from .queries import query_view_locations
from .queries import query_view_data


class SxIndexLocal(SxIndex):
    """SciXTracer local using sqlite"""

    def __init__(self):
        self.__workspace = None
        self.__conn: dict[str, Connection] = {}

    def __del__(self):
        for conn in self.__conn.values():
            if isinstance(conn, Connection):
                conn.close()

    def connect(self, workspace: str = None, **kwargs):
        """Initialize the database connection

        :param workspace: Path to the dataset workspace
        """
        self.__workspace = Path(workspace).resolve()
        if not self.__workspace.exists():
            self.__workspace.mkdir(parents=True, exist_ok=True)

    def datasets(self) -> pd.DataFrame:
        """Get the list of available datasets

        :return: The info of available dataset in the workspace
        """
        raise NotImplementedError()

    def __get_connection(self, uri: URI) -> Connection:
        """Get the connection to the dataset index

        :param uri: Unique identifier of the dataset
        :return: The connection to the database
        """
        db_file = self.__workspace / uri.value / "index.db"
        if uri.value in self.__conn:
            return self.__conn[uri.value]
        conn = connect(db_file)
        self.__conn[uri.value] = conn
        return conn

    def new_dataset(self, name: str) -> Dataset:
        """Create a new dataset

        :param name: Title of the dataset
        """
        # create the folder
        dataset_uri = name.replace(" ", "_").lower()
        dataset_path = self.__workspace / dataset_uri
        if dataset_path.exists():
            raise ValueError("A dataset with the same name already exists")
        dataset_path.mkdir(parents=True, exist_ok=False)
        # create name
        name_file = dataset_path / "info.json"
        with open(name_file, "w", encoding="utf-8") as fp:
            json.dump({"name": name}, fp)
        # Init the database
        conn = self.__get_connection(URI(value=dataset_uri))
        init_database(conn)
        self.__conn[dataset_path] = conn
        return Dataset(name=name, uri=URI(value=dataset_uri))

    def get_dataset(self, uri: URI) -> Dataset:
        """Read the information of a dataset

        :param uri: Unique identifier of the dataset
        """
        name_file = self.__workspace / uri.value / "info.json"
        with open(name_file, "r", encoding="utf-8") as fp:
            d = json.load(fp)
            name = d["name"]
        return Dataset(name=name, uri=uri)

    def new_location(self,
                     dataset: Dataset,
                     annotations: dict[str, str | int | float | bool] = None
                     ) -> Location:
        """Create a new data location in the dataset

        :param dataset: Dataset to be edited,
        :param annotations: Annotations associated to the location,
        :return: The newly created location
        """
        conn = self.__get_connection(dataset.uri)
        uuid = insert_location(conn)
        location = Location(dataset=dataset, uuid=uuid)
        if annotations is not None:
            for key, value in annotations.items():
                self.annotate_location(location, key, value)
        return location

    def annotate_location(self,
                          location: Location,
                          key: str,
                          value: str | int | float | bool):
        """Annotate a location with akey value pair

        :param location: Location to annotate,
        :param key: Annotation key,
        :param value: Annotation value
        """
        conn = self.__get_connection(location.dataset.uri)
        insert_location_annotation(conn, location.uuid, key, value)

    def annotate_data(self,
                      data_info: DataInfo,
                      key: str,
                      value: str | int | float | bool):
        """Annotate a data

        :param data_info: Information of the data,
        :param key: Annotation key,
        :param value: Annotation value
        """
        conn = self.__get_connection(data_info.dataset.uri)
        insert_data_annotation(conn, data_info.uri, key, value)

    def create_data(self,
                    location: Dataset | Location,
                    uri: URI,
                    storage_type: str,
                    annotations: dict[str, any] = None
                    ) -> DataInfo:
        """Create new data to a location

        :param location: Location where to save the data,
        :param uri: URI of the data,
        :param storage_type: Format of the data,
        :param annotations: Annotations of the data with key value pairs,
        :return: The data information
        """
        if isinstance(location, Dataset):
            conn = self.__get_connection(location.uri)
            location_id = insert_location(conn)
            data_location = Location(dataset=location, uuid=location_id)
        else:
            conn = self.__get_connection(location.dataset.uri)
            location_id = location.uuid
            data_location = location

        insert_data(conn, location_id, uri.value, storage_type)
        for key, value in annotations.items():
            insert_data_annotation(conn, uri.value, key, value)
        return DataInfo(location=data_location,
                        storage_type=storage_type,
                        uri=uri)

    def query_data(self,
                   dataset: Dataset, *,
                   annotations: dict[str, any] = None,
                   locations: list[Location] = None
                   ) -> list[DataInfo]:
        """Retrieve data from a dataset

        :param dataset: Dataset to query,
        :param annotations: Query data that have the annotations,
        :param locations: Data at these locations
        """
        conn = self.__get_connection(dataset.uri)
        if annotations is not None and locations is None:
            data = query_data_with_annotations(conn, annotations)
        elif locations is not None and annotations is None:
            data = query_data_with_locations(conn, locations)
        else:
            raise ValueError('query_data cannot combine annotation and '
                             'location query')
        out_data = []
        for dat in data:
            out_data.append(DataInfo(uri=URI(value=dat[1]),
                                     storage_type=dat[2],
                                     location=Location(uuid=dat[0],
                                                       dataset=dataset)))
        return out_data

    def query_location(self,
                       dataset: Dataset,
                       annotations: dict[str, any] = None,
                       ) -> list[Location]:
        """Retrieve locations from a dataset

        :param dataset: Dataset to query,
        :param annotations: query locations that have the annotations,
        :return: Locations that correspond to the query
        """
        conn = self.__get_connection(dataset.uri)
        loc_s = query_locations(conn, annotations)
        locations = []
        for loc in loc_s:
            locations.append(Location(uuid=loc[0], dataset=dataset))
        return locations

    def data_annotations(self, dataset: Dataset) -> dict[str, list[any]]:
        """Get all the data annotations in the datasets with their values

        :param dataset: Dataset to query,
        :return: Available annotations with their values
        """
        conn = self.__get_connection(dataset.uri)
        return query_data_annotations(conn)

    def location_annotations(self, dataset: Dataset) -> dict[str, list[any]]:
        """Get all the location annotations in the datasets with their values

        :param dataset: Dataset to be queried,
        :return: Available locations with their values
        """
        conn = self.__get_connection(dataset.uri)
        return query_locations_annotations(conn)

    def view_locations(self, dataset: Dataset) -> pd.DataFrame:
        """Create a table to visualize the dataset locations structure

        :param dataset: Dataset to visualize
        :return: The data view as a table
        """
        conn = self.__get_connection(dataset.uri)
        results = query_view_locations(conn)
        return results

    def view_data(self,
                  dataset: Dataset,
                  locations: list[Location] = None,
                  ) -> pd.DataFrame:
        """Create a table to visualize the dataset data structure

        :param dataset: Dataset to visualize
        :param locations: Locations to filter
        :return: The data view as a table
        """
        conn = self.__get_connection(dataset.uri)
        loc_ids = []
        if locations is not None:
            loc_ids = [loc.id for loc in locations]
        results = query_view_data(conn, loc_ids)
        return results