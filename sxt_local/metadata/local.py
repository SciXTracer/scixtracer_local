"""Implementation of the local storage plugin"""
import json
from pathlib import Path

from scixtracer.models import URI
from scixtracer.models import Dataset
from scixtracer.metadata import SxMetadata


class SxMetadataLocal(SxMetadata):
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
        Path(dataset_path / "metadata").mkdir(parents=True)

    @staticmethod
    def __make_new_uri(root: Path):
        """Build a local URI

        :param root: Directory of the array storage
        :return: The created URI
        """
        files = sorted(list(root.glob('*.json')))
        uuid = 1
        if len(files) > 0:
            last_id = int(str(files[-1].name).replace(".json", ""))
            uuid = last_id + 1
        return root / f"{str(uuid).zfill(9)}.json"

    def create(self,
               dataset: Dataset,
               content: dict[str, any] = None,
               ) -> URI:
        """Create a new data metadata

        :param dataset: Destination dataset,
        :param content: Metadata content
        :return: The new data URI
        """
        filename = self.__make_new_uri(self.__root /
                                       dataset.uri.value /
                                       "metadata")
        metadata_uri = str(filename).replace(str(self.__root), "")
        if content is None:
            content = {}
        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(content, json_file)
        return URI(value=metadata_uri)

    def write(self, uri: URI, content: dict[str, any]):
        """Write a data metadata into storage

        :param uri: Unique identifier of the data,
        :param content: Metadata to write
        """
        filename = str(Path(str(self.__root) + uri.value).resolve())
        with open(filename, "w", encoding='utf-8') as json_file:
            json.dump(content, json_file)

    def read(self, uri: URI) -> dict[str, any]:
        """Read a data metadata

        :param uri: Unique identifier of the data,
        :return: the read content
        """
        filename = str(Path(str(self.__root) + uri.value).resolve())
        with open(filename, "r", encoding='utf-8') as json_file:
            return json.load(json_file)

    def delete(self, uri: URI):
        """Delete a data

        :param uri: Unique identifier of the data,
        """
        filename = Path(str(Path(str(self.__root) + uri.value).resolve()))
        if filename.is_file():
            filename.unlink()
