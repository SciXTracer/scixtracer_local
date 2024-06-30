"""Local implementation of the SciXTracer runner """
from joblib import Parallel, delayed

from scixtracer.models import DataInfo
from scixtracer.models import BatchItem
from scixtracer.models import Batch
from scixtracer.runner import SxRunner


class SxRunnerLocal(SxRunner):
    def __init__(self):
        self.__items = None
    """Interface for storage interactions"""
    def connect(self, **kwargs):
        """Initialize any needed connection to the database"""

    def run_batch_item(self, item: BatchItem):
        print("run:", item.func.__name__, ", ", item.inputs)
        args = self.__load_inputs(item.inputs)
        outputs = item.func(*args)
        if isinstance(outputs, (list, tuple)):
            for i, value in enumerate(outputs):
                self.storage.write_data(item.outputs[i], value)
        else:
            self.storage.write_data(item.outputs[0], outputs)

    def __load_inputs(self, inputs: list):
        args_values = []
        for input_ in inputs:
            if isinstance(input_, list):
                args_values.append(self.__load_inputs(input_))
            elif isinstance(input_, DataInfo):
                args_values.append(self.storage.read_data(input_))
            else:
                args_values.append(input_)
        return args_values

    def run(self, batches: list[Batch]):
        """Execute a run defined as a list of batch

        :param batches: List of batches to run
        """
        for batch in batches:
            for item in batch.items:
                self.run_batch_item(item)

