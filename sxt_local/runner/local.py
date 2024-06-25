from scixtracer.models import DataInfo
from scixtracer.models import Batch
from scixtracer.runner import SxRunner


class SxRunnerLocal(SxRunner):
    """Interface for storage interactions"""
    def connect(self, **kwargs):
        """Initialize any needed connection to the database"""

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
                print("run:", item.func.__name__, ", ", item.inputs)
                args = self.__load_inputs(item.inputs)
                outputs = item.func(*args)
                if isinstance(outputs, list) or isinstance(outputs, tuple):
                    for i, value in enumerate(outputs):
                        self.storage.write_data(item.outputs[i], value)
                else:
                    self.storage.write_data(item.outputs[0], outputs)
