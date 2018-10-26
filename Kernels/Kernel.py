from abc import abstractmethod, ABCMeta
__author__ = "Pedro Veronezi"


class KernelBase(object):
    """
    Base class for feature generation using kernels, specific for kernels applied to natural language processing
    """

    __metaclass__ = ABCMeta

    def __init__(self, file_reader_instance):
        self.f = file_reader_instance

    def my_kernel(self, x1, x2):
        return self._my_kernel(x1, x2)

    @abstractmethod
    def _my_kernel(self, x1, x2):
        pass
