from abc import abstractmethod, ABCMeta
from Kernels.Kernel import KernelBase
__author__ = "Pedro Veronezi"


class PredictorWrapper(object):
    """
    Class used as a wrapper for interfacing the framework with the libraries, for example SVM
    """

    __metaclass__ = ABCMeta

    def __init__(self, kernel_instance):
        """
        Class constructor
        """
        if isinstance(kernel_instance, KernelBase):
            self.kernel_function = KernelBase.my_kernel
        else:
            print "kernel_instance must be an instance of KernelBase"
            raise ValueError
        self.trained_flag = False
        self.predictor_instance = None
        self.params = None
        self.kernel = 'precomputed'

    def fit(self, x, y):
        """

        :param X:
        :param Y:
        :return:
        """

        return self._fit(x, y)

    @abstractmethod
    def _fit(self, x, y):
        """

        :param X:
        :param Y:
        :return:
        """
        pass

    def predict(self, x, y):
        """

        :param X:
        :param Y:
        :return:
        """
        return self._predict(x, y)

    @abstractmethod
    def _predict(self, x, y):
        """

        :param X:
        :param Y:
        :return:
        """
        pass
