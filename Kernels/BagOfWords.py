from Kernels.Kernel import KernelBase
from sklearn.feature_extraction.text import HashingVectorizer


class BagOfWords(KernelBase):
    """
    Implementation of a SKLearn kernel for Natural Language processing
    """

    def __init__(self, filereader_instance):
        """
        Constructor
        :param lambda_decay: lambda parameter for the algorithm
        :type  lambda_decay: float
        :param subseq_length: maximal subsequence length
        :type subseq_length: int
        """
        self.vec = None
        KernelBase.__init__(filereader_instance)

    def _my_kernel(self, data, n_features):
        """

        :return:
        """
        self.vec = HashingVectorizer(stop_words='english', non_negative=True,
                                     n_features=n_features)
        return self.vec.transform(data)

