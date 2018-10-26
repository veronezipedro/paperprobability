from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from cStringIO import StringIO
from nltk import word_tokenize
import nltk


class FileReader(object):
    """
    Class responsible to read the pdf files and feed the kernel functions for further transformations
    """

    def __init__(self, caching=True, max_pages=0, codec='utf-8'):
        """
        Class constructor
        :param file_path:
        """
        self.__codec = codec
        self.__maxpages = max_pages
        self.__caching = caching
        self.raw = None
        self.tokens = None
        self.text = None
        self.raw_flag = False
        self.tokenized_flag = False
        self.vocab_flag = False
        self.n_pages = 0

    def read_file_to_raw_str(self, file_path):
        resource_manager = PDFResourceManager()
        retstr = StringIO()
        params = LAParams()
        device = TextConverter(resource_manager, retstr, codec=self.__codec, laparams=params)
        fp = file(file_path, 'rb')
        interpreter = PDFPageInterpreter(resource_manager, device)
        pagenos = set()
        n_pages = 0
        for page in PDFPage.get_pages(fp, pagenos, maxpages=self.__maxpages,
                                      caching=self.__caching, check_extractable=True):
            interpreter.process_page(page)
            n_pages += 1
        self.n_pages = n_pages
        raw = retstr.getvalue()
        fp.close()
        device.close()
        retstr.close()
        return raw, n_pages

    @staticmethod
    def tokenize(raw_text):
        """

        :return:
        """
        tokens = word_tokenize(raw_text)
        return tokens

    @staticmethod
    def get_vocab(raw_text=None, tokenized_text=None):
        """

        :return:
        """
        if raw_text and tokenized_text is None:
            print 'Should insert at least one of the values'
            raise ValueError
        elif raw_text is None:
            text = nltk.Text(tokenized_text)
            text = [w.lower() for w in text]
            return text
        elif tokenized_text is None:
            tokens = word_tokenize(raw_text)
            text = nltk.Text(tokens)
            text = [w.lower() for w in text]
            return text

    @staticmethod
    def get_doc_object_from_file(file_path):
        """

        :param file_path:
        :return:
        """
        fp = file(file_path, 'rb')
        parser = PDFParser(fp)
        doc = PDFDocument(parser)
        fp.close()
        return doc
