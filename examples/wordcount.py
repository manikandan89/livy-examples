from livy.client import HttpClient
import sys, os
from operator import add
from urlparse import urlparse


class WordCount(object):
    def __init__(self, uri):
        """
         Initializes the python client with the given uri
         :param uri: Livy server uri generated from urlparse lib
         :type uri: ParseResult generated from urlparse lib

         >>> from urlparse import urlparse
         >>> uri = urlparse("http://test")

        """

        self.client = HttpClient(uri, False)

    def upload_dependent_egg_files(self):
        """
         Uploads the egg files of the python-api and the app code
        """

        client_module = sys.modules[HttpClient.__module__]
        app_module = sys.modules[WordCount.__module__]
        self.__upload_file(client_module)
        self.__upload_file(app_module)

    def __upload_file(self, module_to_be_uploaded):
        egg_path = os.path.dirname(os.path.dirname(module_to_be_uploaded.__file__))
        upload_file_future = self.client.upload_pyfile(open(egg_path))
        upload_file_future.result()

    def process_streaming_word_count(self):
        """
         Submits a spark streaming job to the livy server

        The streaming job reads data from the given host and port. The data read
        is saved in json format as data frames in the given output path. For simplicity,
        the number of streaming batches are 2 with each batch for 20 seconds. The Timeout
        of the streaming job is set to 40 seconds
        """
        return self.client.submit(self.__do_process_streaming_word_count)

    def __do_process_streaming_word_count(self, context):
        context.create_streaming_ctx(20)
        ssc = context.streaming_ctx
        lines = ssc.socketTextStream('localhost', 8085)
        non_empty_lines = lines.filter(lambda line: line is None or line == "")
        counts = non_empty_lines.flatMap(lambda line: line.split(' ')).map(
            lambda word: (word, 1)).reduceByKey(add)
        counts.foreachRDD(lambda rdd: rdd.toDF(['word'])
                          .write.mode('append')
                          .json('/Users/manikandan.nagarajan/py_df'))
        ssc.start
        ssc.awaitTerminationOrTimeout(35)
        ssc.stop(False, True)

    def get_word_with_most_count(self):
        """
         Submits a spark sql job to the livy server

        The sql context job reads data frames from the given json path and executes
        a sql query to get the word with max count on the temp table created with data frames
        """
        return self.client(self.__do_get_word_with_most_count)

    def __do_get_word_with_most_count(self, context):
        sql_ctx = context.sql_ctx
        try:
            rdd = sql_ctx.read.json('/Users/manikandan.nagarajan/py_df')
            rdd.registerTempTable('words')
            result = sql_ctx.sql(
                'select word, count(word) as word_count from words group by '
                'word order by word_count desc limit 1')
            return result.first
        except:
            print("No data frames are present in the path to the sql context")
            raise


def main():
    word_count_app = WordCount(urlparse('http://172.21.0.228:8998'))
    word_count_app.upload_dependent_egg_files()
    handle1 = word_count_app.process_streaming_word_count()
    handle1.result(40)
    handle2 = word_count_app.get_word_with_most_count()
    print("result::", handle2.result(40))

if __name__ == '__main__':
    main()
