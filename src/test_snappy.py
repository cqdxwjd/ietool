from unittest import TestCase
import snappy.hadoop_snappy as snappy


class SnappyCompressionTest(TestCase):
    def test_simple_compress(self):
        text = 'hello hello world!'.encode('utf-8')
        compressor = snappy.StreamCompressor()
        compressed = compressor.compress(text)
        print compressed

    def test_file_compress(self):
        with open('../data/test3.dat', 'r') as input, open('snappy_file', 'w') as output:
            data = input.read()
            compressed = snappy.StreamCompressor().compress(data)
            output.write(compressed)
            input.close()
            output.close()
