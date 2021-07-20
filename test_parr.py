import unittest

from parr import PDataFrame

class PyDataFrameTest(unittest.TestCase):

    def test_import_novalue(self):
        pdf = PDataFrame()
        self.assertEqual(pdf.import_from_dir(), True)
    
    def test_import_src_w_path(self):
        pdf = PDataFrame("csv/")
        self.assertEqual(pdf.import_from_dir(), True)

    def test_import_src_wo_path(self):
        pdf = PDataFrame("csv")
        self.assertEqual(pdf.import_from_dir(), True)

    def test_import_only_dest(self):
        pdf = PDataFrame(dest_file="q")
        self.assertEqual(pdf.import_from_dir(), True)

    def test_read_from_parquet_with_ps_no_arg(self):
        pdf = PDataFrame()
        self.assertEqual(pdf.read_from_parquet_with_ps(), [15.8, '17-03-2016', 'Highland & Eilean Siar'])

    def test_read_from_parquet_with_ps_w_dir(self):
        pdf = PDataFrame("pq")
        self.assertEqual(pdf.read_from_parquet_with_ps(), [15.8, '17-03-2016', 'Highland & Eilean Siar'])
    
    def test_read_from_parquet_with_ps_w_correct_path(self):
        pdf = PDataFrame("pq/weather.parquet")
        self.assertEqual(pdf.read_from_parquet_with_ps(), [15.8, '17-03-2016', 'Highland & Eilean Siar'])

if __name__ == "__main__":
    unittest.main()