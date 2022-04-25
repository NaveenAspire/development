"""This is the test module for the move file sftp to s3"""

from datetime import datetime
import pytest
from move_file_sftp_to_s3 import MoveFileSftpToS3


class Test_MoveFile:
    """This class test for move file sftp to s3 module"""

    def test_move_file_sftp_to_s3_objects(self):
        """This Method will test for the instance belong to the class MoveFileSftpToS3"""
        self.obj = MoveFileSftpToS3()
        assert isinstance(self.obj, MoveFileSftpToS3)

    def test_get_key_name(self):
        """This test function will test if the key name correctly get"""
        self.obj = MoveFileSftpToS3()
        file_name = "Order data as 1.2.2022.csv"
        key = self.obj.get_key_name(file_name)
        print(key)
        assert key == "pt_year=2022/pt_month=02/pt_day=01/" + file_name

    @pytest.mark.xfail
    def test_get_key_name_fail(self):
        """This test function will test if the key name correctly get"""
        self.obj = MoveFileSftpToS3()
        file_name = "Order data as 1.2.222"
        key = self.obj.get_key_name(file_name)
        print(key)
        assert key is False

    def test_get_date_object(self):
        """This method will test if the date format correctly get"""
        self.obj = MoveFileSftpToS3()
        date_object = self.obj.get_date_object("1.02.22")
        print(type(date_object))
        assert isinstance(date_object, datetime)

    @pytest.mark.xfail
    def test_get_date_object_fail(self):
        """This method will test if the date format correctly get"""
        self.obj = MoveFileSftpToS3()
        date_object = self.obj.get_date_object("1.02.222")
        print(type(date_object))
        assert date_object == None

    def test_move_file_is_done(self):
        """This method will test weather the files moved sucessfully"""
        self.obj = MoveFileSftpToS3()
        status = self.obj.move_file()
        assert status == "Success"

    @pytest.mark.xfail
    def test_move_file_not_done(self):
        """This method will test weather move file failed"""
        self.obj = MoveFileSftpToS3()
        status = self.obj.move_file()
        assert status == "Failed"
