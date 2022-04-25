"""This test module will test all methods in sftp_connection module"""
import pytest
from sftp_connection import SftpCon
import os


class Test_sftp:
    """This class test for sftp_connection module"""

    def test_sfpt_mock_conntion(self, sftp_client):
        sftp_client.put("opt/data/sample.csv", "sample.csv")
        print(sftp_client.listdir())
        assert sftp_client.listdir() == ["sample.csv"]

    def test_sftp_object(self):
        """This method test the instance belong to the class of SftpCon"""
        self.sftp_obj = SftpCon()
        assert isinstance(self.sftp_obj, SftpCon)

    def test_list_files(self):
        """This method test weather list_sftp_files return files or not"""
        sftp_obj = SftpCon()
        result = sftp_obj.list_files()
        assert type(result) == list

    @pytest.mark.xfail
    def test_list_files_failed(self):
        """This method test weather list_sftp_files return files or not"""
        sftp_obj = SftpCon()
        result = sftp_obj.list_files()
        assert result == None

    def test_read_file(self):
        """This method test weather sftp read file or not"""
        result = self.sftp_obj.read_file("sftp_file")
        assert type(result) == object

    @pytest.mark.xfail
    def test_read_file_is_failed(self):
        """This method test weather sftp read file or not"""
        result = self.sftp_obj.read_file("<unavilable file name")
        assert result == None

    def test_rename_file(self):
        """This method will test the if the name changed or not"""
        new_name = self.sftp_obj.rename_file("<available file name>")
        assert self.sftp_obj.conn.exists(new_name) == True

    @pytest.mark.xfail
    def test_rename_file_is_failed(self):
        """This method will test the if the name changed or not"""
        new_name = self.sftp_obj.rename_file("<unavailable file name>")
        assert self.sftp_obj.conn.exists(new_name) == False

    def test_download_file(self):
        """This method will test the when the file downloaded or not"""
        result = self.sftp_obj.download_file("<available file name>")
        assert os.path.exists(result) == True

    @pytest.mark.xfail
    def test_download_file_is_failed(self):
        """This method will test the when the file downloaded or not"""
        result = self.sftp_obj.download_file("<unavailable file name>")
        assert os.path.exists(result) == False
