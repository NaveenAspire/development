"""This is the test module for test  the fetch the nobel prizes and laureates year wise.
And upload the information as json file with partition based on aeard year"""
import ast
import configparser
from datetime import date
import os
import logging
import pytest
import pandas as pd
import requests
from dummy_S3.dummy_s3 import DummyS3

from nobelprize_api import NobelPrizeApi
from S3.s3 import S3Service
from fetch_nobelprize_and_laureates import (
    NobelprizeLaureates,
    get_partition,
    parent_dir,
)

log_dir = os.path.join(parent_dir, "opt/logging/fetch_nobel_prizes_laureates/")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "fetch_nobel_prizes_laureates.log")
logging.basicConfig(
    filename=log_file,
    datefmt="%d-%b-%y %H:%M:%S",
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    force=True,
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = configparser.ConfigParser()
config.read(parent_dir + "/develop.ini")


@pytest.fixture
def year():
    today_date = date.today()
    year = (
        today_date.year
        if today_date.month >= 12 and today_date.day > 10
        else today_date.year - 1
    )
    return year


@pytest.fixture
def year_to():
    return None


@pytest.fixture
def wrong_year():
    return 1801


@pytest.fixture
def prize_endpoint(year):
    return f"https://api.nobelprize.org/2.0/nobelPrizes?nobelPrizeYear={year}"


@pytest.fixture
def laureate_endpoint(year):
    return f"https://api.nobelprize.org/2.0/laureates?nobelPrizeYear={year}"


@pytest.fixture
def prize_kwargs():
    kwargs = config["nobel_api"]["prize_arguments"]
    kwargs = ast.literal_eval(kwargs)
    return kwargs


@pytest.fixture
def data_key():
    return "nobelPrizes"


@pytest.fixture
def path():
    return "nobel/source/prize/"


@pytest.fixture
def data_frame(data_key, prize_endpoint):
    response = requests.get(prize_endpoint).json()
    data_frame = pd.DataFrame.from_records(response.get(data_key))
    return data_frame


@pytest.fixture
def api_res(endpoint):
    response = requests.get(endpoint).json()
    print(response)
    return response


@pytest.fixture
def partition(year):
    partition_path = f"pt_year={year}"
    return partition_path


@pytest.fixture
def file_name(data_key, year):
    file_name = f"{data_key}_{year}.json"
    return file_name


@pytest.fixture
def file(file_name):
    file_path = os.path.join(
        parent_dir,
        config["local"]["local_file_path"],
        "nobelPrize_laureates",
        file_name,
    )
    return file_path


@pytest.fixture
def key(partition, file_name):
    key_name = partition + file_name
    return key_name


@pytest.fixture
def bucket():
    bucket_name = config.get("s3", "bucket_name")
    return bucket_name


@pytest.fixture
def bucket_path():
    bucket_path_name = config.get("s3", "bucket_path")
    return bucket_path_name


# class Test_nobelPrize_api:
#     """This class will check all success and failure test cases in nobelPrize api"""

#     def test_metaweather_api_object(self):
#         """This Method will test for the instance belong to the class MetaWeatherApi"""
#         self.obj = NobelPrizeApi(config, logger)
#         assert isinstance(self.obj, NobelPrizeApi)

#     def test_fetch_nobel_prize_is_done(self, prize_endpoint, year):
#         """This method will test fetch nobel prizes is done given year"""
#         self.obj = NobelPrizeApi(config, logger)
#         try:
#             response = self.obj.fetch_prize_or_laureaes_response(prize_endpoint, year)
#             result = requests.get(prize_endpoint).json()
#             print(result)
#         except Exception as err:
#             print(err)
#             result = None
#         assert response == result

#     @pytest.mark.xfail
#     def test_fetch_nobel_prize_not_done(self, prize_endpoint, wrong_year):
#         """This method will test fetch nobel prizes is not done for given year"""
#         self.obj = NobelPrizeApi(config, logger)
#         try:
#             response = self.obj.fetch_prize_or_laureaes_response(
#                 prize_endpoint, wrong_year
#             )
#             result = requests.get(prize_endpoint).json()
#             print(result)
#         except Exception as err:
#             print(err)
#             result = None
#         assert response != result

#     def test_fetch_laureates_is_done(self, laureate_endpoint, year):
#         """This method will test fetch laureates is done for given year"""
#         self.obj = NobelPrizeApi(config, logger)
#         try:
#             response = self.obj.fetch_prize_or_laureaes_response(
#                 laureate_endpoint, year
#             )
#             result = requests.get(laureate_endpoint).json()
#             print(result)
#         except Exception as err:
#             print(err)
#             result = None
#         assert response == result

#     @pytest.mark.xfail
#     def test_fetch_laureates_not_done(self, laureate_endpoint, wrong_year):
#         """This method will test fetch laureates is not done for given year"""
#         self.obj = NobelPrizeApi(config, logger)
#         try:
#             response = self.obj.fetch_prize_or_laureaes_response(
#                 laureate_endpoint, wrong_year
#             )
#             result = requests.get(laureate_endpoint).json()
#             print(result)
#         except Exception as err:
#             print(err)
#             result = None
#         assert response != result


# # class Test_s3:
# #     """This class will test all success and failure cases for s3 module"""

# #     def test_s3_object(self):
# #         """This method test the instance belong to the class of S3Service"""
# #         self.s3_obj = S3Service(logger)
# #         assert isinstance(self.s3_obj, S3Service)

# #     def test_upload_file(self, partition, key, bucket, bucket_path, file):
# #         """This method will test file is sucessfully uploaded"""
# #         self.my_client = S3Service(logger)
# #         self.my_client.upload_file(file, key)
# #         self.files = self.my_client.s3_obj.list_files(
# #             bucket, bucket_path + "/" + partition
# #         )
# #         assert len(self.files) > 0

# #     @pytest.mark.xfail
# #     def test_upload_file_failed(
# #         self, key, bucket, bucket_path, partition, unavailable_file
# #     ):
# #         """This method will test file is not sucessfully uploaded"""
# #         self.my_client = S3Service(logger)
# #         self.my_client.upload_file(unavailable_file, key)
# #         self.files = self.my_client.s3_obj.list_files(
# #             bucket, bucket_path + "/" + partition
# #         )
# #         assert len(self.files) == 0


# class Test_fetch_nobelPrizes_laureates:
#     """This class will test all success and failure cases for
#     fetch_nobel_prizes_laureates module"""

#     def test_nobel_prizes_laureates_object(self, year, year_to):
#         """This method test the instance belong to the class of NobelprizeLaureates"""
#         self.obj = NobelprizeLaureates(year, year_to)
#         assert isinstance(self.obj, NobelprizeLaureates)

#     def test_fetch_endpoint_response_done(self, year, year_to, prize_kwargs):
#         """This method will test whether the reponse get successfully"""
#         self.obj = NobelprizeLaureates(year, year_to)
#         response = self.obj.fetch_endpoint_response(**prize_kwargs)
#         print(response)
#         assert isinstance(response, pd.DataFrame)

#     @pytest.mark.xfail
#     def test_fetch_endpoint_response_not_done(self, year, year_to, prize_kwargs):
#         """This method will test whether the reponse not get successfully"""
#         year_to = 1000  # for failure case
#         self.obj = NobelprizeLaureates(year, year_to)
#         response = self.obj.fetch_endpoint_response(**prize_kwargs)
#         assert response is False

#     def test_get_dataframe_response_done(
#         self,
#         year,
#         year_to,
#         prize_kwargs,
#     ):
#         """This method will test whether response get successfully for given endpoint"""
#         self.obj = NobelprizeLaureates(year, year_to)
#         params = prize_kwargs
#         params["year"] = year
#         data_frame = self.obj.get_dataframe_response(**params)
#         assert isinstance(data_frame, pd.DataFrame)

#     @pytest.mark.xfail
#     def test_get_dataframe_response_not_done(self, wrong_year, year_to, prize_kwargs):
#         """This method will test whether response not get successfully for given endpoint"""
#         self.obj = NobelprizeLaureates(wrong_year, year_to)
#         params = prize_kwargs
#         params["year"] = wrong_year
#         data_frame = self.obj.get_dataframe_response(**params)
#         assert not isinstance(data_frame, pd.DataFrame)

#     def test_create_json_file_done(self, year, year_to, data_key, data_frame, path):
#         """This method will test whether it create json file is sucessful"""
#         self.obj = NobelprizeLaureates(year, year_to)
#         response = self.obj.create_json(data_key, data_frame, year, path)
#         assert response is True

#     @pytest.mark.xfail
#     def test_create_json_file_not_done(
#         self, year, year_to, data_key, data_frame, path
#     ):  # --tested
#         """This method will test whether it create json file is sucessful"""
#         self.obj = NobelprizeLaureates(year, year_to)
#         data_frame = pd.DataFrame()  # for failed case
#         print(data_frame)
#         response = self.obj.create_json(data_key, data_frame, year, path)
#         assert response is False

#     def test_get_partition_path_done(self, year, partition):  # --tested
#         """This method will test whether it get paartition is sucessful"""
#         response = get_partition(year)
#         assert response == partition

#     # @pytest.mark.xfail
#     # def test_get_partition_path_not_done(self,year,partition):  # --tested
#     #     """This method will test whether it get partition is sucessful"""
#     #     response = get_partition(year)
#     #     assert response == partition
    
class Test_dummy_s3:
    """This class will test the dummy_s3 module methods"""
    def test_dummy_s3_obj(self):
        """This method will test the instance belong to the class of NobelprizeLaureates"""
        self.obj = DummyS3(config,logger)
        assert isinstance(self.obj, DummyS3)
        
    def test_upload_dummy_local_s3_done(self, path, file, partition):
        """This method will test whether the upload to dummy s3 is success"""
        self.obj = DummyS3(config,logger)
        dest = path+partition
        response = self.obj.upload_dummy_local_s3(file,dest)
        assert response is True
    
    pytest.mark.xfail        
    def test_upload_dummy_local_s3_not_done(self, path, partition):
        """This method will test whether the upload to dummy s3 is not success"""
        self.obj = DummyS3(config,logger)
        dest = path+partition
        response = self.obj.upload_dummy_local_s3('unavilable_file',dest)
        print(response)
        assert response is False
        
