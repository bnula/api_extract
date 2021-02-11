import requests
import time
import datetime
import zipfile
import os
import setup_logs
import json
from configparser import RawConfigParser


class CallApi:
    def __init__(self, api_realm, db_connection="dev_server"):
        self.__logger = setup_logs.setup_logger(name=f"API_{api_realm}", path="D:/Python/Logs/Call_API")
        # validation sets
        misc_config_file = "D:/Python/config_files/misc_api_config.json"
        api_config_file = "D:/Python/config_files/procurement_api.config"
        with open(misc_config_file, "r", encoding="utf-8") as file:
            data = json.load(file)
            self.__valid_realms = data["reams"]
            self.__valid_views = data["views"]
        if api_realm not in self.__valid_realms:
            print(f"Please select a valid realm from the list: {self.__valid_realms}")
            self.__api_realm = input("Input:")
        else:
            self.__api_realm = api_realm
        self.__config = RawConfigParser()
        self.__config.read(api_config_file)
        self.__api_key = self.__config.get(self.__api_realm, "api_key")
        self.__tool_url = self.__config.get("server", "tool_url")
        self.__job_result_endpoint = self.__config.get("server", "job_result")
        self.__access_token = None
        self.__refresh_token = None
        self.__token_time_left = None
        self.__view_name = None
        self.__job_id = None
        self.__zip_file_list = None

    def authenticate(self, token_type="refresh"):
        # can be changed to use if access_token is None for initial token request
        try:
            # based on token_type pick an endpoint
            if token_type.lower() == "access":
                self.__logger.info("Request access and refresh tokens")
                access_param = self.__config.get("server", "access") 
                t_type = access_param
            else:
                self.__logger.info("Refresh access and request tokens")
                refresh_param = self.__config.get("server", "refresh")
                t_type = refresh_param + self.__refresh_token
            
            # create request url
            auth_url = self.__config.get("server", "auth_url")
            url = auth_url + t_type
            self.__logger.info(f"token request url = {url}")

            # create request
            auth_code = self.__config.get(self.__api_realm, "auth_code")
            payload = {}
            headers = {
                "Authorization": f"Basic {auth_code}",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            # send the request
            response = requests.request("POST", url=url, headers=headers, data=payload)

            # parse the response
            if response.status_code == 200:
                self.__logger.info(f"authentication successful - {response.status_code}")
                r_json = response.json()

                # return both tokens
                self.__access_token = r_json["access_token"]
                self.__refresh_token = r_json["refresh_token"]
                self.__token_time_left = r_json["expires_in"]
            else:
                self.__logger.info(f"authentication request response status code - {response.status_code}")
                self.__logger.info(f"authentication request response - {response.json()}")
                raise ValueError(f"Authentication failed - {response.status_code} {response.json()}")

        except Exception as e:
            self.__logger.error(e)
            print(e)

    def submit_job(self, view_name):
        self.__logger.info(f"========== {view_name} submit job ==========")
        self.__view_name = view_name
        try:
            self.__logger.info(f"refresh tokens")
            # check the token expiration time and refresh the token
            self.authenticate()
            if self.__token_time_left < 45:
                time.sleep(60)
                self.authenticate()
            # create url
            job_sub_endpoint = self.__config.get("server", "job_submission")
            url = f"{self.__tool_url}/{job_sub_endpoint}/jobs?realm={self.__api_realm}"
            self.__logger.info(f"job submit url - {url}")
            # create request variables
            yesterday = datetime.datetime.now() - datetime.timedelta(-1)
            start_date = f'{yesterday.strftime("%D-%m-%y")}T00:00:01'
            end_date = f'{yesterday.strftime("%D-%m-%y")}T23:59:59'
            # create request
            payload = '{"ViewTemplateName": ' + f'"{view_name}"' + ' "filters": {"createdFrom": ' + f'"{start_date}"' + ', "createdTo:" ' + f'"{end_date}"' + "}}"
            headers = {
                "apiKey": self.__api_key,
                "Authorization": f"Bearer {self.__access_token}",
                "Content-Type": "application/json",
                "Accept": "applicaiton/json"
            }
            self.__logger.info(f"job submission payload = {payload}")
            # send the request
            response = requests.request("POST", url, headers=headers, data=payload)
            # if the token has expired refresh it and resend the request
            self.authenticate()
            if response.status_code == 401:
                print("need to refresh the token")
                self.authenticate()
                response = requests.request("POST", url, headers=headers, data=payload)
            # retrieve job_id
            r_json = response.json()
            self.__job_id = r_json["jobId"]
            print(f"job id: {self.__job_id}")
            self.__logger.info(f"Job Id: {self.__job_id}")
        except Exception as e:
            self.__logger.error(e)
            print(e)

    def check_job_result(self):
        self.__logger.info(f"========== job result - {self.__view_name} ==========")
        try:
            self.__logger.info("refresh tokens")
            self.authenticate()
            if self.__token_time_left() < 45:
                time.sleep(60)
                self.authenticate()
            # create url
            url = f"{self.__tool_url}/{self.__job_result_endpoint}/jobs/{self.__job_id}?realm={self.__api_realm}"
            payload = {}
            headers = {
                "apiKey": self.__api_key,
                "Authorization": f"Bearer {self.__access_token}",
                "Accept": "application/json"
            }

            # send the request
            response = requests.request("GET", url, headers=headers, data=payload)

            if response.status_code == 401:
                print("need to refresh token")
                self.authenticate()
                response = requests.request("GET", headers=headers, data=payload)

            r_json = response.json()
            job_status = r_json["status"]

            print(f"job status: {job_status}")
            while job_status == "pending":
                self.authenticate()
                # refresh access token and refresh headers
                if self.__token_time_left < 45:
                    time.sleep(60)
                    self.authenticate()
                    headers = {
                        "apiKey": self.__api_key,
                        "Authorization": f"Bearer {self.__access_token}",
                        "Accept": "application/json"
                    }
                response = requests.request("GET", headers=headers, data=payload)
                r_json = response.json()
                job_status = r_json["status"]
                if job_status == "completed":
                    self.authenticate()
                    # refresh access token and refresh headers
                    if self.__token_time_left < 45:
                        time.sleep(60)
                        self.authenticate()
                    break
                print("waiting 180 seconds")
                time.sleep(180)
                print(f"job status: {job_status}")
            self.__zip_file_list = r_json["files"]
            self.__logger.info(f"list of zip files: {self.__zip_file_list}")
        except Exception as e:
            self.__logger.error(e)
            print(e)

    def download_zip_files(self):
        self.__logger.info(f"========== download zip files - {self.__view_name} ==========")
        try:
            # refresh the tokens
            self.__logger.info("refresh tokens")
            self.authenticate()
            if self.__token_time_left < 45:
                time.sleep(60)
                self.authenticate()
            self.__logger.info("go over each zip file")
            for zip_file in self.__zip_file_list:
                self.__logger.info(f"zip file: {zip_file}")
                url = f"{self.__tool_url}/{self.__job_result_endpoint}/jobs/{self.__job_id}/files/{zip_file}?realm={self.__api_realm}"
                payload = {}
                headers = {
                    "apiKey": self.__api_key,
                    "Authorization": f"Bearer {self.__access_token}"
                }
                # send the http request
                response = requests.request("GET", url, headers=headers, data=payload)
                if response.status_code == 401:
                    self.authenticate()
                    headers = {
                        "apiKey": self.__api_key,
                        "Authorization": f"Bearer {self.__access_token}"
                    }
                    response = requests.request("GET", url, headers=headers, data=payload)
                if response.status_code == 200:
                    binary_content = response.content
                    download_file_path = f"D:/Api_extracts/{zip_file}"
                    if os.path.isfile(download_file_path):
                        os.unlink(download_file_path)
                    open(download_file_path, "x")
                    with open(download_file_path, "wb") as f:
                        f.write(binary_content)
                        f.close()
                    extract_file_path = "D:/API_extracts/unzipped"
                    extracted_file = f"{extract_file_path}/records.txt"
                    if os.path.isfile(extracted_file):
                        os.unlink(extracted_file)
                    with zipfile.ZipFile(download_file_path, "r") as zip_ref:
                        zip_ref.extractall(extract_file_path)
                    # call ParseFile class
                    if self.__view_name == self.__config.get("views", "order"):
                        # run purchase order method
                        os.unlink(extracted_file)
                    elif self.__view_name == self.__config.get("views", "invoice"):
                        # run invoice method
                        os.unlink(extracted_file)
                    elif self.__view_name == self.__config.get("views", "order"):
                        # run requisition method
                        os.unlink(extracted_file)
        except Exception as e:
            self.__logger.error(e)
            print(e)
