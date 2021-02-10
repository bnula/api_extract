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
            self.api_realm = input("Input:")
        else:
            self.api_realm = api_realm
        self.__config = RawConfigParser()
        self.__config.read(api_config_file)
        self.__access_token = None
        self.__refresh_token = None
        self.__token_time_left = None

    def authenticate(self, token_type="refresh"):
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
            auth_code = self.__config.get(self.api_realm, "auth_code")
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
