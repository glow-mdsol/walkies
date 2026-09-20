###############################################################################
#  
#  Carelink Client 2 library
#  
#  Description:
#
#    This library implements a client for the Medtronic Carelink API
#    as used by the official Carelink Connect Android app.
#  
#  Author:
#
#    Ondrej Wisniewski (ondrej.wisniewski *at* gmail.com)
#  
#  Changelog:
#
#    28/12/2023 - Initial version
#    11/04/2024 - Check for valid data in API response in _get_data()
#    19/11/2024 - Update CARELINK_CONFIG_URL
#    11/02/2025 - Update CARELINK_CONFIG_URL to ver 3.3
#    03/01/2026 - Fix for new Auth method (by @m0rt4l1n)
#
#  Copyright 2023-2026, Ondrej Wisniewski
#
###############################################################################

import json
import requests
import time
import base64
import os
import logging as log
from datetime import datetime, timedelta

# Version string
VERSION = "1.4"

# Constants
DEFAULT_FILENAME = "logindata.json"
CARELINK_CONFIG_URL = "https://clcloud.minimed.eu/connect/carepartner/v13/discover/android/3.6"
AUTH_ERROR_CODES = [401, 403]
COMMON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 10; Nexus 5X Build/QQ3A.200805.001)",
}

# Logging config
FORMAT = "[%(asctime)s:%(levelname)s] %(message)s"
log.basicConfig(format=FORMAT, datefmt="%Y-%m-%d %H:%M:%S", level=log.INFO)


class CareLinkClient(object):

    def __init__(self, tokenFile=DEFAULT_FILENAME):
        self.__version = VERSION

        # Authorization
        self.__tokenFile = tokenFile
        self.__tokenData = None
        self.__accessTokenPayload = None

        # API config
        self.__config = None

        # User info
        self.__username = None
        self.__user = None
        self.__patient = None
        self.__country = None

        # API status
        self.__last_api_status = None

    def _read_token_file(self, filename):
        log.info("_read_token_file()")
        token_data = None
        if os.path.isfile(filename):
            try:
                with open(filename, "r") as f:
                    token_data = json.loads(f.read())
            except json.JSONDecodeError:
                log.error("ERROR: failed parsing token file %s" % filename)

            if token_data is not None:
                required_fields = ["access_token", "refresh_token", "scope", "client_id"]
                for f in required_fields:
                    if f not in token_data:
                        log.error("ERROR: field %s is missing from token file" % f)
        else:
            log.error("ERROR: token file %s not found" % filename)
        return token_data

    def _write_token_file(self, obj, filename):
        log.info("_write_token_file()")
        with open(filename, 'w') as f:
            json.dump(obj, f, indent=4)

    def _get_config(self, discovery_url, country):
        log.info("_get_config()")
        resp = requests.get(discovery_url)
        data = resp.json()
        region = None
        config = None

        for c in data["supportedCountries"]:
            try:
                region = c[country.upper()]["region"]
                break
            except KeyError:
                pass
        if region is None:
            raise Exception("ERROR: country code %s is not supported" % country)

        for c in data["CP"]:
            if c["region"] == region:
                config = c
                break
        if config is None:
            raise Exception("ERROR: failed to get config base urls for region %s" % region)

        sso_configuration_key = config["UseSSOConfiguration"]
        resp = requests.get(config[sso_configuration_key])
        sso_config = resp.json()
        sso_base_url = "https://%s:%d/%s" % (sso_config["server"]["hostname"],
                                             sso_config["server"]["port"],
                                             sso_config["server"]["prefix"])
        if sso_base_url.endswith('/'):
            sso_base_url = sso_base_url[:-1]

        token_url = sso_base_url + sso_config["system_endpoints"]["token_endpoint_path"]
        c["token_url"] = token_url
        return config

    def _get_user(self, config, token_data):
        log.info("_get_user()")
        url = config["baseUrlCareLink"] + "/users/me"
        headers = dict(COMMON_HEADERS)
        if "mag-identifier" in token_data:
            headers["mag-identifier"] = token_data["mag-identifier"]
        headers["Authorization"] = "Bearer " + token_data["access_token"]
        self.__last_api_status = None
        resp = requests.get(url=url, headers=headers)
        self.__last_api_status = resp.status_code
        try:
            user = resp.json()
        except:
            user = None
        return user

    def _get_patient(self, config, token_data):
        log.info("_get_patient()")
        url = config["baseUrlCareLink"] + "/links/patients"
        headers = dict(COMMON_HEADERS)
        if "mag-identifier" in token_data:
            headers["mag-identifier"] = token_data["mag-identifier"]
        headers["Authorization"] = "Bearer " + token_data["access_token"]
        self.__last_api_status = None
        resp = requests.get(url=url, headers=headers)
        self.__last_api_status = resp.status_code
        try:
            patient = resp.json()[0]
        except:
            patient = None
        return patient

    def _get_data(self, config, token_data, username, role, patientid):
        log.info("_get_data()")
        url = config["baseUrlCumulus"] + "/display/message"
        headers = dict(COMMON_HEADERS)
        if "mag-identifier" in token_data:
            headers["mag-identifier"] = token_data["mag-identifier"]
        headers["Authorization"] = "Bearer " + token_data["access_token"]
        data = {}
        data["username"] = username
        if role in ["CARE_PARTNER", "CARE_PARTNER_OUS"]:
            data["role"] = "carepartner"
            data["patientId"] = patientid
        else:
            data["role"] = "patient"

        self.__last_api_status = None
        resp = requests.post(url=url, headers=headers, data=json.dumps(data))
        self.__last_api_status = resp.status_code
        try:
            my_data = resp.json()
        except:
            my_data = None
        return my_data

    def _do_refresh(self, config, token_data):
        log.info("_do_refresh()")
        token_url = config["token_url"]
        data = {
            "refresh_token": token_data["refresh_token"],
            "client_id": token_data["client_id"],
            "grant_type": "refresh_token"
        }
        if "client_secret" in token_data:
            data["client_secret"] = token_data["client_secret"]
        headers = {}
        if "mag-identifier" in token_data:
            headers["mag-identifier"] = token_data["mag-identifier"]
        resp = requests.post(url=token_url, headers=headers, data=data)
        if resp.status_code != 200:
            raise Exception("ERROR: failed to refresh token")
        new_data = resp.json()
        token_data["access_token"] = new_data["access_token"]
        token_data["refresh_token"] = new_data["refresh_token"]
        return token_data

    def _get_access_token_payload(self, token_data):
        log.info("_get_access_token_payload()")
        try:
            token = token_data["access_token"]
        except:
            return None
        try:
            payload_b64 = token.split('.')[1]
            payload_b64_bytes = payload_b64.encode()
            missing_padding = (4 - len(payload_b64_bytes) % 4) % 4
            if missing_padding:
                payload_b64_bytes += b'=' * missing_padding
            payload_bytes = base64.b64decode(payload_b64_bytes)
            payload = payload_bytes.decode()
            payload_json = json.loads(payload)
        except:
            return None
        return payload_json

    def _is_token_valid(self, access_token_payload):
        log.info("_is_token_valid()")
        try:
            token_validto = access_token_payload["exp"]
        except:
            return False

        tdiff = token_validto - time.time()
        if tdiff < 0:
            return False
        if tdiff < 600:
            return False
        return True

    def _init(self):
        self.__tokenData = self._read_token_file(self.__tokenFile)
        if self.__tokenData is None:
            return False
        self.__accessTokenPayload = self._get_access_token_payload(self.__tokenData)
        if self.__accessTokenPayload is None:
            return False
        try:
            self.__country = self.__accessTokenPayload["token_details"]["country"]
            self.__config = self._get_config(CARELINK_CONFIG_URL, self.__country)
            self.__username = self.__accessTokenPayload["token_details"]["preferred_username"]
            self.__user = self._get_user(self.__config, self.__tokenData)
            if self.__user["role"] in ["CARE_PARTNER", "CARE_PARTNER_OUS"]:
                self.__patient = self._get_patient(self.__config, self.__tokenData)
        except Exception as e:
            log.error(e)
            if self.__last_api_status in AUTH_ERROR_CODES:
                try:
                    self.__tokenData = self._do_refresh(self.__config, self.__tokenData)
                    self.__accessTokenPayload = self._get_access_token_payload(self.__tokenData)
                    self._write_token_file(self.__tokenData, self.__tokenFile)
                except Exception as e:
                    log.error(e)
            return False
        return True

    def init(self):
        if self._init() == False:
            if self._init() == False:
                log.error("ERROR: unable to initialize")
                return False
        return True

    def getRecentData(self):
        if not self._is_token_valid(self.__accessTokenPayload):
            self.__tokenData = self._do_refresh(self.__config, self.__tokenData)
            self.__accessTokenPayload = self._get_access_token_payload(self.__tokenData)
            self._write_token_file(self.__tokenData, self.__tokenFile)
            if not self._is_token_valid(self.__accessTokenPayload):
                log.error("ERROR: unable to get valid access token")
                return None

        if self.__patient is not None:
            patientId = self.__patient["username"]
        else:
            patientId = None

        data = self._get_data(self.__config,
                              self.__tokenData,
                              self.__username,
                              self.__user["role"],
                              patientId)
        if self.__last_api_status in AUTH_ERROR_CODES:
            self.__tokenData = self._do_refresh(self.__config, self.__tokenData)
            self.__accessTokenPayload = self._get_access_token_payload(self.__tokenData)
            self._write_token_file(self.__tokenData, self.__tokenFile)

            data = self._get_data(self.__config,
                                  self.__tokenData,
                                  self.__username,
                                  self.__user["role"],
                                  patientId)
            if self.__last_api_status in AUTH_ERROR_CODES:
                log.error("ERROR: unable to get data")
                return None
        return data

    def getLastResponseCode(self):
        return self.__last_api_status

    def getClientVersion(self):
        return self.__version
