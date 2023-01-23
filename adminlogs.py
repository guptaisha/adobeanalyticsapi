import configparser
import logging
import datetime
import requests
import jwt
import pandas as pd
import json
from datetime import datetime, timedelta, time

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

logging.basicConfig(level="INFO")
logger = logging.getLogger()

# Timestamp of yesterday
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")

# Start time of yesterday (midnight)
yesterday_start = datetime.combine(yesterday, time.min)
start_date_str = yesterday_start.strftime("%Y-%m-%dT%H:%M:%S-07")

# End time of yesterday (23:59:59)
yesterday_end = datetime.combine(yesterday, time.max)
end_date_str = yesterday_end.strftime("%Y-%m-%dT%H:%M:%S-07")


def get_jwt_token(config):
    with open(config["key_path"], 'r') as file:
        private_key = file.read()

    return jwt.encode({
        "exp": datetime.utcnow() + timedelta(seconds=30),
        "iss": config["orgid"],
        "sub": config["technicalaccountid"],
        "https://{}/s/{}".format(config["imshost"], config["metascopes"]): True,
        "aud": "https://{}/c/{}".format(config["imshost"], config["apikey"])
    }, private_key, algorithm='RS256')


def get_access_token(config, jwt_token):
    post_body = {
        "client_id": config["apikey"],
        "client_secret": config["secret"],
        "jwt_token": jwt_token
    }

    logger.info("Sending 'POST' request to {}".format(config["imsexchange"]))
    logger.info("Post body: {}".format(post_body))

    response = requests.post(config["imsexchange"], data=post_body)
    return response.json()["access_token"]


def get_first_global_company_id(config, access_token):
    response = requests.get(
        config["discoveryurl"],
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apikey"]
        }
    )

    # Return the first global company id
    return response.json().get("imsOrgs")[0].get("companies")[0].get("globalCompanyId")


def get_users_me(config, global_company_id, access_token):
    response = requests.get(
        "{}/{}/users/me".format(config["analyticsapiurl"], global_company_id),
        headers={
            "Authorization": "Bearer {}".format(access_token),
            "x-api-key": config["apikey"],
            #"x-proxy-global-company-id": global_company_id
        }
    )
    return response.json()


config_parser = configparser.ConfigParser()
config_parser.read("config.ini")

config = dict(config_parser["default"])
jwt_token = get_jwt_token(config)
logger.info("JWT Token: {}".format(jwt_token))
access_token = get_access_token(config, jwt_token)
logger.info("Access Token: {}".format(access_token))

global_company_id = get_first_global_company_id(config, access_token)
logger.info("global_company_id: {}".format(global_company_id))

response = get_users_me(config, global_company_id, access_token)
logger.info("users/me response: {}".format(response))

url = "https://analytics.adobe.io/api/threei2/auditlogs/usage"

querystring = {"startDate": start_date_str, "endDate": end_date_str, "limit":"100"}

headers = {
    "x-api-key": "eb5cf2643bd1412491a709483447b1b2",
    "Authorization": "Bearer" + access_token,
    "Accept": "application/json"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

# Convert the response to json
logs = json.loads(response.text)

# Method 1
# Convert json to pandas dataframe
logs_df = pd.DataFrame.from_dict(logs)

# extract sub dict
content_df = pd.json_normalize(logs_df['content'])
assert(len(logs_df) == len(content_df), "DataFrames are of different lengths, should match")

# left join both dataframes
final_logs_df = logs_df.join(content_df)

# drop original 'content' column
final_logs_df.drop('content', axis=1, inplace=True)

# reorder columns
final_logs_df = final_logs_df[['totalElements', 'lastPage', 'numberOfElements', 'totalPages',
       'firstPage', 'sort', 'size', 'number', 'eventType', 'eventDescription',
       'ipAddress', 'login', 'rsid', 'dateCreated']]


# Save the logs to a csv file
final_logs_df.to_csv('admin_logs_'+yesterday_str+'.csv', index=False)

print(f'{len(final_logs_df)} admin logs retrieved and saved to' 'admin_logs_'+yesterday_str+'.csv')
