import asyncio
import json
from requests_futures.sessions import FuturesSession
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

# Disable insecure requests warning from self-signed certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from src.services.logger_service import info
from src.services.config_service import get_pi_web_api_url
from src.services.logger_service import LOG

KERBEROS_AUTH = HTTPKerberosAuth(
    force_preemptive=True,
    mutual_authentication=OPTIONAL,
    delegate=True)

BASE_URL = get_pi_web_api_url()

futures_session = None

def get_webid(pitagname):
    """ Get the WebId for each tag in CSV file """
    try:
        url = f'{BASE_URL}/search/query?q=name:"{pitagname}"'
        LOG.info(f'getting {url}')
        # verifty=false to ignore certificate. Will still give warning. Download .crt file
        # using Firefox and use verify=file.crt to verify self-signed cert
        resp = requests.get(url, auth=KERBEROS_AUTH, verify=False)
        resp = json.loads(resp.text)
        items = resp['Items']
        return items[0]['WebId']
    except Exception as ex:
        LOG.error(f'Exception: {ex}')   

async def post_data_to_pi(data):
    """ 
    
    """
    for reading in data:
        webid = reading["WebId"]
        payload = dict()
        payload["Timestamp"] = reading["Items"][0]['Timestamp']
        payload["Value"] = reading["Items"][0]['Value']
        url = f'{BASE_URL}/streams/{webid}/value'
        json_payload = json.dumps(payload)
        
        resp = requests.post(url, json=json_payload, auth=KERBEROS_AUTH, verify=False)

        print(resp)

    return 'SUCCESS'





