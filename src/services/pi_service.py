# Our PI Service handles call to and from the PI WebAPI. 
# We are using the requests_kerberos library to handle authenticating to the PI WebAPI.
#
# TODO: 
# (1) Consider using requests_futures, which would allow us to post asynchrously while still using
# requests_kerberos.
# (2) Consider moving PI WebAPI to a token-based authentication scheme so that we can run this
# consumer on Linux.

import json
import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL

# Disable insecure requests warning from self-signed certificate
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from src.services.logger_service import info
from src.services.config_service import get_pi_web_api_url
from src.services.logger_service import LOG

# We are using requests_kerberos library to add Kerberos authentication
# to the requests library. 
KERBEROS_AUTH = HTTPKerberosAuth(
    force_preemptive=True,
    mutual_authentication=OPTIONAL,
    delegate=True)

BASE_URL = get_pi_web_api_url()

futures_session = None

def get_webid(pitagname):
    """ 
    Get the WebId for tag using WebAPI query
    We use this to cache the WebIds to make the translation
    from asset name to PI WebId faster.
    """
    try:
        url = f'{BASE_URL}/search/query?q=name:"{pitagname}"'
        LOG.info(f'getting {url}')
        # verifty=false to ignore certificate. Will still give warning, which is suppressed
        # by our urllib3 call above. 
        # TODO: Download .crt file using Firefox and use verify=file.crt 
        # to verify self-signed cert
        resp = requests.get(url, auth=KERBEROS_AUTH, verify=False)
        resp = json.loads(resp.text)
        items = resp['Items']
        return items[0]['WebId']
    except Exception as ex:
        LOG.error(f'Exception: {ex}')   

def post_data_to_pi(data):
    """ 
    We post the data for multiple tags in bulk to the PI WebAPI for efficiency. PI will 
    compress the incoming data according to tag settings.

    We must run this on a Windows machine to use Kerberos authentication to the PI WebAPI.
    Switching to a token-based authentication mechanism would remove this dependency.

    TODO: Consider using requests_futures to POST asynchrounously.
    """
    url = f'{BASE_URL}/streamsets/recorded'
    resp = requests.post(url, json=data, auth=KERBEROS_AUTH, verify=False)
    
    if resp.status_code != 202:
        LOG.error(f'Problem posting data to PI: {resp.status_code}: {resp.text}')

    return





