import asycio
import aiohttp

from src.services.logger_service import info
from src.services.config_service import get_pi_web_api_crt, get_pi_web_api_url

async def post_data_to_pi(data):
    """ 
    

    TODO: Explore using requests_futures
    """
    URL = get_pi_web_api_url
    CERT_FILE_NAME = get_pi_web_api_crt



    return 'SUCCESS'





