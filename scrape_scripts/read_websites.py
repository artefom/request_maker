from requests.exceptions import ConnectionError, BaseHTTPError
import requests

requests.packages.urllib3.disable_warnings()

import re
from urllib.parse import urlparse, urlunparse
def format_site(x,protocol='http'):
    url_parsed = list(urlparse(x))
    url_parsed[0] = protocol

    return urlunparse(url_parsed).replace('///','//')

def main(site_orig):
    attempt_http = False
    attempt_http_message = (-1,'')
    attempt_https = False
    attempt_https_message = (-1,'')
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
    'From': 'https://www.google.com/'  # This is another valid field
    }
    
    try:
        site = format_site(site_orig,protocol='http')
        ret = requests.get( site, headers=headers, timeout=10, verify=False )
        if ret.status_code != 200:
            attempt_http_message = (ret.status_code, ret.text)
            if str(ret.status_code)[:1] == '3':
                return (ret.status_code, ret.text)

        else:
            attempt_http = True
            return ret.text
    except ConnectionError as ex:
        attempt_http_message = ('HTTP ConnectionError', str(ex) )
    except BaseHTTPError as ex:
        attempt_http_message = ('BASE HTTP ERROR', str(ex) )

    try:
        site = format_site(site_orig,protocol='https')
        ret = requests.get( site, headers=headers, timeout=5, verify=False )
        if ret.status_code != 200:
            attempt_https_message = (ret.status_code, ret.text)
            if str(ret.status_code)[:1] == '3':
                return (ret.status_code, ret.text)


        else:
            attempt_https = True
            return ret.text
    except ConnectionError as ex:
        attempt_https_message = ('HTTPS ConnectionError', str(ex) )
    except BaseHTTPError as ex:
        attempt_https_message = ('BASE HTTP ERROR', str(ex) )

    raise ValueError("#SITE: {}\n#HTTP ERORR {}: {}\n#HTTPS ERROR {}: {}".format(site_orig,attempt_http_message[0],attempt_http_message[1],
                                                                    attempt_https_message[0],attempt_https_message[1]))
