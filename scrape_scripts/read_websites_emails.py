from requests.exceptions import ConnectionError, BaseHTTPError
import requests
import re
from difflib import SequenceMatcher
import string
from urllib.parse import urlparse, urlunparse
from slugify import slugify

requests.packages.urllib3.disable_warnings()
email_regex = '(?:[a-z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&\'*+/=?^_`{|}~-]+)*|"(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21\\x23-\\x5b\\x5d-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\\x01-\\x08\\x0b\\x0c\\x0e-\\x1f\\x21-\\x5a\\x53-\\x7f]|\\\\[\\x01-\\x09\\x0b\\x0c\\x0e-\\x7f])+)\\])'
alnum = [ i for i in string.printable if not i.isalnum() ]


def format_site(x,protocol='http'):
    url_parsed = list(urlparse(x))
    url_parsed[0] = protocol

    return urlunparse(url_parsed).replace('///','//')

def lcs(S,T):
    m = len(S)
    n = len(T)
    counter = [[0]*(n+1) for x in range(m+1)]
    longest = 0
    lcs_set = set()
    for i in range(m):
        for j in range(n):
            if S[i] == T[j]:
                c = counter[i][j] + 1
                counter[i+1][j+1] = c
                if c > longest:
                    lcs_set = set()
                    longest = c
                    lcs_set.add(S[i-c+1:i+1])
                elif c == longest:
                    lcs_set.add(S[i-c+1:i+1])

    return lcs_set

def strip_email(x):
    changed = True
    while changed:
        changed = False
        for ch in alnum:
            lx = len(x)
            x = x.strip(ch)
            if len(x) != lx:
                changed = True
    return x
                
def process_mail(company_name, x):

    s1 = slugify(company_name,separator='')
    s2 = slugify(x,separator='')
    
    common_len = 0
    
    x_low = x.lower()
    if 'info' in x_low:
        common_len += 10
    if '.mos' in x_low or 'mos.' in x_low or\
        'msk.' in x_low or '.msk' in x_low or\
        'mos@' in x_low or\
        'msk@' in x_low or\
        'moscow' in x_low:
        common_len += 8
    if 'sales' in x_low or 'order' in x_low or 'zakaz' in x_low:
        common_len += 5
    if 'office' in x_low:
        common_len += 2
    
    changed = True
    while changed:
        changed = False
        common_strs = lcs(s1,s2)
        for common_str in common_strs:
            if len(common_str) <= 2:
                continue
            change_len = len(s2)
            s1 = s1.replace(common_str,'')
            s2 = s2.replace(common_str,'')
            change_len = change_len-len(s2)
            common_len += change_len
            changed = True
    #common_len = SequenceMatcher(None, s1, s2).find_longest_match(0, len(s1), 0, len(s2))
    return common_len,x

def parse_html(text):
    emails = re.findall(email_regex,text)
    return emails
    
def get_emails(site,html):
    emails = [ strip_email(i) for i in parse_html(html) if '/' not in i and '\\' not in i ]
    emails_count = dict()
    for email in emails:
        if email not in emails_count:
            emails_count[email] = 0
        emails_count[email] += 1
    emails = list(set(emails))
    emails = [ process_mail(site,i) for i in emails ]
    emails = [ (k,emails_count[v],v) for k,v in emails ]
    return sorted(emails,key=lambda x: -x[0])


def main(link):
    html = read_site(link)
    if isinstance(html,tuple):
        html = html[1]
    emails = get_emails(link,html)
    return emails

def read_site(site_orig):
    
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
