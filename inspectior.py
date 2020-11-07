from __future__ import unicode_literals

from builtins import str, bytes, dict, int
from builtins import range
import random
import requests
import hashlib
import sys
import unidecode

unicode = str

USER_AGENT = "Mozilla/5.0 (Windows; U; MSIE 10.0; Windows NT 9.0; es-ES)"
user_agent = {"user-agent": USER_AGENT}


class Inspector:
    """This class mission is to examine the behaviour of the application when on
        purpose an inexistent page is requested"""
    TEST404_OK = 0
    TEST404_MD5 = 1
    TEST404_STRING = 2
    TEST404_URL = 3
    TEST404_NONE = 4

    def __init__(self, target):
        self.target = target

    def _give_it_a_try(self):
        """Every time this method is called it will request a random resource
            the target domain. Return value is a dictionary with values as
            HTTP response code, resquest size, md5 of the content and the content
            itself. If there were a redirection it will record the new url"""
        s = []
        target = self.target

        print("Checking with %s" % target)

        page = requests.get(target, headers=user_agent, verify=False)
        content = page.text

        result = {'code': str(page.status_code),
                  'size': len(content),
                  'md5': hashlib.md5(str("".join(str(content)))).hexdigest(),
                  'content': content,
                  'location': None}

        if len(page.history) >= 1:
            result['location'] = page.url

        return result

i = Inspector('https://www.frysfood.com')
print(i._give_it_a_try())