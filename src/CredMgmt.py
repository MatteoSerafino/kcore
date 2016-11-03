
import api_secrets
import ujson as json
import requests
from urllib.parse import quote
from base64 import b64encode
import os

from TwAPIer import TwiAPIer

class CredMgmt(object) :
    '''
    OAuth2 credential management utility for TwitterMiner.
     
    Tokens do not currently expire, so you shouldn't have to run this. Get lost.
    '''
    
    def __init__(self, token_dir=None) :

        self.secrets_list = []
        self.account_idx = -1
        
        try :            
            # Load user secrets
            if token_dir is not None:
                token_filename = os.path.join(token_dir, 'temp_tokens.json')
            else:
                token_filename = 'temp_tokens.json'
            with open(token_filename,'r') as tokenfile :
                user_tokens = json.load(tokenfile)
            
            for user in user_tokens :
                self.secrets_list.append((user['id'], user['oauth_token'], user['oauth_token_secret']))
            
        except :
            print('Invalid token file.')
            raise
    
    def useCredentials(self) :
        self.account_idx = (self.account_idx + 1) % len(self.secrets_list)
        return self.secrets_list[self.account_idx]
    
    def giveAPI(self) :
        
        app_secrets = api_secrets.user_oauth1_secrets
        usr_secrets = self.useCredentials()
        
        permissions_bundle = {
            "consumer_key" : app_secrets[1]['consumer_key'],
            "consumer_secret" : app_secrets[1]['consumer_secret'],
            "token_key" : usr_secrets[1],
            "token_secret" : usr_secrets[2]
        }
        
        return TwiAPIer(api_keys=permissions_bundle)
    
    def getBearerToken(self, application_name) :
        
        # Twitter Oauth2 token-dispensing endpoint
        ENDPOINT = 'https://api.twitter.com/oauth2/token'
        
        # Generate Bearer Token credentials
        secrets = api_secrets.oauth()
        con_key = quote(secrets['consumer_key'])
        con_sec = quote(secrets['consumer_secret'])
        bt_creds = b64encode((con_key + ':' + con_sec).encode('utf-8'))
        bt_creds = bt_creds[0:len(bt_creds)-2].decode('utf-8')
        
        
        # Prep headers
        headers = {
            'Authorization' : 'Basic ' + bt_creds,
            'Host' : 'api.twitter.com',
            'User-Agent' : application_name
        }
        
        # Prep request
        params = {
            'grant_type' : 'client_credentials'
        }
        
        # Moment of truth
        resp = requests.post(ENDPOINT, data=params, headers=headers)
        print(resp)
        return resp.json()['access_token']
    
    def getUserTokens(self, kcore_token) :
        
        ENDPOINT = 'http://www.kcore-analytics.com/api/'
        
        # Prep headers
        headers = {
            'ACCESSTOKEN' : kcore_token
        }
        
        resp = requests.get(ENDPOINT, headers=headers)
        print(resp)
        return resp.json()
        
        
# Quick and dirty driver
if __name__ == '__main__' :
    
    '''
    application_name = 'KCoreAnalytics'
    '''
    
    cm = CredMgmt()
    
    '''
    token = cm.getBearerToken(application_name)
    print(application_name)
    print('Bearer ' + token)
    '''
    
    tokens = cm.getUserTokens(0000000000000000)
    print('User tokens: ')
    print(json.dumps(tokens,sort_keys=True,indent=4))
    
    