import requests
import json
url_base = "http://0.0.0.0:7777"

synapse_d = [{
    'type': 'synapse',
    'pre_pt':
        {
            'position': [31, 31, 0],
        },
    'ctr_pt':
        {
            'position': [31, 31, 0],
        },
    'post_pt':
        {
            'position': [31, 31, 0],
        }
}]
url = '{}/annotation/dataset/{}/synapse'.format(url_base, 'demo')


response = requests.post(url, data=json.dumps(synapse_d))
print(dir(response))
print(response.text)
