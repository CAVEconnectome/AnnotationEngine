import json


def test_post_bad_type(client, test_dataset):
    junk_d = {
        'type': 'junk_annotation',
        'tag': 'junk'
    }
    url = '/annotation/dataset/{}/junk_annotation'.format(test_dataset)
    response = client.post(url,
                           data=json.dumps(junk_d))
    assert(response.status_code == 404)
