from annotationengine.errors import AlignedVolumeNotFoundException
from flask import current_app
import requests
import logging
import os
from annotationframeworkclient.infoservice import InfoServiceClient
from annotationframeworkclient.auth import AuthClient


def get_aligned_volumes():
    server= current_app.config['GLOBAL_SERVER']
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(server_address=server,
                                   auth_client=auth,
                                   api_version=current_app.config.get('INFO_API_VERSION', 2))
    aligned_volume_names = infoclient.get_aligned_volumes()
    return aligned_volume_names


def get_aligned_volume(aligned_volume):
    infoservice = current_app.config['INFOSERVICE_ENDPOINT']
    url = os.path.join(infoservice, "api/v2/aligned_volume/{}".format(aligned_volume))
    r = requests.get(url)
    if r.status_code != 200:
        raise AlignedVolumeNotFoundException('aligned_volume {} not found'.format(aligned_volume))
    else:
        return r.json()
