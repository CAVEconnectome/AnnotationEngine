from annotationengine.errors import AlignedVolumeNotFoundException
from flask import current_app
import requests
import logging
import os


def get_aligned_volumes():
    infoservice = current_app.config['INFOSERVICE_ENDPOINT']
    url = os.path.join(infoservice, "api/v2/aligned_volume")
    r = requests.get(url)
    aligned_volume_names = r.json()
    return aligned_volume_names


def get_aligned_volume(aligned_volume):
    infoservice = current_app.config['INFOSERVICE_ENDPOINT']
    url = os.path.join(infoservice, "api/v2/aligned_volume/{}".format(aligned_volume))
    r = requests.get(url)
    if r.status_code != 200:
        raise AlignedVolumeNotFoundException('aligned_volume {} not found'.format(aligned_volume))
    else:
        return r.json()
