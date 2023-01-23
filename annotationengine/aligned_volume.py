from annotationengine.errors import AlignedVolumeNotFoundException
from flask import current_app
import requests
import logging
import os
from caveclient.infoservice import InfoServiceClient
from caveclient.auth import AuthClient
import cachetools.func


@cachetools.func.ttl_cache(maxsize=2, ttl=5 * 60)
def get_aligned_volumes():
    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    aligned_volume_names = infoclient.get_aligned_volumes()
    return aligned_volume_names


@cachetools.func.ttl_cache(maxsize=10, ttl=5 * 60)
def get_aligned_volume(aligned_volume):
    infoservice = current_app.config["INFOSERVICE_ENDPOINT"]
    url = os.path.join(infoservice, f"api/v2/aligned_volume/{aligned_volume}")
    r = requests.get(url)
    if r.status_code != 200:
        raise AlignedVolumeNotFoundException(
            f"aligned_volume {aligned_volume} not found"
        )
    else:
        return r.json()


@cachetools.func.ttl_cache(maxsize=10, ttl=5 * 60)
def get_datastack_info(datastack_name):
    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    return infoclient.get_datastack_info(datastack_name=datastack_name)


@cachetools.func.ttl_cache(maxsize=5, ttl=60 * 60)
def get_datastacks_from_aligned_volumes(aligned_volume_name):
    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)
    infoclient = InfoServiceClient(
        server_address=server,
        auth_client=auth,
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    datastack_names = infoclient.get_datastacks_by_aligned_volume(aligned_volume_name)
    return datastack_names
