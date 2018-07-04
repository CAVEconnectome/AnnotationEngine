from flask import current_app, g
import cloudvolume


class MyCloudVolume(cloudvolume.CloudVolume):

    def lookup_voxel(self, x, y, z):
        voxel = self[x, y, z]
        return voxel


def get_cv():
    if 'cv' not in g:
        cv_path = current_app.config['CV_SEGMENTATION_PATH']
        g.cv = MyCloudVolume(cv_path,
                             mip=0,
                             fill_missing=True)
    return g.cv
