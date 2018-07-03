from annotationengine import app
import cloudvolume

cv_seg = cloudvolume.CloudVolume(app.config['CV_SEGMENTATION_PATH'],
                                 mip=0,
                                 fill_missing=True)


def lookup_supervoxel(x, y, z):
    supervoxel_id = cv_seg[x, y, z]
    return supervoxel_id
