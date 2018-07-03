import cloudvolume
import os
import numpy as np

N = 512
blockN = 16
block_per_row = N / blockN

path = "file://{}/data/segmentation".format(os.path.abspath('.'))
print(path)
info = cloudvolume.CloudVolume.create_new_info(
    num_channels=1,
    layer_type='segmentation',
    data_type='uint64',  # Channel images might be 'uint8'
    # raw, jpeg, compressed_segmentation, fpzip, kempressed
    encoding='raw',
    resolution=[4, 4, 40],  # Voxel scaling, units are in nanometers
    voxel_offset=[0, 0, 0],  # x,y,z offset in voxels from the origin
    # Pick a convenient size for your underlying chunk representation
    # Powers of two are recommended, doesn't need to cover image exactly
    chunk_size=[128, 128, 64],  # units are voxels
    volume_size=[N, N, N],  # e.g. a cubic millimeter dataset
)
vol = cloudvolume.CloudVolume(path, info=info)
vol.commit_info()

xx, yy, zz = np.meshgrid(np.arange(0, N),
                         np.arange(0, N),
                         np.arange(0, N))

seg = np.int64(xx / blockN) + \
    block_per_row * np.int64(yy / blockN) + \
    block_per_row * block_per_row * np.int64(zz / blockN)
vol[:] = np.uint64(seg)
