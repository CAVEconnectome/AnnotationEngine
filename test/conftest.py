import pytest
from annotationengine import create_app
import cloudvolume
import numpy as np
import tempfile
import shutil


@pytest.fixture
def cv(N=64, blockN=16):
    block_per_row = N / blockN

    tempdir = tempfile.mkdtemp()
    path = "file:/{}".format(tempdir)

    info = cloudvolume.CloudVolume.create_new_info(
        num_channels=1,
        layer_type='segmentation',
        data_type='uint64',
        encoding='raw',
        resolution=[4, 4, 40],  # Voxel scaling, units are in nanometers
        voxel_offset=[0, 0, 0],  # x,y,z offset in voxels from the origin
        # Pick a convenient size for your underlying chunk representation
        # Powers of two are recommended, doesn't need to cover image exactly
        chunk_size=[64, 64, 64],  # units are voxels
        volume_size=[N, N, N],
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

    yield path

    shutil.rmtree(tempdir)


@pytest.fixture
def test_dataset():
    
    return 'test'


@pytest.fixture
def app(cv, test_dataset):
    app = create_app(
        {
            'TESTING': True,
            'DATASETS': [
                {
                    'name': test_dataset,
                    'CV_SEGMENTATION_PATH': cv
                }
            ],
            'BIGTABLE_CONFIG': {
                'emulate': True
            }
        }
    )
    yield app


@pytest.fixture
def client(app):
    return app.test_client()
