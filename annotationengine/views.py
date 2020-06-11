from flask import jsonify, render_template, current_app, make_response, Blueprint
from annotationinfoservice.datasets.models import DataSet, DataSetV2, PermissionGroup, TableMapping
from nglui.statebuilder import *


__version__ = "0.4.0"

views_bp = Blueprint('datasets', __name__, url_prefix='/datasets')


@views_bp.route("/")
def index():
    datasets = DataSetV2.query.all()
    return render_template('datasets.html',
                            datasets=datasets,
                            version=__version__)


@views_bp.route("/dataset/<datasetname>")
def dataset_view(datasetname):
    dataset = DataSetV2.query.filter(DataSetV2.name == datasetname).first_or_404()
    
    img_layer = ImageLayerConfig(name='layer23',
                                    source=dataset.image_source,
                                    )
    # we want the segmentation layer with our target neuron always on
    seg_layer = SegmentationLayerConfig(name = 'seg',
                                        source=dataset.segmentation_source)
    ann_layer = AnnotationLayerConfig(name='ann')
                                
    # setup a state builder with this layer pipeline
    sb = StateBuilder([img_layer, seg_layer, ann_layer])
    
    if dataset.viewer_site is not None:
        site = dataset.viewer_site
    else:
        site = current_app.config['NEUROGLANCER_URL']
    ng_url=sb.render_state(return_as='url', url_prefix = site)

    return render_template('dataset.html',
                            dataset=dataset,
                            ng_url=ng_url,
                            version=__version__)

