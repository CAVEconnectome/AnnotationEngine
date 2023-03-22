import logging

import requests
from dynamicannotationdb.errors import (
    AnnotationInsertLimitExceeded,
    TableAlreadyExists,
    UpdateAnnotationError,
)
from flask import Response, abort, g, request, current_app
from flask_accepts import accepts
from flask_restx import Namespace, Resource, reqparse, inputs
from middle_auth_client import (
    auth_requires_admin,
    auth_requires_permission,
    users_share_common_group,
)

from caveclient.materializationengine import MaterializationClient
from caveclient.auth import AuthClient

from annotationengine.aligned_volume import (
    get_aligned_volumes,
    get_datastacks_from_aligned_volumes,
    get_datastack_info,
)
from annotationengine.anno_database import (
    get_db,
    check_write_permission,
    check_read_permission,
    check_ownership,
)
from annotationengine.errors import SchemaServiceError
from annotationengine.schemas import (
    CreateTableSchema,
    DeleteAnnotationSchema,
    FullMetadataSchema,
    PutAnnotationSchema,
    UpdateMetadataSchema,
)

from .api_examples import synapse_table_example, synapse_table_update_example

__version__ = "4.11.0"

authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

api_bp = Namespace(
    "Annotation Engine", authorizations=authorizations, description="Annotation Engine"
)


@api_bp.errorhandler
def handle_invalid_usage(error):
    return {"message": str(error)}, getattr(error, "code", 500)


annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument(
    "annotation_ids", type=int, action="split", help="list of annotation ids"
)

query_parser = reqparse.RequestParser()
query_parser.add_argument(
    "filter_valid",
    type=inputs.boolean,
    default=True,
    location="args",
    help="whether to only return valid items",
)


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


def get_schema_from_service(annotation_type, endpoint):
    url = f"{endpoint}/type/{annotation_type}"
    r = requests.get(url)
    if r.status_code != 200:
        raise (SchemaServiceError(r.text))
    return r.json()


def trigger_supervoxel_lookup(aligned_volume_name: str, table_name: str, inserted_ids: list):

    # look up datastacks with this
    datastacks = get_datastacks_from_aligned_volumes(aligned_volume_name)

    server = current_app.config["GLOBAL_SERVER"]
    auth = AuthClient(server_address=server)

    for datastack in datastacks:
        ds_info = get_datastack_info(datastack_name=datastack)
        local_server = ds_info["local_server"]
        # if not local server is configured we can't trigger this datastack
        if local_server is None:
            continue
        matclient = MaterializationClient(
            server_address=local_server,
            datastack_name=datastack,
            auth_client=auth,
            version=1,
        )
        try:
            matclient.lookup_supervoxel_ids(table_name,
             annotation_ids=inserted_ids, 
             datastack_name=datastack)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                logging.warning(
                    f"""Could not trigger lookup of table {table_name} in datastack {datastack} at server {local_server}.
Encountered status code {e.response.status_code} and message {e}"""
                )
            elif (e.response.status_code == 403):
                logging.warning(
                    f"""Permission error, could not trigger lookup of table {table_name} in datastack {datastack} at server {local_server}.
Encountered status code {e.response.status_code} and message {e}"""
                )
            else:
                raise (e)


@api_bp.route("/aligned_volume/<string:aligned_volume_name>/table")
class Table(Resource):
    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("create_table", security="apikey", example=synapse_table_example)
    @accepts("CreateTableSchema", schema=CreateTableSchema, api=api_bp)
    def post(self, aligned_volume_name: str):
        """Create a new annotation table"""
        check_aligned_volume(aligned_volume_name)
        data = request.parsed_obj
        db = get_db(aligned_volume_name)
        metadata_dict = data.get("metadata")
        logging.info(metadata_dict)
        description = metadata_dict.get("description")
        if metadata_dict.get("user_id", None) is None:
            metadata_dict["user_id"] = str(g.auth_user["id"])
        if description is None:
            msg = "Table description required"
            abort(404, msg)
        else:
            table_name = data.get("table_name")
            headers = None
            if not table_name.islower():
                headers = {
                    "Warning": f'201 - "Table name "{table_name}" needs to be lower case. Table will be posted to the database as "{table_name.lower()}"'
                }
                table_name = table_name.lower()
            schema_type = data.get("schema_type")
            table_name = table_name.lower().replace(" ", "_")
            try:
                table_info = db.annotation.create_table(
                    table_name, schema_type, **metadata_dict
                )
            except (TableAlreadyExists):
                abort(400, f"Table {table_name} already exists")
            except Exception as e:
                abort(400, str(e))
        return Response(table_info, headers=headers)

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("update_table", security="apikey", example=synapse_table_update_example)
    @accepts("UpdateMetadataSchema", schema=UpdateMetadataSchema, api=api_bp)
    def put(self, aligned_volume_name: str) -> FullMetadataSchema:
        data = request.parsed_obj
        db = get_db(aligned_volume_name)
        metadata_dict = data.get("metadata")
        if metadata_dict.get("user_id", None) is None:
            metadata_dict["user_id"] = str(g.auth_user["id"])

        table_name = data.get("table_name")
        check_ownership(db, table_name)

        new_md = db.annotation.update_table_metadata(table_name, **metadata_dict)
        return new_md, 200

    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("get_aligned_volume_tables", security="apikey")
    @api_bp.expect(query_parser)
    def get(self, aligned_volume_name: str):
        """Get list of annotation tables for a aligned_volume"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        args = query_parser.parse_args()
        tables = db.database._get_existing_table_names(
            filter_valid=args.get("filter_valid", True)
        )
        return tables, 200


@api_bp.route("/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>")
@api_bp.param("aligned_volume_name", "AlignedVolume Name")
@api_bp.param("table_name", "Name of table")
class AnnotationTable(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc(description="get table metadata", security="apikey")
    def get(self, aligned_volume_name: str, table_name: str) -> FullMetadataSchema:
        """Get metadata for a given table"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        md = db.database.get_table_metadata(table_name)
        headers = None
        if md.get("notice_text", None) is not None:
            headers = {"Warning": f"Table Owner Notice: {md['notice_text']}"}
        return md, 201, headers

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc(
        description="mark an annotation table for deletion",
        security="apikey",
    )
    def delete(self, aligned_volume_name: str, table_name: str) -> bool:
        """Delete an annotation table (marks for deletion, will suspend materialization, admin only)"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_ownership(db, table_name)
        is_deleted = db.annotation.delete_table(table_name)
        return is_deleted, 200


@api_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/count"
)
class TableInfo(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc(description="get_table_size", security="apikey")
    def get(self, aligned_volume_name: str, table_name: str) -> int:
        """Get count of rows of an annotation table"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_read_permission(db, table_name)
        return db.database.get_annotation_table_size(table_name), 200


@api_bp.route(
    "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/annotations"
)
class Annotations(Resource):
    @auth_requires_permission(
        "view", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("get annotations", security="apikey")
    @api_bp.expect(annotation_parser)
    def get(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Get annotations by list of IDs"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_read_permission(db, table_name)
        args = annotation_parser.parse_args()

        annotation_ids = args["annotation_ids"]

        md = db.database.get_table_metadata(table_name)
        headers = None
        if md.get("notice_text", None) is not None:
            headers = {"Warning": f"Table Owner Notice: {md['notice_text']}"}

        annotations = db.annotation.get_annotations(table_name, annotation_ids)

        if annotations is None:
            msg = f"annotation_id {annotation_ids} not in {table_name}"
            abort(404, msg)

        return annotations, 200, headers

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("post annotation", security="apikey")
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def post(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Insert annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_write_permission(db, table_name)
        data = request.parsed_obj
        annotations = data.get("annotations")

        try:
            inserted_ids = db.annotation.insert_annotations(table_name, annotations)
        except AnnotationInsertLimitExceeded as limit_error:
            logging.error(f"INSERT LIMIT EXCEEDED {limit_error}")
            abort(413, str(limit_error))
        except Exception as error:
            logging.error(f"INSERT FAILED {annotations}")
            abort(404, error)

        trigger_supervoxel_lookup(aligned_volume_name, table_name, inserted_ids)

        return inserted_ids, 200

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("update annotation", security="apikey")
    @accepts("PutAnnotationSchema", schema=PutAnnotationSchema, api=api_bp)
    def put(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Update annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_write_permission(db, table_name)
        data = request.parsed_obj

        annotations = data.get("annotations")

        new_ids = []

        for annotation in annotations:
            try:
                updated_id = db.annotation.update_annotation(table_name, annotation)
                new_ids.append(updated_id)
            except UpdateAnnotationError as update_error:
                abort(409, str(update_error))
            except Exception as error:
                abort(400, error)

        trigger_supervoxel_lookup(aligned_volume_name, table_name, new_ids)
        return new_ids, 200

    @auth_requires_permission(
        "edit", table_arg="aligned_volume_name", resource_namespace="aligned_volume"
    )
    @api_bp.doc("delete annotation", security="apikey")
    @accepts("DeleteAnnotationSchema", schema=DeleteAnnotationSchema, api=api_bp)
    def delete(self, aligned_volume_name: str, table_name: str, **kwargs):
        """Delete annotations"""
        check_aligned_volume(aligned_volume_name)
        db = get_db(aligned_volume_name)
        check_write_permission(db, table_name)

        data = request.parsed_obj

        ids = data.get("annotation_ids")
        deleted_ids = db.annotation.delete_annotation(table_name, ids)

        if deleted_ids is None:
            return f"annotation_id {ids} not in table {table_name}", 404

        return deleted_ids, 200
