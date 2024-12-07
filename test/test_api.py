from flask import g
import json
import logging
import sys
from unittest import mock

sys.modules["annotationengine.api.check_aligned_volume"] = mock.MagicMock()

aligned_volume_name = "test_aligned_volume"

class TestHealthEndpoint:
    def test_health_endpoint(self, client):
        url = "annotation/api/versions"
        response = client.get(url)
        logging.info(response)
        assert response.json == [2]


class TestTableEndpoints:
    def test_post_table(self, client):
        logging.info("TEST")
        data = {
            "table_name": "test_table",
            "schema_type": "synapse",
            "metadata": {
                "user_id": "1",
                "description": "Test description",
                "flat_segmentation_source": "precomputed://gs://my_cloud_bucket/image",
                "voxel_resolution_x": 4,
                "voxel_resolution_y": 4,
                "voxel_resolution_z": 40,
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.post(
                url,
                json=data,
                content_type="application/json",
                follow_redirects=True,
            )
            logging.info(response)
            assert response.json is None

    def test_put_table(self, client, modify_g):
        '''
        These tests have to be run in sequential order, so instead of trusting
        the testing framework to run independent tests in the intended order,
        simply package them into a single function.
        '''
        
        # Alter the description and populate the notice_text
        logging.info("TEST")
        data = {
            "table_name": "test_table",
            "metadata": {
                "user_id": "1",
                "description": "Altered test description",
                "flat_segmentation_source": "precomputed://gs://my_cloud_bucket/image",
                "notice_text": "Notice",
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.put(
                url,
                json=data,
                content_type="application/json",
                follow_redirects=True,
            )
            logging.info(response)
            assert response.json['description'] == "Altered test description"
            assert response.json['notice_text'] == "Notice"
        
        # ================================================================================
        # Test that notice_text is erased
        logging.info("TEST")
        data = {
            "table_name": "test_table",
            "metadata": {
                "user_id": "1",
                "notice_text": "",
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.put(
                url,
                json=data,
                content_type="application/json",
                follow_redirects=True,
            )
            logging.info(response)
            assert response.json['notice_text'] is None

        # ================================================================================
        # Repopulate notice_text
        logging.info("TEST")
        data = {
            "table_name": "test_table",
            "metadata": {
                "user_id": "1",
                "notice_text": "Notice2",
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.put(
                url,
                json=data,
                content_type="application/json",
                follow_redirects=True,
            )
            logging.info(response)
            assert response.json['notice_text'] == "Notice2"
        
        # ================================================================================
        # Test that notice_text is erased
        logging.info("TEST")
        data = {
            "table_name": "test_table",
            "metadata": {
                "user_id": "1",
                "notice_text": "",
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.put(
                url,
                json=data,
                content_type="application/json",
                follow_redirects=True,
            )
            logging.info(response)
            assert response.json['notice_text'] is None

    def test_post_table_to_be_deleted(self, client):
        logging.info(client)
        data = {
            "table_name": "test_table_to_delete",
            "schema_type": "synapse",
            "metadata": {
                "user_id": "1",
                "description": "Test description",
                "flat_segmentation_source": "precomputed://gs://my_cloud_bucket/image",
                "voxel_resolution_x": 4,
                "voxel_resolution_y": 4,
                "voxel_resolution_z": 40,
            },
        }
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            response = client.post(
                url,
                data=json.dumps(data),
                content_type="application/json",
                follow_redirects=False,
            )
            logging.info(response)

    def test_get_tables(self, client):
        logging.info(client)

        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            response = client.get(
                url, content_type="application/json",
                follow_redirects=False
            )
            logging.info(response)
            
            assert response.json == ["test_table", "test_table_to_delete"]


class TestAnnotationTableEndpoints:
    def test_get_table_metadata(self, client):

        url = (
            f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table"
        )

        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.get(url,
                                  follow_redirects=False
                                  )
            logging.info(response.json)

            metadata = {
                "table_name": "test_table",
                "id": 1,
                "deleted": None,
                "description": "Altered test description",
                "flat_segmentation_source": "precomputed://gs://my_cloud_bucket/image",
                "voxel_resolution_y": 4.0,
                "valid": True,
                "schema_type": "synapse",
                "user_id": "1",
                "notice_text": None,
                "reference_table": None,
                "voxel_resolution_x": 4.0,
                "voxel_resolution_z": 40.0,
                "read_permission": "PUBLIC",
                "write_permission": "PRIVATE",
            }
            response_json = response.json
            del response_json['created']
            del response_json['last_modified']
            assert response_json == metadata

    def test_mark_table_to_delete(self, client, modify_g):

        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table_to_delete"
        
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name

            response = client.delete(url,
                                    follow_redirects=False
                                    )
            logging.info(response.json)
            assert response.json is True


class TestTableInfo:
    def test_get_row_count(self, client):
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table/count"
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            response = client.get(url,
                                  follow_redirects=False
                                  )
            logging.info(response.json)
            assert response.json == 0


class TestAnnotationsEndpoints:
    def test_post_annotations(self, client, modify_g):

        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table/annotations"
        data = {
            "annotations": [
                {
                    "pre_pt": {"position": [31, 31, 0]},
                    "ctr_pt": {"position": [32, 32, 0]},
                    "post_pt": {"position": [33, 33, 0]},
                    "size": 1,
                },
            ]
        }
        auth_user = mock.MagicMock()
        auth_user.name = "aa"
        auth_user.id = 1

        with mock.patch(
            "annotationengine.api.check_aligned_volume",
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            with mock.patch(
                "annotationengine.api.trigger_supervoxel_lookup",
            ) as mock_trigger_supervoxel_lookup:
                mock_trigger_supervoxel_lookup.return_value = None
                
                response = client.post(
                    url,
                    data=json.dumps(data),
                    content_type="application/json",
                    follow_redirects=False,
                )
                logging.info(response)
                assert response.json == [1]

    def test_get_annotations(self, client):
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table/annotations"
        data = {"annotation_ids": "1"}
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            response = client.get(url, query_string=data,
                                  follow_redirects=False
                                  )
            logging.info(response)
            logging.info(response.json)
            logging.info(response.json[0])
            assert response.json[0]["id"] == 1
            assert response.json[0]["pre_pt_position"] == [31, 31, 0]
            assert response.json[0]["ctr_pt_position"] == [32, 32, 0]
            assert response.json[0]["post_pt_position"] == [33, 33, 0]
            assert response.json[0]["valid"] == True
            assert response.json[0]["superceded_id"] == None
            assert response.json[0]["deleted"] == 'None'

    def test_update_annotations(self, client):
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table/annotations"
        data = {
            "annotations": [
                {
                    "id": 1,
                    "pre_pt": {"position": [22, 22, 0]},
                    "ctr_pt": {"position": [32, 32, 0]},
                    "post_pt": {"position": [33, 33, 0]},
                    "size": 1,
                },
            ]
        }
        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            with mock.patch(
                "annotationengine.api.trigger_supervoxel_lookup",
            ) as mock_trigger_supervoxel_lookup:
                mock_trigger_supervoxel_lookup.return_value = None
                response = client.put(url, data=json.dumps(data),
                                   follow_redirects=False
                                   )
                logging.info(response)
                assert response.json == {'1': 2}

    def test_delete_annotations(self, client):
        url = f"/annotation/api/v2/aligned_volume/{aligned_volume_name}/table/test_table/annotations"
        data = {"annotation_ids": [2]}

        with mock.patch(
            "annotationengine.api.check_aligned_volume"
        ) as mock_aligned_volumes:
            mock_aligned_volumes.return_value = aligned_volume_name
            response = client.delete(url, data=json.dumps(data),
                                     follow_redirects=False
                                     )
            logging.info(response)
            assert response.json == [2]
