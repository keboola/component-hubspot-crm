import json
import logging
from enum import Enum
from typing import List, Union, Iterator

from keboola.http_client import HttpClient

MAX_RETRIES = 10
BASE_URL = 'https://api.hubapi.com/'


class EngagementObjects(Enum):
    calls = "calls"
    emails = "emails"
    meetings = "meetings"
    notes = "notes"
    tasks = "tasks"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.name, cls))

    @classmethod
    def validate_fields(cls, fields: List[str]):
        errors = []
        for f in fields:
            if f not in cls.list():
                errors.append(f'"{f}" is not valid {cls.__name__} value!')
        if errors:
            raise ValueError(
                ', '.join(errors) + f'\n Supported {cls.__name__} values are: [{cls.list()}]')

    @classmethod
    def validate_field(cls, field: str):
        if field not in cls.list():
            raise ValueError(
                f'{field}" is not valid {cls.__name__} value! Supported {cls.__name__} values are: [{cls.list()}]')
        return True


class ClientV3(HttpClient):

    def __init__(self, token, authentication_type):
        """

        Args:
            token:
            authentication_type: "API Key" or "Private App Token"
        """
        if authentication_type == "API Key":
            default_params = {"hapikey": token}
            auth_header = {}
        else:
            default_params = {}
            auth_header = {'Authorization': f'Bearer {token}'}
        HttpClient.__init__(self, base_url=BASE_URL, max_retries=MAX_RETRIES, backoff_factor=0.3,
                            status_forcelist=(429, 500, 502, 504, 524), default_params=default_params,
                            auth_header=auth_header)

    def _get_paged_result_pages(self, endpoint, parameters, limit=100, default_cols=None) -> Iterator[List[dict]]:

        has_more = True
        while has_more:
            parameters['limit'] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            self._check_http_result(req, endpoint)
            resp_text = str.encode(req.text, 'utf-8')
            req_response = json.loads(resp_text)

            if req_response.get('paging', {}).get('next', {}).get('after'):
                has_more = True
                after = req_response['paging']['next']['after']
                parameters['after'] = after
            else:
                has_more = False

            results = []
            if req_response.get('results'):
                results = req_response['results']
            else:
                logging.debug(f'Empty response {req_response}')

            yield results

    def _check_http_result(self, response, endpoint):
        http_error_msg = ''
        error_detail = ''
        if isinstance(response.reason, bytes):
            # We attempt to decode utf-8 first because some servers
            # choose to localize their reason strings. If the string
            # isn't utf-8, we fall back to iso-8859-1 for all other
            # encodings. (See PR #3538)
            try:
                reason = response.reason.decode('utf-8')

            except UnicodeDecodeError:
                reason = response.reason.decode('iso-8859-1')
        else:
            reason = response.reason

        if response.status_code >= 400:
            error_detail = response.json()['message'] + f' Errors: {response.json()["errors"]}'
        if 401 == response.status_code:
            http_error_msg = f'Failed to login: {reason} - Please check your API token'
        elif 401 < response.status_code < 500:
            http_error_msg = f'Request to {endpoint} failed {response.status_code} Client Error: {reason} ' \
                             f'Detail: {error_detail}'

        elif 500 <= response.status_code < 600:
            http_error_msg = f'Request to {endpoint} failed {response.status_code} Client Error: {reason} ' \
                             f'Detail: {error_detail}'

        if http_error_msg:
            raise RuntimeError(http_error_msg)

    def get_forms(self, archived: bool = False, form_types: List[str] = None) -> Iterator[List[dict]]:
        request_params = {"archived": archived}
        if form_types:
            form_types_str = ','.join(form_types)
            request_params['formTypes'] = form_types_str

        return self._get_paged_result_pages('marketing/v3/forms', request_params)

    def get_engagement_calls(self, properties: List[str] = None):

        request_params = {}
        if properties:
            properties_str = ','.join(properties)
            request_params['properties'] = properties_str

        return self._get_paged_result_pages('crm/v3/objects/calls', request_params)

    def get_engagement_object(self, object_type: Union[EngagementObjects, str], properties: List[str] = None):

        if isinstance(object_type, str):
            EngagementObjects.validate_field(object_type)
        elif isinstance(object_type, EngagementObjects):
            object_type = object_type.value

        request_params = {}
        if properties:
            properties_str = ','.join(properties)
            request_params['properties'] = properties_str

        return self._get_paged_result_pages(f'crm/v3/objects/{object_type}', request_params)

    def get_associations(self, from_object_type: str, to_object_type: str, ids: List[str]) -> dict:
        """

        Args:
            from_object_type: e.g. company, contact
            to_object_type: e.g. company, contact
            ids: List of IDs of from_object type

        Returns: Result as dict

        """
        body = {'inputs': [{"id": id_value} for id_value in ids]}

        resp = self.post_raw(f'/crm/v4/associations/{from_object_type}/{to_object_type}/batch/read', json=body)

        resp.raise_for_status()

        return resp.json()['results']
