import logging
from collections.abc import Iterable
from datetime import datetime
from json import JSONDecodeError
from typing import List

import numpy as np
import pandas as pd
from keboola.http_client import HttpClient
from pandas import json_normalize
from requests import Response

from hubspot_api import client_v3

COMPANIES_DEFAULT_COLS = ["additionalDomains", "companyId", "isDeleted", "mergeAudits", "portalId", "stateChanges"]
COMPANY_DEFAULT_PROPERTIES = ['about_us', 'name', 'phone', 'facebook_company_page', 'city', 'country', 'website',
                              'industry', 'annualrevenue', 'linkedin_company_page',
                              'hs_lastmodifieddate', 'hubspot_owner_id', 'notes_last_updated', 'description',
                              'createdate', 'numberofemployees', 'hs_lead_status', 'founded_year',
                              'twitterhandle',
                              'linkedinbio']

DEAL_DEFAULT_COLS = ["associations.associatedCompanyIds",
                     "associations.associatedDealIds",
                     "associations.associatedVids",
                     "dealId",
                     "imports",
                     "isDeleted",
                     "portalId",
                     "stateChanges"]

DEAL_DEFAULT_PROPERTIES = ["hs_object_id", 'authority', 'budget', 'campaign_source', 'hs_analytics_source',
                           'hs_campaign',
                           'hs_lastmodifieddate', 'need', 'timeframe', 'dealname', 'amount', 'closedate', 'pipeline',
                           'createdate', 'engagements_last_meeting_booked', 'dealtype', 'hs_createdate', 'description',
                           'start_date', 'closed_lost_reason', 'closed_won_reason', 'end_date', 'lead_owner',
                           'tech_owner', 'service_amount', 'contract_type',
                           'hubspot_owner_id',
                           'partner_name', 'notes_last_updated']

CONTACTS_DEFAULT_COLS = ["addedAt",
                         "canonical-vid",
                         "form-submissions",
                         "identity-profiles",
                         "is-contact",
                         "list-memberships",
                         "merge-audits",
                         "merged-vids",
                         "portal-id",
                         "profile-token",
                         "profile-url",
                         "vid"]

CONTACT_DEFAULT_PROPERTIES = ['hs_facebookid', 'hs_linkedinid', 'ip_city', 'ip_country',
                              'ip_country_code', 'newsletter_opt_in', 'linkedin_profile',
                              'email', 'mobilephone', 'phone', 'city',
                              'country', 'region', 'jobtitle', 'website', 'numemployees',
                              'industry', 'associatedcompanyid', 'hs_lead_status', 'lastmodifieddate',
                              'source', 'hs_email_optout', 'twitterhandle', 'lead_type',
                              'hubspot_owner_id', 'notes_last_updated', 'hs_analytics_source', 'opt_in',
                              'createdate', 'hs_twitterid', 'lifecyclestage']

LISTS_COLS = ['archived', 'authorId', 'createdAt', 'deleteable', 'dynamic', 'filters',
              'internalListId', 'listId', 'listType', 'metaData.error',
              'metaData.lastProcessingStateChangeAt', 'metaData.lastSizeChangeAt',
              'metaData.listReferencesCount', 'metaData.parentFolderId',
              'metaData.processing', 'metaData.size', 'name', 'portalId', 'readOnly',
              'updatedAt']

EMAIL_EVENTS_COLS = ['appId', 'appName', 'browser', 'browser.family', 'browser.name', 'browser.producer',
                     'browser.producerUrl', 'browser.type', 'browser.url', 'browser.version', 'causedBy.created',
                     'causedBy.id', 'created', 'deviceType', 'duration', 'emailCampaignId', 'filteredEvent', 'id',
                     'ipAddress', 'location', 'location.city', 'location.country', 'location.state', 'portalId',
                     'recipient', 'sentBy.created', 'sentBy.id', 'smtpId', 'type', 'userAgent']

ENGAGEMENTS_COLS = ['metadata.isBot', 'metadata.endTime', 'metadata.postSendStatus', 'associations.quoteIds',
                    'metadata.from.raw', 'engagement.createdBy', 'metadata.to',
                    'metadata.agentResponseTimeMilliseconds', 'metadata.visitorStartTime', 'metadata.messageId',
                    'metadata.durationMilliseconds', 'metadata.externalUrl', 'engagement.source',
                    'associations.contactIds', 'engagement.lastUpdated', 'metadata.body', 'metadata.forObjectType',
                    'metadata.categoryId', 'metadata.sessionClosedAt', 'metadata.visitorEndTime', 'metadata.from.email',
                    'metadata.recordingUrl', 'associations.ticketIds', 'associations.contentIds',
                    'metadata.fromfirstName', 'metadata.numVisitorMessages', 'engagement.ownerId',
                    'metadata.facsimileSendId', 'metadata.createdFromLinkId', 'metadata.sessionDurationMilliseconds',
                    'metadata.startTime', 'metadata.subject', 'associations.ownerIds', 'associations.dealIds',
                    'metadata.html', 'metadata.source', 'metadata.sendDefaultReminder', 'engagement.createdAt',
                    'engagement.sourceId', 'metadata.category', 'metadata.fromemail', 'metadata.sender.email',
                    'metadata.online', 'metadata.from.lastName', 'engagement.uid', 'engagement.allAccessibleTeamIds',
                    'metadata.from.firstName', 'metadata.text', 'metadata.conversationSource', 'metadata.toNumber',
                    'associations.workflowIds', 'metadata.sentVia', 'attachments', 'metadata.numAgentMessages',
                    'metadata.url', 'metadata.agentJoinTime', 'engagement.id', 'engagement.type',
                    'associations.companyIds', 'metadata.title', 'metadata.disposition', 'metadata.state',
                    'engagement.modifiedBy', 'metadata.fromlastName', 'metadata.externalId', 'metadata.sourceId',
                    'metadata.fromNumber', 'metadata.cc', 'metadata.externalAccountId',
                    'metadata.visitorWaitTimeMilliseconds', 'engagement.portalId', 'metadata.trackerKey',
                    'metadata.preMeetingProspectReminders', 'metadata.attachedVideoOpened',
                    'metadata.validationSkipped', 'metadata.loggedFrom', 'metadata.mediaProcessingStatus',
                    'metadata.threadId', 'metadata.reminders', 'metadata.status', 'metadata.name',
                    'engagement.timestamp', 'metadata.contentId', 'metadata.campaignGuid', 'metadata.taskType',
                    'metadata.bcc', 'scheduledTasks', 'engagement.active', 'metadata.attachedVideoWatched',
                    'engagement.teamId', 'metadata.to.email', 'metadata.calleeObjectId', 'metadata.calleeObjectType',
                    'metadata.emailSendEventId.created', 'metadata.emailSendEventId.id', 'metadata.errorMessage']

CAMPAIGNS = 'email/public/v1/campaigns/'

LISTS = 'contacts/v1/lists'

ENGAGEMENTS_PAGED = 'engagements/v1/engagements/paged'
ENGAGEMENTS_PAGED_SINCE = 'engagements/v1/engagements/recent/modified'

EMAIL_EVENTS = 'email/public/v1/events'

CAMPAIGNS_BY_ID = 'email/public/v1/campaigns/by-id'
CAMPAIGNS_BY_ID_RECENT = 'email/public/v1/campaigns'

DEALS_ALL = 'deals/v1/deal/paged'
DEALS_RECENT = 'deals/v1/deal/recent/modified'

COMPANIES_ALL = 'companies/v2/companies/paged'
COMPANIES_RECENT = 'companies/v2/companies/recent/modified'

MAX_RETRIES = 10
BASE_URL = 'https://api.hubapi.com/'

# endpoints
CONTACTS_ALL = 'contacts/v1/lists/all/contacts/all'
CONTACTS_RECENT = 'contacts/v1/lists/recently_updated/contacts/recent'

COMPANY_PROPERTIES = 'properties/v1/companies/properties/'


class HubspotClientService(HttpClient):

    def __init__(self, token, authentication_type: str = "API Key"):
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
        self._client_v3 = client_v3.ClientV3(token, authentication_type)

    def _parse_response_text(self, response: Response, endpoint, parameters) -> dict:
        try:
            return response.json()
        except JSONDecodeError as e:
            charp = str(e).split('(char ')
            start_pos = 0
            if len(charp) == 2:
                start_pos = int(charp[1].replace(')', '')) - 100
                if start_pos < 0:
                    start_pos = 0
            raise RuntimeError(f'The HS API response is invalid. enpoint: {endpoint}, parameters: {parameters}. '
                               f'Status: {response.status_code}. '
                               f'Response: {response.text[start_pos:start_pos + 100]}... {e}')

    def _get_paged_result_pages(self, endpoint, parameters, res_obj_name, limit_attr, offset_req_attr, offset_resp_attr,
                                has_more_attr, offset, limit, default_cols=None):

        has_more = True
        while has_more:
            final_df = pd.DataFrame()
            parameters[offset_req_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            self._check_http_result(req, endpoint)
            req_response = self._parse_response_text(req, endpoint, parameters)
            if req_response.get(has_more_attr):
                has_more = True
                offset = req_response[offset_resp_attr]
            else:
                has_more = False
            if req_response.get(res_obj_name):
                final_df = final_df.append(json_normalize(req_response[res_obj_name]), sort=True)
            else:
                logging.debug(f'Empty response {req_response}')
            if default_cols and not final_df.empty:
                # dedupe
                default_cols = list(set(default_cols))
                final_df = final_df.reindex(columns=default_cols).fillna('')
                # final_df = final_df.loc[:, default_cols].fillna('')
            # sort cols
            final_df = final_df.reindex(sorted(final_df.columns), axis=1)
            yield final_df

    def _get_paged_result_pages_dict(self, endpoint, parameters, res_obj_name, limit_attr, offset_req_attr,
                                     offset_resp_attr,
                                     has_more_attr, offset, limit, default_cols=None):

        has_more = True
        while has_more:
            final_result = {}
            parameters[offset_req_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            self._check_http_result(req, endpoint)
            req_response = self._parse_response_text(req, endpoint, parameters)

            if req_response.get(has_more_attr) or not req_response.get(res_obj_name):
                has_more = True
                offset = req_response[offset_resp_attr]
            else:
                has_more = False
            if req_response.get(res_obj_name):
                final_result = req_response.get(res_obj_name)
            else:
                logging.debug(f'Empty response {req_response}')

            yield final_result

    def _get_contact_recent_pages(self, parameters, since_time_offset, limit, default_cols=None):
        """
        Recent contacts enpoint paginates backwards, from time offset back to 30 day ago.
        This simulates the expected behaviour -> gets data from now until the point in time

        :param parameters:
        :param since_time_offset:
        :param limit:
        :param default_cols:
        :return:
        """
        res_obj_name = 'contacts'
        endpoint = CONTACTS_RECENT
        # start from today
        timeoffset = int(datetime.utcnow().timestamp() * 1000)

        has_more = True
        while has_more:
            final_df = pd.DataFrame()
            parameters['timeOffset'] = timeoffset
            parameters['count'] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            self._check_http_result(req, endpoint)
            req_response = self._parse_response_text(req, endpoint, parameters)
            timeoffset = req_response.get('time-offset', since_time_offset)

            if req_response.get('has-more') and timeoffset >= since_time_offset:
                has_more = True
            else:
                has_more = False

            if req_response.get(res_obj_name):
                final_df = final_df.append(json_normalize(req_response[res_obj_name]), sort=True)
            else:
                logging.debug(f'Empty response {req_response}')
            if default_cols and not final_df.empty:
                # dedupe
                default_cols = list(set(default_cols))
                final_df = final_df.reindex(columns=default_cols).fillna('')
                # final_df = final_df.loc[:, default_cols].fillna('')
            # sort cols
            final_df = final_df.reindex(sorted(final_df.columns), axis=1)
            yield final_df

    def _check_http_result(self, response, endpoint):
        http_error_msg = ''
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
        if 401 == response.status_code:
            http_error_msg = u'Failed to login: %s - Please check your API token' % (reason)
        elif 401 < response.status_code < 500:
            http_error_msg = u'Request to %s failed %s Client Error: %s' % (endpoint, response.status_code, reason)

        elif 500 <= response.status_code < 600:
            http_error_msg = u'Request to %s failed %s Client Error: %s' % (endpoint, response.status_code, reason)

        if http_error_msg:
            raise RuntimeError(http_error_msg, response.text)

    def get_contacts(self, property_attributes, start_time=None, fields=None,
                     show_list_membership: bool = True) -> Iterable:
        """
        Get either all available contacts or recent ones specified by start_time.

        API supports more options, possible to extend in the future
        :type fields: list list of contact properties to get
        :param start_time: datetime
        :return: generator object with all available pages
        """
        offset = -1

        if not fields:
            contact_properties = CONTACT_DEFAULT_PROPERTIES
            expected_contact_cols = CONTACTS_DEFAULT_COLS + self._build_property_cols(
                CONTACTS_DEFAULT_COLS, property_attributes)
        else:
            contact_properties = fields
            expected_contact_cols = CONTACTS_DEFAULT_COLS + self._build_property_cols(fields, property_attributes)

        parameters = {'property': contact_properties, 'formSubmissionMode': 'all',
                      'showListMemberships': show_list_membership}

        # hubspot_api api allows only 30 days back, get all data if larger
        if start_time and (datetime.utcnow() - start_time).days >= 30:
            start_time = None

        parameters['propertyMode'] = 'value_and_history'
        if start_time:
            logging.info('Getting contacts using incremental endpoint (<30 days ago)')
            return self._get_contact_recent_pages(parameters, int(start_time.timestamp() * 1000), 100,
                                                  default_cols=expected_contact_cols)
        else:
            logging.info('Getting ALL contacts using "full scan" endpoint (period >30 days ago)')
            return self._get_paged_result_pages(CONTACTS_ALL, parameters, 'contacts', 'count', 'vidOffset',
                                                'vid-offset', 'has-more', offset, 100,
                                                default_cols=expected_contact_cols)

    def get_companies(self, property_attributes, recent=None, fields=None):

        offset = 0
        if not fields:
            company_properties = COMPANY_DEFAULT_PROPERTIES
            expected_company_cols = COMPANIES_DEFAULT_COLS + self._build_property_cols(
                COMPANY_DEFAULT_PROPERTIES, property_attributes)
        else:
            company_properties = fields
            expected_company_cols = COMPANIES_DEFAULT_COLS + self._build_property_cols(fields, property_attributes)

        parameters = {'properties': company_properties}

        if property_attributes['include_versions']:
            parameters['propertiesWithHistory'] = company_properties

        # # hubspot_api api allows only 30 days back, get all data if larger
        # if start_time and (datetime.utcnow() - start_time).days >= 30:
        #     recent = False
        # else:
        #     recent = True

        if recent:
            return self._get_paged_result_pages(COMPANIES_RECENT, parameters, 'results', 'count', 'offset',
                                                'offset',
                                                'hasMore',
                                                offset, 1000, default_cols=expected_company_cols)
        else:
            return self._get_paged_result_pages(COMPANIES_ALL, parameters, 'companies', 'limit', 'offset',
                                                'offset',
                                                'has-more', offset, 250, default_cols=expected_company_cols)

    def get_company_properties(self):
        req = self.get_raw(self.base_url + COMPANY_PROPERTIES)
        self._check_http_result(req, COMPANY_PROPERTIES)
        req_response = req.json()
        return req_response

    def _build_property_cols(self, properties, property_attributes):
        # get flattened property cols
        prop_cols = []
        for p in properties:
            if property_attributes.get('include_source', True):
                prop_cols.append('properties.' + p + '.source')
                prop_cols.append('properties.' + p + '.sourceId')
            if property_attributes.get('include_timestamp', True):
                prop_cols.append('properties.' + p + '.timestamp')
            if property_attributes.get('include_versions', True):
                prop_cols.append('properties.' + p + '.versions')

            prop_cols.append('properties.' + p + '.value')
        return prop_cols

    def _build_contact_property_cols(self, properties):
        # get flattened property cols
        prop_cols = []
        for p in properties:
            prop_cols.append('properties.' + p + '.value')
            prop_cols.append('properties.' + p + '.versions')
        return prop_cols

    def get_deals(self, property_attributes, start_time=None, fields=None) -> Iterable:
        """
        Get either all available deals or recent ones specified by start_time.

        API supports more options, possible to extend in the future
        :type fields: list list of deal properties to get
        :param start_time: datetime
        :return: generator object with all available pages
        """
        offset = 0
        if not fields:
            deal_properties = DEAL_DEFAULT_PROPERTIES
            expected_deal_cols = DEAL_DEFAULT_COLS + self._build_property_cols(
                DEAL_DEFAULT_PROPERTIES, property_attributes)
        else:
            if 'dealstage' in fields:
                fields.remove('dealstage')
            deal_properties = fields
            expected_deal_cols = DEAL_DEFAULT_COLS + self._build_property_cols(fields, property_attributes)

        property_attributes['include_versions'] = True
        expected_deal_cols += self._build_property_cols(['dealstage'], property_attributes)
        parameters = {'properties': deal_properties,
                      'propertiesWithHistory': 'dealstage',
                      'includeAssociations': 'true'}
        if start_time:
            parameters['since'] = int(start_time.timestamp() * 1000)
            return self._get_paged_result_pages(DEALS_RECENT, parameters, 'results', 'count', 'offset',
                                                'offset',
                                                'hasMore',
                                                offset, 100, default_cols=expected_deal_cols)
        else:
            return self._get_paged_result_pages(DEALS_ALL, parameters, 'deals', 'limit', 'offset', 'offset',
                                                'hasMore',
                                                offset, 250, default_cols=expected_deal_cols)

    def get_campaigns(self, recent=False):
        final_df = pd.DataFrame()
        if recent:
            url = CAMPAIGNS_BY_ID_RECENT
        else:
            url = CAMPAIGNS_BY_ID

        for res in self._get_paged_result_pages(url, {}, 'campaigns', 'limit', 'offset', 'offset', 'hasMore',
                                                None,
                                                1000):

            for index, row in res.iterrows():
                req = self.get_raw(self.base_url + CAMPAIGNS + str(row['id']))
                self._check_http_result(req, CAMPAIGNS)
                req_response = req.json()

                final_df = final_df.append(json_normalize(req_response), sort=True)
            # add missing cols
            columns = ['counters.open', 'counters.click', 'id', 'name', 'counters.delivered',
                       'counters.processed', 'counters.sent', 'lastProcessingFinishedAt',
                       'lastProcessingStartedAt', 'lastProcessingStateChangeAt',
                       'numIncluded', 'processingState', 'subject', 'type', 'appId', 'appName', 'contentId', ]

            if not final_df.empty:
                for c in columns:
                    if c not in final_df:
                        final_df[c] = np.nan

            yield final_df if final_df.empty else final_df[columns]

    def get_email_events(self, start_date: datetime, events_list: list) -> Iterable:
        offset = ''
        timestamp = None
        if start_date:
            timestamp = int(start_date.timestamp() * 1000)

        for event in events_list:
            logging.info(f"Getting {event} events.")
            parameters = {'eventType': event, 'startTimestamp': timestamp}
            for open_ev in self._get_paged_result_pages(EMAIL_EVENTS, parameters, 'events', 'limit', 'offset',
                                                        'offset',
                                                        'hasMore',
                                                        offset, 1000, default_cols=EMAIL_EVENTS_COLS):
                yield open_ev

    def get_activities(self, start_time: datetime) -> Iterable:
        offset = 0

        if start_time:
            pages = self._get_paged_result_pages(ENGAGEMENTS_PAGED_SINCE, {"since": int(start_time.timestamp() * 1000)},
                                                 'results', 'count', 'offset', 'offset', 'hasMore', offset, 250,
                                                 default_cols=ENGAGEMENTS_COLS)
        else:
            pages = self._get_paged_result_pages(ENGAGEMENTS_PAGED, {}, 'results', 'limit', 'offset', 'offset',
                                                 'hasMore',
                                                 offset, 250, default_cols=ENGAGEMENTS_COLS)
        for pg_res in pages:
            if 'metadata.text' in pg_res.columns:
                pg_res.drop(['metadata.text'], 1)
            if 'metadata.html' in pg_res.columns:
                pg_res.drop(['metadata.html'], 1)
            yield pg_res

    def get_lists(self):
        offset = 0

        return self._get_paged_result_pages(LISTS, {}, 'lists', 'limit', 'offset', 'offset', 'has-more',
                                            offset, 250, default_cols=LISTS_COLS)

    def get_pipelines(self, include_inactive=None):
        final_df = pd.DataFrame()

        req = self.get_raw('https://api.hubapi.com/deals/v1/pipelines', params={'include_inactive': include_inactive})
        self._check_http_result(req, 'deals/pipelines')
        req_response = req.json()

        final_df = final_df.append(json_normalize(req_response), sort=True)

        return [final_df]

    def get_owners(self, include_inactive=True):
        final_df = pd.DataFrame()

        req = self.get_raw('https://api.hubapi.com/owners/v2/owners/', params={'include_inactive': include_inactive})
        self._check_http_result(req, 'owners')
        req_response = req.json()

        final_df = final_df.append(json_normalize(req_response), sort=True)

        return [final_df]

    def get_email_statistics(self, include_inactive=True):

        resp = self._get_paged_result_pages_dict('marketing-emails/v1/emails/with-statistics', {}, 'objects', 'limit',
                                                 'offset', 'offset', 'hasmore', 0, 300)

        return resp

    def get_v3_engagement_object(self, object_type: str, properties: List[str] = None):
        return self._client_v3.get_engagement_object(object_type, properties)

    def get_forms(self):
        return self._client_v3.get_forms()

    def get_associations(self, from_object_type: str, to_object_type: str, ids: List[str]):
        """

               Args:
                   from_object_type: e.g. company, contact
                   to_object_type: e.g. company, contact
                   ids: List of IDs of from_object type

               Returns: Result as dict

               """
        return self._client_v3.get_associations(from_object_type, to_object_type, ids)
