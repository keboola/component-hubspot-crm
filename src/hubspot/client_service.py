import json
from _datetime import timedelta
from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from kbc.client_base import HttpClientBase
from pandas.io.json import json_normalize

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
                     "properties.dealstage.source",
                     "properties.dealstage.sourceId",
                     "properties.dealstage.timestamp",
                     "properties.dealstage.value",
                     "properties.dealstage.versions",
                     "properties.hs_object_id.source",
                     "properties.hs_object_id.sourceId",
                     "properties.hs_object_id.timestamp",
                     "properties.hs_object_id.value",
                     "properties.hs_object_id.versions",
                     "stateChanges"]
DEAL_DEFAULT_PROPERTIES = ['authority', 'budget', 'campaign_source', 'hs_analytics_source', 'hs_campaign',
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
                         "properties.company.value",
                         "properties.company.versions",
                         "properties.firstname.value",
                         "properties.firstname.versions",
                         "properties.lastmodifieddate.value",
                         "properties.lastmodifieddate.versions",
                         "properties.lastname.value",
                         "properties.lastname.versions",
                         "vid"]

CONTACT_DEFAULT_PROPERTIES = ['hs_facebookid', 'hs_linkedinid', 'ip_city', 'ip_country',
                              'ip_country_code', 'newsletter_opt_in', 'firstname', 'linkedin_profile',
                              'lastname', 'email', 'mobilephone', 'phone', 'city',
                              'country', 'region', 'jobtitle', 'company', 'website', 'numemployees',
                              'industry', 'associatedcompanyid', 'hs_lead_status', 'lastmodifieddate',
                              'source', 'hs_email_optout', 'twitterhandle', 'lead_type',
                              'hubspot_owner_id', 'notes_last_updated', 'hs_analytics_source', 'opt_in',
                              'createdate', 'hs_twitterid', 'lifecyclestage']


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


class HubspotClientService(HttpClientBase):

    def __init__(self, token):
        HttpClientBase.__init__(self, base_url=BASE_URL, max_retries=MAX_RETRIES, backoff_factor=0.3,
                                status_forcelist=(429, 500, 502, 504), default_params={"hapikey": token})

    def _get_paged_result_pages(self, endpoint, parameters, res_obj_name, limit_attr, offset_req_attr, offset_resp_attr,
                                has_more_attr, offset, limit, default_cols=None):

        has_more = True
        while has_more:
            final_df = pd.DataFrame()
            parameters[offset_req_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            resp_text = str.encode(req.text, 'utf-8')
            req_response = json.loads(resp_text)

            if req_response[has_more_attr]:
                has_more = True
            else:
                has_more = False
            offset = req_response[offset_resp_attr]
            final_df = final_df.append(json_normalize(req_response[res_obj_name]))
            if default_cols:
                final_df = final_df.loc[:, default_cols].fillna('')
            yield final_df

    def _get_all_pages_result(self, endpoint, parameters, res_obj_name, limit_attr, offset_attr, has_more_attr, offset,
                              limit):
        final_df = pd.DataFrame()

        has_more = True
        while has_more:
            parameters[offset_attr] = offset
            parameters[limit_attr] = limit

            req = self.get_raw(self.base_url + endpoint, params=parameters)
            resp_text = str.encode(req.text, 'utf-8')
            req_response = json.loads(resp_text)

            if req_response[has_more_attr]:
                has_more = True
            else:
                has_more = False
            offset = req_response[offset_attr]
            return final_df.append(json_normalize(req_response[res_obj_name]))

    def get_contacts(self, start_time=None, fields=None) -> Iterable:
        """
        Get either all available contacts or recent ones specified by start_time.

        API supports more options, possible to extend in the future
        :param start_time:
        :return: generator object with all available pages
        """
        offset = -1

        if not fields:
            contact_properties = CONTACT_DEFAULT_PROPERTIES
            expected_contact_cols = CONTACTS_DEFAULT_COLS + self._build_contact_property_cols(
                CONTACTS_DEFAULT_COLS)
        else:
            contact_properties = fields
            expected_contact_cols = CONTACTS_DEFAULT_COLS + self._build_property_cols(fields)

        parameters = {'property': contact_properties, 'formSubmissionMode': 'all', 'showListMemberships': 'true'}

        # hubspot api allows only 30 days back
        if start_time and (datetime.utcnow() - start_time).days >= 30:
            start_time = datetime.now() + timedelta(-30)
        parameters['propertyMode'] = 'value_and_history'
        if start_time:
            return self._get_paged_result_pages(CONTACTS_RECENT, parameters, 'contacts', 'count', 'timeOffset',
                                                'time-offset', 'has-more', int(start_time.timestamp() * 1000), 100,
                                                default_cols=expected_contact_cols)
        else:
            return self._get_paged_result_pages(CONTACTS_ALL, parameters, 'contacts', 'count', 'vidOffset',
                                                'vid-offset', 'has-more', offset, 100,
                                                default_cols=expected_contact_cols)

    def get_companies(self, recent=False, fields=None):

        offset = 0
        if not fields:
            company_properties = COMPANY_DEFAULT_PROPERTIES
            expected_company_cols = COMPANIES_DEFAULT_COLS + self._build_property_cols(
                COMPANY_DEFAULT_PROPERTIES)
        else:
            company_properties = fields
            expected_company_cols = COMPANIES_DEFAULT_COLS + self._build_property_cols(fields)

        parameters = {'properties': company_properties}

        if recent:
            return self._get_paged_result_pages(COMPANIES_RECENT, parameters, 'results', 'count', 'offset', 'offset',
                                                'hasMore',
                                                offset, 200, default_cols=expected_company_cols)
        else:
            return self._get_paged_result_pages(COMPANIES_ALL, parameters, 'companies', 'limit', 'offset', 'offset',
                                                'has-more', offset, 250, default_cols=expected_company_cols)

    def get_company_properties(self):
        req = self.get_raw(self.base_url + COMPANY_PROPERTIES)
        req_response = req.json()
        return req_response

    def _build_property_cols(self, properties):
        # get flattened property cols
        prop_cols = []
        for p in properties:
            prop_cols.append('properties.' + p + '.source')
            prop_cols.append('properties.' + p + '.sourceId')
            prop_cols.append('properties.' + p + '.timestamp')
            prop_cols.append('properties.' + p + '.value')
            prop_cols.append('properties.' + p + '.versions')
        return prop_cols

    def _build_contact_property_cols(self, properties):
        # get flattened property cols
        prop_cols = []
        for p in properties:
            prop_cols.append('properties.' + p + '.value')
            prop_cols.append('properties.' + p + '.versions')
        return prop_cols

    def get_deals(self, start_time=None, fields=None) -> Iterable:
        offset = 0
        if not fields:
            deal_properties = DEAL_DEFAULT_PROPERTIES
            expected_deal_cols = DEAL_DEFAULT_COLS + self._build_property_cols(
                DEAL_DEFAULT_PROPERTIES)
        else:
            deal_properties = fields
            expected_deal_cols = COMPANIES_DEFAULT_COLS + self._build_property_cols(fields)

        parameters = {'properties': deal_properties, 'propertiesWithHistory': 'dealstage',
                      'includeAssociations': 'true'}
        if start_time:
            parameters['since'] = int(start_time.timestamp() * 1000)
            return self._get_paged_result_pages(DEALS_RECENT, parameters, 'results', 'count', 'offset', 'offset',
                                                'hasMore',
                                                offset, 100, default_cols=expected_deal_cols)
        else:
            return self._get_paged_result_pages(DEALS_ALL, parameters, 'deals', 'limit', 'offset', 'offset', 'hasMore',
                                                offset, 250, default_cols=expected_deal_cols)

    def get_campaigns(self, recent=False):
        final_df = pd.DataFrame()
        if recent:
            url = CAMPAIGNS_BY_ID_RECENT
        else:
            url = CAMPAIGNS_BY_ID

        for res in self._get_paged_result_pages(url, {}, 'campaigns', 'limit', 'offset', 'offset', 'hasMore', None,
                                                1000):

            for index, row in res.iterrows():
                req = self.get_raw(self.base_url + CAMPAIGNS + str(row['id']))
                req_response = req.json()

                final_df = final_df.append(json_normalize(req_response))

            yield final_df[['counters.open', 'counters.click', 'id', 'name']]

    def get_email_events(self, start_date: datetime) -> Iterable:
        offset = ''
        timestamp = None
        if start_date:
            timestamp = int(start_date.timestamp() * 1000)
        parameters = {'eventType': 'OPEN', 'startTimestamp': timestamp}
        for open_ev in self._get_paged_result_pages(EMAIL_EVENTS, parameters, 'events', 'limit', 'offset', 'offset',
                                                    'hasMore',
                                                    offset, 1000):
            yield open_ev

        parameters = {'eventType': 'CLICK', 'startTimestamp': timestamp}
        for click_ev in self._get_paged_result_pages(EMAIL_EVENTS, parameters, 'events', 'limit', 'offset', 'offset',
                                                     'hasMore',
                                                     offset, 1000):
            yield click_ev

    def get_activities(self, start_time: datetime) -> Iterable:
        offset = 0

        if start_time:
            pages = self._get_paged_result_pages(ENGAGEMENTS_PAGED_SINCE, {"since": int(start_time.timestamp() * 1000)},
                                                 'results', 'count', 'offset', 'offset', 'hasMore', offset, 250)
        else:
            pages = self._get_paged_result_pages(ENGAGEMENTS_PAGED, {}, 'results', 'limit', 'offset', 'offset',
                                                 'hasMore',
                                                 offset, 250)
        for pg_res in pages:
            if 'metadata.text' in pg_res.columns:
                pg_res.drop(['metadata.text'], 1)
            if 'metadata.html' in pg_res.columns:
                pg_res.drop(['metadata.html'], 1)
            yield pg_res

    def get_lists(self):
        offset = 0

        return self._get_paged_result_pages(LISTS, {}, 'lists', 'limit', 'offset', 'offset', 'has-more',
                                            offset, 250)

    def get_pipelines(self, include_inactive=None):
        final_df = pd.DataFrame()

        req = self.get_raw('https://api.hubapi.com/deals/v1/pipelines', params={'include_inactive': include_inactive})
        req_response = req.json()

        final_df = final_df.append(json_normalize(req_response))

        return [final_df]

    def get_owners(self, include_inactive=True):
        final_df = pd.DataFrame()

        req = self.get_raw('https://api.hubapi.com/owners/v2/owners/', params={'include_inactive': include_inactive})
        req_response = req.json()

        final_df = final_df.append(json_normalize(req_response))

        return [final_df]
