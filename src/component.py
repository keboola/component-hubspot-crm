'''
Template Component main class.

'''
import json
import logging
import os
import sys
import warnings
from typing import Dict, List

import keboola.csvwriter
import keboola.utils as kbcutils
import pandas as pd
from keboola.component import ComponentBase
from keboola.csvwriter import ElasticDictWriter

from hubspot_api.client_service import HubspotClientService, CONTACTS_DEFAULT_COLS
from json_parser import FlattenJsonParser

ENGAGEMENT_ASSOC_COLS = ["contactIds",
                         "companyIds",
                         "dealIds",
                         "ownerIds"]

KEY_CONTACT_VID = 'contact_canonical_vid'

# primary keys
PIPELINE_STAGE_PK = ['pipelineId', 'stageId']
PIPELINE_PK = ['pipelineId']
OWNER_PK = ['ownerId']
LISTS_PK = ['listId']
ACTIVITIES_PK = ['engagement_id ']
EMAIL_EVENTS_PK = ['id', 'created']
CAMPAIGNS_PK = ['id']
DEAL_C_LIST_PK = ['dealId', 'contact_vid']
DEAL_STAGE_HIST_PK = ['dealId', 'sourceVid', 'sourceId', 'timestamp']
DEAL_PK = ['dealId ']
CONTACT_LIST_PK = ['internal_list_id', 'static_list_id', KEY_CONTACT_VID]
C_SUBMISSION_PK = ['form_id', KEY_CONTACT_VID, 'portal_id', 'conversion_id', 'page_id', 'page_url']
CONTACT_PK = ['canonical_vid', 'portal_id']
COMPANY_ID_COL = ['companyId']

# config keys
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'
KEY_ENDPOINTS = 'endpoints'
KEY_INCR_OUT = 'incremental_output'
KEY_COMPANY_PROPERTIES = 'company_properties'
KEY_CONTACT_PROPERTIES = 'contact_properties'
KEY_DEAL_PROPERTIES = 'deal_properties'
KEY_PROPERTY_ATTRIBUTES = "property_attributes"
# for debug
KEY_STDLOG = 'stdlogging'

SUPPORTED_ENDPOINTS = ['companies', 'campaigns', 'email_events', 'activities', 'lists', 'owners', 'contacts', 'deals',
                       'pipelines']

MANDATORY_PARS = [KEY_API_TOKEN]
MANDATORY_IMAGE_PARS = []

# columns
CONTACT_FORM_SUBISSION_COLS = ["contact-associated-by", "conversion-id", "form-id", "form-type", "meta-data",
                               "page-id", "page-url", "portal-id", "timestamp", "title", KEY_CONTACT_VID]
CONTACT_PROFILES_COLS = ["vid", "saved-at-timestamp", KEY_CONTACT_VID, 'identity_profile_pk']
CONTACT_PROFILE_IDENTITIES_COLS = ['type', 'value', 'timestamp', 'is-primary', 'identity_profile_pk']
CONTACT_LISTS_COLS = ["internal-list-id", "is-member", "static-list-id", "timestamp", "vid", KEY_CONTACT_VID]
DEAL_STAGE_HIST_COLS = ['name', 'source', 'sourceId', 'sourceVid', 'timestamp', 'value', 'dealId']

ENGAGEMENT_COLS = [
    "id",
    "portalId",
    "active",
    "createdAt",
    "lastUpdated",
    "ownerId",
    "type",
    "timestamp",
    "metadata"
]

APP_VERSION = '1.0.1'


class Component(ComponentBase):

    def __init__(self, debug=False):
        ComponentBase.__init__(self)

        # temp suppress pytz warning
        warnings.filterwarnings(
            "ignore",
            message="The localize method is no longer necessary, as this time zone supports the fold attribute",
        )
        logging.info('Loading configuration...')

        try:
            self.validate_configuration_parameters(MANDATORY_PARS)
        except ValueError as ex:
            logging.exception(ex)
            exit(1)

        self.incremental = self.configuration.parameters.get(KEY_INCR_OUT)
        state = self.get_state_file() or {}
        self._object_schemas: dict = state.get('table_schemas', {})

        self._writer_cache: Dict[str, ElasticDictWriter] = {}

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters  # noqa
        token = params[KEY_API_TOKEN]
        client_service = HubspotClientService(token)

        if params.get(KEY_PERIOD_FROM):
            import dateparser
            period = params.get(KEY_PERIOD_FROM)
            if not dateparser.parse(period):
                raise ValueError(F'Invalid date from period "{period}", check the supported format')
            start_date, end_date = kbcutils.parse_datetime_interval(period, 'now')
            recent = True
            logging.info(f"Getting data since: {period}")
        else:
            start_date = None
            recent = False

        endpoints = params.get(KEY_ENDPOINTS, SUPPORTED_ENDPOINTS)
        property_attributes = params.get(KEY_PROPERTY_ATTRIBUTES,
                                         {"include_versions": True, "include_source": True, "include_timestamp": True})

        if 'companies' in endpoints:
            logging.info('Extracting Companies')
            res_file_path = os.path.join(self.tables_out_path, 'companies.csv')
            self._get_simple_ds(res_file_path, COMPANY_ID_COL, client_service.get_companies, property_attributes,
                                recent, self._parse_props(params.get(KEY_COMPANY_PROPERTIES)))

        if 'campaigns' in endpoints:
            logging.info('Extracting Campaigns from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'campaigns.csv')
            self._get_simple_ds(res_file_path, CAMPAIGNS_PK, client_service.get_campaigns, recent)

        email_events = [e for e in endpoints if e.startswith('email_events')]
        if email_events:
            email_events = set(email_events)
            # backward compatibility
            if "email_events" in email_events:
                email_events.add('email_events-CLICK')
                email_events.add('email_events-OPEN')
                email_events.remove('email_events')

            logging.info('Extracting Email Events from HubSpot CRM')

            events_list = [e.split('-')[1] for e in email_events]
            res_file_path = os.path.join(self.tables_out_path, 'email_events.csv')
            self._get_simple_ds(res_file_path, EMAIL_EVENTS_PK, client_service.get_email_events, start_date,
                                events_list)

        if 'activities' in endpoints:
            logging.info('Extracting Activities from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'activities.csv')
            self._get_simple_ds(res_file_path, ACTIVITIES_PK, client_service.get_activities, start_date)

        if 'lists' in endpoints:
            logging.info('Extracting Lists from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'lists.csv')
            self._get_simple_ds(res_file_path, LISTS_PK, client_service.get_lists)

        if 'owners' in endpoints:
            logging.info('Extracting Owners from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'owners.csv')
            self._get_simple_ds(res_file_path, OWNER_PK, client_service.get_owners, recent)

        if 'contacts' in endpoints:
            logging.info('Extracting Contacts from HubSpot CRM')
            self.get_contacts(client_service, start_date, self._parse_props(params.get(KEY_CONTACT_PROPERTIES)),
                              property_attributes, params.get('include_contact_list_membership', True))

        if 'deals' in endpoints:
            logging.info('Extracting Deals from HubSpot CRM')
            self.get_deals(client_service, start_date, self._parse_props(params.get(KEY_DEAL_PROPERTIES)),
                           property_attributes)

        if 'pipelines' in endpoints:
            logging.info('Extracting Pipelines from HubSpot CRM')
            self.get_pipelines(client_service)

        if 'dispositions' in endpoints:
            logging.info('Extracting Engagement Dispositons from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'engagement-dispositions.csv')
            self._get_simple_ds(res_file_path, ['id'], client_service.get_owners, recent)

        if 'calls' in endpoints:
            logging.info('Extracting Calls HubSpot CRM')
            self._dowload_crm_v3_object(client_service, 'calls',
                                        properties=self._parse_props(params.get('call_properties', [])))

        if 'emails' in endpoints:
            logging.info('Extracting Emails HubSpot CRM')
            self._dowload_crm_v3_object(client_service, 'emails',
                                        properties=self._parse_props(params.get('email_properties', [])))

        if 'meetings' in endpoints:
            logging.info('Extracting Meetings HubSpot CRM')
            self._dowload_crm_v3_object(client_service, 'meetings',
                                        properties=self._parse_props(params.get('meeting_properties', [])))
        if 'forms' in endpoints:
            logging.info('Extracting Forms HubSpot CRM')
            parser = FlattenJsonParser(child_separator='__', exclude_fields=['displayOptions'],
                                       keys_to_ignore=['fieldGroups'])
            self._download_v3_parsed(client_service.get_forms, parser, 'forms')

        if 'marketing_email_statistics' in endpoints:
            logging.info('Extracting marketing_email_statistics HubSpot')
            parser = FlattenJsonParser(child_separator='__', exclude_fields=['smartEmailFields'],
                                       keys_to_ignore=['styleSettings'])
            self._download_v3_parsed(client_service.get_email_statistics, parser, 'marketing_email_statistics')

        self._close_files()

    def _get_simple_ds(self, res_file_path, pkey, ds_getter, *fpars):
        """
        Generic method to get simple objects
        :param res_file_path:
        :param pkey:
        :param ds_getter:
        :return:
        """
        res_columns = list()
        counter = 0
        for res in (df for df in ds_getter(*fpars) if not df.empty):
            counter += 1
            self.output_file(res, res_file_path, res.columns)
            res_columns = list(res.columns.values)
            if counter % 100 == 0:
                logging.info(f"Processed {counter} records.")

        # store manifest
        if os.path.isfile(res_file_path):
            cleaned_columns = self._cleanup_col_names(res_columns)
            self._write_table_manifest_legacy(file_name=res_file_path, primary_key=pkey,
                                              incremental=self.incremental,
                                              columns=cleaned_columns)

    # CONTACTS
    def get_contacts(self, client: HubspotClientService, start_time, fields, property_attributes,
                     include_membership: True):
        res_file_path = os.path.join(self.tables_out_path, 'contacts.csv')
        res_columns = []
        counter = 0
        for res in client.get_contacts(property_attributes, start_time, fields, include_membership):
            counter += 100
            if len(res.columns.values) == 0:
                logging.info("No contact records for specified period.")
                continue

            if self.configuration.parameters.get('contact_associations'):
                self._download_contact_associations(client, res)

            if 'form-submissions' in res.columns or 'list-memberships' in res.columns:
                self._store_contact_submission_and_list(res)
                res.drop(['form-submissions', 'list-memberships'], 1, inplace=True, errors='ignore')

            if 'identity-profiles' in res.columns:
                self._store_contact_identity_profiles(res)
                res.drop(['identity-profiles'], 1, inplace=True, errors='ignore')

            if counter % 100 == 0:
                logging.info(f"Processed {counter} Contact records.")

            self._drop_duplicate_properties(res, CONTACTS_DEFAULT_COLS)
            self.output_file(res, res_file_path, res.columns)
            # store columns
            res_columns = list(res.columns.values)
            logging.debug(f"Returned contact columns: {res_columns}")

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self._write_table_manifest_legacy(file_name=res_file_path, primary_key=CONTACT_PK,
                                              incremental=self.incremental,
                                              columns=cl_cols)

    def _download_contact_associations(self, client: HubspotClientService, result: pd.DataFrame):
        vids = result['vid'].tolist()
        for ass in self.configuration.parameters['contact_associations']:
            results = client.get_associations('contact', ass['to_object_type'], vids)
            self._write_associations('contact', ass['to_object_type'], results)

    def _write_associations(self, from_type: str, to_type: str, data: List[dict]):
        result_table = self.create_out_table_definition('object_associations.csv', incremental=self.incremental,
                                                        primary_key=['from_id', 'from_type', 'to_id', 'to_type'])
        result_row = {}
        header = ['from_id'
                  'from_type',
                  'to_id',
                  'to_type',
                  'association_types']
        for row in data:
            for association in row['to']:
                result_row['from_id'] = row['from']['id']
                result_row['from_type'] = from_type
                result_row['to_id'] = association['toObjectId']
                result_row['to_type'] = to_type
                result_row['association_types'] = association['associationTypes']
                self.output_object_dict(result_row, result_table.full_path, header)

        if data:
            self.write_manifest(result_table)

    def _drop_duplicate_properties(self, df, property_names: list):
        columns = list(df.columns.values)
        drop_columns = []
        for c in columns:
            if c.startswith('properties') and c.split('.')[1] in property_names:
                drop_columns.append(c)

        df.drop(columns=drop_columns, inplace=True)

    def _store_contact_submission_and_list(self, contacts):

        c_subform_path = os.path.join(self.tables_out_path, 'contacts_form_submissions.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'contacts_lists.csv')
        # Create table with Contact's form submissions and lists and drop column afterwards
        for index, row in contacts.iterrows():

            if len(row['form-submissions']) > 0:
                temp_contacts_sub_forms = pd.DataFrame(row['form-submissions'])
                temp_contacts_sub_forms[KEY_CONTACT_VID] = row['canonical-vid']
                res_cols = CONTACT_FORM_SUBISSION_COLS
                temp_contacts_sub_forms = temp_contacts_sub_forms.reindex(columns=res_cols).fillna('')

                # save res
                self.output_file(temp_contacts_sub_forms, c_subform_path, temp_contacts_sub_forms.columns)

            if len(row['list-memberships']) > 0:
                temp_contacts_lists = pd.DataFrame(row['list-memberships'])
                temp_contacts_lists[KEY_CONTACT_VID] = row['canonical-vid']
                res_cols = CONTACT_LISTS_COLS
                temp_contacts_lists = temp_contacts_lists.reindex(columns=res_cols).fillna('')
                # save res
                self.output_file(temp_contacts_lists, c_lists_path, temp_contacts_lists.columns)

        if os.path.isfile(c_subform_path):
            self._write_table_manifest_legacy(file_name=c_subform_path, primary_key=C_SUBMISSION_PK,
                                              columns=CONTACT_FORM_SUBISSION_COLS,
                                              incremental=self.incremental)
        if os.path.isfile(c_lists_path):
            self._write_table_manifest_legacy(file_name=c_lists_path, primary_key=CONTACT_LIST_PK,
                                              columns=CONTACT_LISTS_COLS,
                                              incremental=self.incremental)

    def _store_contact_identity_profiles(self, contacts):
        c_profiles = os.path.join(self.tables_out_path, 'contacts_identity_profiles.csv')
        c_identities = os.path.join(self.tables_out_path, 'contacts_identity_profile_identities.csv')
        # Create table with Contact's form submissions and lists and drop column afterwards
        for index, row in contacts.iterrows():

            if len(row['identity-profiles']) > 0:
                tmp_profiles = pd.DataFrame(row['identity-profiles'])
                tmp_profiles[KEY_CONTACT_VID] = row['canonical-vid']
                identity_profile_pk = str(row['canonical-vid']) + '|' + str(row['vid'])
                tmp_profiles['identity_profile_pk'] = identity_profile_pk

                if len(tmp_profiles['identities']) > 0:
                    self._store_identities(tmp_profiles['identities'], c_identities, identity_profile_pk)
                    # tmp_identities = pd.DataFrame(list(tmp_profiles['identities']))
                    # tmp_identities['identity_profile_pk'] = identity_profile_pk
                    # self.output_file(tmp_identities, c_identities, tmp_identities.columns)

                res_cols = CONTACT_PROFILES_COLS
                tmp_profiles = tmp_profiles.reindex(columns=res_cols).fillna('')

                # save res
                self.output_file(tmp_profiles, c_profiles, tmp_profiles.columns)

        if os.path.isfile(c_profiles):
            self._write_table_manifest_legacy(file_name=c_profiles, primary_key=['identity_profile_pk'],
                                              columns=CONTACT_PROFILES_COLS,
                                              incremental=self.incremental)
        if os.path.isfile(c_identities):
            self._write_table_manifest_legacy(file_name=c_identities,
                                              primary_key=['identity_profile_pk', 'type', 'value'],
                                              columns=CONTACT_PROFILE_IDENTITIES_COLS,
                                              incremental=self.incremental)

    def _store_identities(self, identities, res_file, identity_profile_pk):
        for index, row in identities.iteritems():
            tmp_identities = pd.DataFrame(row).copy()
            tmp_identities['identity_profile_pk'] = identity_profile_pk
            tmp_identities = tmp_identities.reindex(columns=CONTACT_PROFILE_IDENTITIES_COLS)
            self.output_file(tmp_identities, res_file, tmp_identities.columns)

    # DEALS
    def get_deals(self, client: HubspotClientService, start_time, fields, property_attributes):
        res_file_path = os.path.join(self.tables_out_path, 'deals.csv')
        res_columns = list()
        counter = 0
        for res in client.get_deals(property_attributes, start_time, fields):
            counter += 1
            self._store_deals_stage_hist_and_list(res)
            res.drop(['properties.dealstage.versions'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedVids'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedDealIds'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedCompanyIds'], 1, inplace=True, errors='ignore')
            self.output_file(res, res_file_path, res.columns)
            # store columns
            if not res.empty:
                res_columns = list(res.columns.values)

            if counter % 100 == 0:
                logging.info(f"Processed {counter} Deals records.")

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self._write_table_manifest_legacy(file_name=res_file_path, primary_key=DEAL_PK,
                                              incremental=self.incremental,
                                              columns=cl_cols)

    def _store_deals_stage_hist_and_list(self, deals):

        stage_hist_path = os.path.join(self.tables_out_path, 'deals_stage_history.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'deals_contacts_list.csv')
        deal_lists_path = os.path.join(self.tables_out_path, 'deals_assoc_deals_list.csv')
        companies_lists_path = os.path.join(self.tables_out_path, 'deals_assoc_companies_list.csv')
        # Create table with Deals' Stage History & Deals' Contacts List
        c_list_cols, stage_his_cols, comp_list_cols, ass_deal_list_cols = None, None, None, None
        for index, row in deals.iterrows():

            if row.get('properties.dealstage.versions') and str(
                    row['properties.dealstage.versions']) != 'nan' and len(row['properties.dealstage.versions']) > 0:
                temp_stage_history = pd.DataFrame(row['properties.dealstage.versions'])
                temp_stage_history['dealId'] = row['dealId']
                # fix columns - sometimes there are some missing in the response
                temp_stage_history = temp_stage_history.reindex(columns=DEAL_STAGE_HIST_COLS).fillna('')

                self.output_file(temp_stage_history, stage_hist_path, temp_stage_history.columns)
                if not stage_his_cols:
                    stage_his_cols = list(temp_stage_history.columns.values)

            if row.get('associations.associatedVids') and len(row['associations.associatedVids']) != 0:
                temp_deals_contacts_list = pd.DataFrame(row['associations.associatedVids'],
                                                        columns=['contact_vid'])
                temp_deals_contacts_list['dealId'] = row['dealId']
                self.output_file(temp_deals_contacts_list, c_lists_path, temp_deals_contacts_list.columns)
                if not c_list_cols:
                    c_list_cols = list(temp_deals_contacts_list.columns.values)

            if row.get('associations.associatedCompanyIds') and len(row['associations.associatedCompanyIds']) != 0:
                comp_list = pd.DataFrame(row['associations.associatedCompanyIds'],
                                         columns=['associated_companyId'])
                comp_list['dealId'] = row['dealId']
                self.output_file(comp_list, companies_lists_path, comp_list.columns)
                if not comp_list_cols:
                    comp_list_cols = list(comp_list.columns.values)

            if row.get('associations.associatedDealIds') and len(row['associations.associatedDealIds']) != 0:
                ass_deal_list = pd.DataFrame(row['associations.associatedDealIds'],
                                             columns=['associated_dealId'])
                ass_deal_list['dealId'] = row['dealId']
                self.output_file(ass_deal_list, deal_lists_path, ass_deal_list.columns)
                if not ass_deal_list_cols:
                    ass_deal_list_cols = list(ass_deal_list.columns.values)

        if os.path.isfile(stage_hist_path):
            self._write_table_manifest_legacy(file_name=stage_hist_path, primary_key=DEAL_STAGE_HIST_PK,
                                              columns=stage_his_cols,
                                              incremental=self.incremental)
        if os.path.isfile(c_lists_path):
            self._write_table_manifest_legacy(file_name=c_lists_path, primary_key=DEAL_C_LIST_PK,
                                              columns=c_list_cols,
                                              incremental=self.incremental)
        if os.path.isfile(deal_lists_path):
            self._write_table_manifest_legacy(file_name=deal_lists_path,
                                              primary_key=['dealId', 'associated_dealId'],
                                              columns=ass_deal_list_cols,
                                              incremental=self.incremental)
        if os.path.isfile(companies_lists_path):
            self._write_table_manifest_legacy(file_name=companies_lists_path,
                                              primary_key=['dealId', 'associated_companyId'],
                                              columns=comp_list_cols,
                                              incremental=self.incremental)

    # PIPELINES
    def get_pipelines(self, client: HubspotClientService):
        res_file_path = os.path.join(self.tables_out_path, 'pipelines.csv')
        res_columns = list()
        counter = 0
        for res in client.get_pipelines():
            counter += 1
            self._store_pipeline_stages(res)
            res.drop(['stages'], 1, inplace=True, errors='ignore')
            self.output_file(res, res_file_path, res.columns)
            if not res_columns:
                res_columns = list(res.columns.values)

            if counter % 100 == 0:
                logging.info(f"Processed {counter} Pipelines records.")

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self._write_table_manifest_legacy(file_name=res_file_path, primary_key=PIPELINE_PK, columns=cl_cols,
                                              incremental=self.incremental)

    def _store_pipeline_stages(self, pipelines):

        stage_hist_path = os.path.join(self.tables_out_path, 'pipeline_stages.csv')
        # Create table with Pipelines' Stages.
        res_columns = list()
        for index, row in pipelines.iterrows():

            if len(row['stages']) > 0:
                temp_pipelines_stages = pd.DataFrame(row['stages'])
                temp_pipelines_stages['pipelineId'] = row['pipelineId']
                self.output_file(temp_pipelines_stages, stage_hist_path, temp_pipelines_stages.columns)
                if not res_columns:
                    res_columns = list(temp_pipelines_stages.columns.values)

        if os.path.isfile(stage_hist_path):
            self._write_table_manifest_legacy(file_name=stage_hist_path, primary_key=PIPELINE_STAGE_PK,
                                              columns=res_columns,
                                              incremental=self.incremental)

    def _dowload_crm_v3_object(self, client: HubspotClientService, object_name: str, **kwargs):
        result_table = self.create_out_table_definition(f'{object_name}.csv', incremental=self.incremental,
                                                        primary_key=['id'])
        result_path = result_table.full_path
        header_columns = self._object_schemas.get(result_path, ['id'])
        counter = 0
        for res in client.get_v3_engagement_object(object_name, **kwargs):
            if counter % 500 == 0:
                logging.info(f"Downloaded {counter} records.")
            for row in res:
                counter += 1
                self.output_object_dict(row, result_path, header_columns)

        if counter > 0:
            self.write_manifest(result_table)

    def _download_v3_parsed(self, method, parser: FlattenJsonParser, object_name: str, **kwargs):
        result_table = self.create_out_table_definition(f'{object_name}.csv', incremental=self.incremental,
                                                        primary_key=['id'])
        result_path = result_table.full_path
        header_columns = self._object_schemas.get(result_path, ['id'])

        counter = 0
        for res in method(**kwargs):
            if counter % 500 == 0:
                logging.info(f"Downloaded {counter} records.")
            for row in res:
                parsed_row = parser.parse_row(row)
                counter += 1
                self.output_object_dict(parsed_row, result_path, header_columns)

        if counter > 0:
            self.write_manifest(result_table)

    def output_file(self, data_output, file_output, column_headers):
        """
        Output the dataframe input to destination file
        Append to the file if file does not exist
        * row by row
        """
        if data_output.empty:
            logging.debug("No results for %s", file_output)
            return
        data_output = data_output.astype(str)
        _mode = 'w+' if not os.path.isfile(file_output) else 'a'

        with open(file_output, _mode, encoding='utf-8', newline='') as b:
            data_output.to_csv(b, index=False, header=False, columns=column_headers, line_terminator="")

    def output_object_dict(self, data_output: dict, file_output, column_headers):
        """
        Output the dataframe input to destination file
        Append to the file if file does not exist
        * row by row
        """
        writer = self._get_writer_from_cache(file_output, column_headers)
        data_output = self._flatten_properties(data_output)
        writer.writerow(data_output)

    def _flatten_properties(self, result: dict):
        properties = result.pop('properties', {})
        for p in properties:
            result[p] = properties[p]
        return result

    def _get_writer_from_cache(self, output_path: str, column_headers):
        if not self._writer_cache.get(output_path):
            self._writer_cache[output_path] = keboola.csvwriter.ElasticDictWriter(output_path, column_headers)
            self._writer_cache[output_path].writeheader()

        return self._writer_cache[output_path]

    def _close_files(self):
        for key, f in self._writer_cache.items():
            f.close()
            self._object_schemas[key] = f.fieldnames

        self.write_state_file({"table_schemas": self._object_schemas})

    def _parse_props(self, param):
        cols = []
        if param:
            cols = [p.strip() for p in param.split(",")]
        return cols

    def _cleanup_col_names(self, columns):
        new_cols = list()
        for col in columns:
            new_col = col.replace('properties.', '', 1).replace('.value', '', 1).replace('.', '_')
            if new_col not in new_cols:
                new_cols.append(new_col)
            else:
                new_cols.append(col)
        return new_cols

    def _write_table_manifest_legacy(self,
                                     file_name,
                                     destination='',
                                     primary_key=None,
                                     columns=None,
                                     incremental=None):
        """
        Write manifest for output table Manifest is used for
        the table to be stored in KBC Storage.
        Args:
            file_name: Local file name of the CSV with table data.
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns for headless CSV files
            incremental: Set to true to enable incremental loading
        """
        manifest = {}
        if destination:
            if isinstance(destination, str):
                manifest['destination'] = destination
            else:
                raise TypeError("Destination must be a string")
        if primary_key:
            if isinstance(primary_key, list):
                manifest['primary_key'] = primary_key
            else:
                raise TypeError("Primary key must be a list")
        if columns:
            if isinstance(columns, list):
                manifest['columns'] = columns
            else:
                raise TypeError("Columns must by a list")
        if incremental:
            manifest['incremental'] = True
        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)


"""
            Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except KeyError as e:
        logging.exception(Exception(F'Invalid Key value:{e}'))
        exit(2)
    except Exception as e:
        logging.exception(e)
        exit(1)
