'''
Template Component main class.

'''

import logging
import os
import sys

import pandas as pd
from datetime import datetime
from kbc.env_handler import KBCEnvHandler

from hubspot.client_service import HubspotClientService

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

APP_VERSION = '1.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True

        log_level = logging.DEBUG if debug else logging.INFO
        if self.cfg_params.get(KEY_STDLOG):
            # for debug purposes
            self.set_default_logger(log_level)
        else:
            self.set_gelf_logger(log_level)

        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as ex:
            logging.exception(ex)
            exit(1)

        self.incremental = self.cfg_params.get(KEY_INCR_OUT)

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        token = params[KEY_API_TOKEN]
        client_service = HubspotClientService(token)

        if params.get(KEY_PERIOD_FROM):
            import dateparser
            period = params.get(KEY_PERIOD_FROM)
            if not dateparser.parse(period):
                raise ValueError(F'Invalid date from period "{period}", check the supported format')
            start_date, end_date = self.get_date_period_converted(period,
                                                                  datetime.utcnow().strftime('%Y-%m-%d'))
            recent = True
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

        if 'email_events' in endpoints:
            logging.info('Extracting Email Events from HubSpot CRM')
            res_file_path = os.path.join(self.tables_out_path, 'email_events.csv')
            self._get_simple_ds(res_file_path, EMAIL_EVENTS_PK, client_service.get_email_events, start_date)

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
                              property_attributes)

        if 'deals' in endpoints:
            logging.info('Extracting Deals from HubSpot CRM')
            self.get_deals(client_service, start_date, self._parse_props(params.get(KEY_DEAL_PROPERTIES)),
                           property_attributes)

        if 'pipelines' in endpoints:
            logging.info('Extracting Pipelines from HubSpot CRM')
            self.get_pipelines(client_service)

    def _get_simple_ds(self, res_file_path, pkey, ds_getter, *fpars):
        """
        Generic method to get simple objects
        :param res_file_path:
        :param pkey:
        :param ds_getter:
        :return:
        """
        res_columns = list()
        for res in ds_getter(*fpars):
            self.output_file(res, res_file_path, res.columns)
            res_columns = list(res.columns.values)

        # store manifest
        if os.path.isfile(res_file_path):
            cleaned_columns = self._cleanup_col_names(res_columns)
            self.configuration.write_table_manifest(file_name=res_file_path, primary_key=pkey,
                                                    incremental=self.incremental,
                                                    columns=cleaned_columns, destination='in.c-test.destination')

    # CONTACTS
    def get_contacts(self, client: HubspotClientService, start_time, fields, property_attributes):
        res_file_path = os.path.join(self.tables_out_path, 'contacts.csv')
        res_columns = []
        for res in client.get_contacts(property_attributes, start_time, fields):
            if len(res.columns.values) == 0:
                logging.info("No contact records for specified period.")
                continue
            if 'form-submissions' in res.columns or 'list-memberships' in res.columns:
                self._store_contact_submission_and_list(res)
                res.drop(['form-submissions', 'list-memberships'], 1, inplace=True, errors='ignore')

            if 'identity-profiles' in res.columns:
                self._store_contact_identity_profiles(res)
                res.drop(['identity-profiles'], 1, inplace=True, errors='ignore')

            self.output_file(res, res_file_path, res.columns)
            # store columns
            res_columns = list(res.columns.values)

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self.configuration.write_table_manifest(file_name=res_file_path, primary_key=CONTACT_PK,
                                                    incremental=self.incremental,
                                                    columns=cl_cols)

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
            self.configuration.write_table_manifest(file_name=c_subform_path, primary_key=C_SUBMISSION_PK,
                                                    columns=CONTACT_FORM_SUBISSION_COLS,
                                                    incremental=self.incremental)
        if os.path.isfile(c_lists_path):
            self.configuration.write_table_manifest(file_name=c_lists_path, primary_key=CONTACT_LIST_PK,
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
            self.configuration.write_table_manifest(file_name=c_profiles, primary_key=['identity_profile_pk'],
                                                    columns=CONTACT_PROFILES_COLS,
                                                    incremental=self.incremental)
        if os.path.isfile(c_identities):
            self.configuration.write_table_manifest(file_name=c_identities,
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
        for res in client.get_deals(property_attributes, start_time, fields):
            self._store_deals_stage_hist_and_list(res)
            res.drop(['properties.dealstage.versions'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedVids'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedDealIds'], 1, inplace=True, errors='ignore')
            res.drop(['associations.associatedCompanyIds'], 1, inplace=True, errors='ignore')
            self.output_file(res, res_file_path, res.columns)
            # store columns
            res_columns = list(res.columns.values)

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self.configuration.write_table_manifest(file_name=res_file_path, primary_key=DEAL_PK,
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
                stage_his_cols = list(temp_stage_history.columns.values)

            if row.get('associations.associatedVids') and len(row['associations.associatedVids']) != '[]':
                temp_deals_contacts_list = pd.DataFrame(row['associations.associatedVids'],
                                                        columns=['contact_vid'])
                temp_deals_contacts_list['dealId'] = row['dealId']
                self.output_file(temp_deals_contacts_list, c_lists_path, temp_deals_contacts_list.columns)
                c_list_cols = list(temp_deals_contacts_list.columns.values)

            if row.get('associations.associatedCompanyIds') and len(row['associations.associatedCompanyIds']) != '[]':
                comp_list = pd.DataFrame(row['associations.associatedCompanyIds'],
                                         columns=['associated_companyId'])
                comp_list['dealId'] = row['dealId']
                self.output_file(comp_list, companies_lists_path, comp_list.columns)
                comp_list_cols = list(comp_list.columns.values)

            if row.get('associations.associatedDealIds') and len(row['associations.associatedDealIds']) != '[]':
                ass_deal_list = pd.DataFrame(row['associations.associatedDealIds'],
                                             columns=['associated_dealId'])
                ass_deal_list['dealId'] = row['dealId']
                self.output_file(ass_deal_list, deal_lists_path, ass_deal_list.columns)
                ass_deal_list_cols = list(ass_deal_list.columns.values)

        if os.path.isfile(stage_hist_path):
            self.configuration.write_table_manifest(file_name=stage_hist_path, primary_key=DEAL_STAGE_HIST_PK,
                                                    columns=stage_his_cols,
                                                    incremental=self.incremental)
        if os.path.isfile(c_lists_path):
            self.configuration.write_table_manifest(file_name=c_lists_path, primary_key=DEAL_C_LIST_PK,
                                                    columns=c_list_cols,
                                                    incremental=self.incremental)
        if os.path.isfile(deal_lists_path):
            self.configuration.write_table_manifest(file_name=deal_lists_path,
                                                    primary_key=['dealId', 'associated_dealId'],
                                                    columns=ass_deal_list_cols,
                                                    incremental=self.incremental)
        if os.path.isfile(companies_lists_path):
            self.configuration.write_table_manifest(file_name=companies_lists_path,
                                                    primary_key=['dealId', 'associated_companyId'],
                                                    columns=comp_list_cols,
                                                    incremental=self.incremental)

    # PIPELINES
    def get_pipelines(self, client: HubspotClientService):
        res_file_path = os.path.join(self.tables_out_path, 'pipelines.csv')
        res_columns = list()
        for res in client.get_pipelines():
            self._store_pipeline_stages(res)
            res.drop(['stages'], 1, inplace=True, errors='ignore')
            self.output_file(res, res_file_path, res.columns)
            res_columns = list(res.columns.values)

        # store manifests
        if os.path.isfile(res_file_path):
            cl_cols = self._cleanup_col_names(res_columns)
            self.configuration.write_table_manifest(file_name=res_file_path, primary_key=PIPELINE_PK, columns=cl_cols,
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
                res_columns = list(temp_pipelines_stages.columns.values)

        if os.path.isfile(stage_hist_path):
            self.configuration.write_table_manifest(file_name=stage_hist_path, primary_key=PIPELINE_STAGE_PK,
                                                    columns=res_columns,
                                                    incremental=self.incremental)

    def output_file(self, data_output, file_output, column_headers):
        """
        Output the dataframe input to destination file
        Append to the file if file does not exist
        * row by row
        """
        if data_output.empty:
            logging.debug("No results for %s", file_output)
            return

        if not os.path.isfile(file_output):
            with open(file_output, 'w+', encoding='utf-8', newline='') as b:
                data_output.to_csv(b, index=False, header=False, columns=column_headers, line_terminator="")
            b.close()
        else:
            with open(file_output, 'a', encoding='utf-8', newline='') as b:
                data_output.to_csv(b, index=False, header=False, columns=column_headers, line_terminator="")
            b.close()

    def _parse_props(self, param):
        cols = []
        if param:
            cols = [p.strip() for p in param.split(",")]
        return cols

    def _cleanup_col_names(self, columns):
        new_cols = list()
        for col in columns:
            new_cols.append(col.replace('properties.', '', 1).replace('.value', '', 1).replace('.', '_'))
        return new_cols


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
