'''
Template Component main class.

'''

import logging
import os
from datetime import datetime

import pandas as pd
from kbc.env_handler import KBCEnvHandler

from hubspot.client_service import HubspotClientService

# primary keys
PIPELINE_STAGE_PK = ['PIPELINE_ID', 'stageId']
PIPELINE_PK = ['pipelineId']
OWNER_PK = ['ownerId']
LISTS_PK = ['listId']
ACTIVITIES_PK = ['engagement_id ']
EMAIL_EVENTS_PK = ['id', 'created']
CAMPAIGNS_PK = ['id']
DEAL_C_LIST_PK = ['Deal_ID', 'Contact_ID']
DEAL_STAGE_HIST_PK = ['DEAL_ID', 'sourceVid', 'sourceId', 'timestamp']
DEAL_PK = ['dealId ']
CONTACT_LIST_PK = ['internal-list-id', 'static-list-id', 'CONTACT_ID']
C_SUBMISSION_PK = ['form-id', 'CONTACT_ID', 'portal-id', 'conversion-id', 'page-id']
CONTACT_PK = ['vid', 'portal_id']
COMPANY_ID_COL = ['companyId']

# config keys
KEY_API_TOKEN = '#api_token'
KEY_PERIOD_FROM = 'period_from'
KEY_PERIOD_TO = 'period_to'

MANDATORY_PARS = [KEY_API_TOKEN]
MANDATORY_IMAGE_PARS = []

# columns
CONTACT_FORM_SUBISSION_COLS = ["contact-associated-by", "conversion-id", "form-id", "form-type", "meta-data",
                               "page-url", "portal-id", "timestamp", "title", 'CONTACT_ID']
CONTACT_LISTS_COLS = ["internal-list-id", "is-member", "static-list-id", "timestamp", "vid", "CONTACT_ID"]
DEAL_STAGE_HIST_COLS = ['name', 'source', 'sourceId', 'sourceVid', 'timestamp', 'value', 'DEAL_ID']

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True

        self.set_default_logger('DEBUG' if debug else 'INFO')
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.error(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        token = params[KEY_API_TOKEN]
        client_service = HubspotClientService(token)

        if params.get(KEY_PERIOD_FROM):
            start_date, end_date = self.get_date_period_converted(params.get(KEY_PERIOD_FROM),
                                                                  datetime.utcnow().strftime('%Y-%m-%d'))
            recent = True
        else:
            start_date = None
            recent = False

        logging.info('Extracting Companies')
        res_file_path = os.path.join(self.tables_out_path, 'companies.csv')
        self._get_simple_ds(res_file_path, COMPANY_ID_COL, client_service.get_companies, recent)

        logging.info('Extracting Campaigns from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'campaigns.csv')
        self._get_simple_ds(res_file_path, CAMPAIGNS_PK, client_service.get_campaigns, recent)

        logging.info('Extracting Email Events from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'email_events.csv')
        self._get_simple_ds(res_file_path, EMAIL_EVENTS_PK, client_service.get_email_events, start_date)

        logging.info('Extracting Activities from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'activities.csv')
        self._get_simple_ds(res_file_path, ACTIVITIES_PK, client_service.get_activities, start_date)

        logging.info('Extracting Lists from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'lists.csv')
        self._get_simple_ds(res_file_path, LISTS_PK, client_service.get_lists)

        logging.info('Extracting Owners from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'owners.csv')
        self._get_simple_ds(res_file_path, OWNER_PK, client_service.get_owners, recent)

        logging.info('Extracting Contacts from HubSpot CRM')
        self.get_contacts(client_service, start_date)

        logging.info('Extracting Deals from HubSpot CRM')
        self.get_deals(client_service, start_date)

        logging.info('Extracting Pipelines from HubSpot CRM')
        self.get_pipelines(client_service)

    def _get_simple_ds(self, res_file_path, pkey, ds_getter, *fpars):
        """
        Generic class to get simple objects
        :param res_file_path:
        :param pkey:
        :param ds_getter:
        :return:
        """
        for res in ds_getter(*fpars):
            self.output_file(res, res_file_path, res.columns)

        # store manifest
        self.configuration.write_table_manifest(file_name=res_file_path, primary_key=pkey, incremental=True)

    # CONTACTS
    def get_contacts(self, client: HubspotClientService, start_time):
        res_file_path = os.path.join(self.tables_out_path, 'contacts.csv')
        for res in client.get_contacts(start_time):
            self._store_contact_submission_and_list(res)
            res.drop(['form-submissions', 'list-memberships'], 1)
            self.output_file(res, res_file_path, res.columns)

        # store manifests
        self.configuration.write_table_manifest(file_name=res_file_path, primary_key=CONTACT_PK, incremental=True)
        c_subform_path = os.path.join(self.tables_out_path, 'contacts_form_submissions.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'contacts_lists.csv')
        if os.path.isfile(c_subform_path):
            self.configuration.write_table_manifest(file_name=c_subform_path, primary_key=C_SUBMISSION_PK,
                                                    incremental=True)
        if os.path.isfile(c_lists_path):
            self.configuration.write_table_manifest(file_name=c_lists_path, primary_key=CONTACT_LIST_PK,
                                                    incremental=True)

    def _store_contact_submission_and_list(self, contacts):

        c_subform_path = os.path.join(self.tables_out_path, 'contacts_form_submissions.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'contacts_lists.csv')
        # Create table with Contact's form submissions and lists and drop column afterwards
        for index, row in contacts.iterrows():

            if len(row['form-submissions']) > 0:
                temp_contacts_sub_forms = pd.DataFrame(row['form-submissions'])
                temp_contacts_sub_forms['CONTACT_ID'] = row['canonical-vid']

                res_cols = CONTACT_FORM_SUBISSION_COLS

                # save res
                self.output_file(temp_contacts_sub_forms, c_subform_path, temp_contacts_sub_forms.columns)

            if len(row['list-memberships']) > 0:
                temp_contacts_lists = pd.DataFrame(row['list-memberships'])
                temp_contacts_lists['CONTACT_ID'] = row['canonical-vid']
                res_cols = CONTACT_LISTS_COLS
                temp_contacts_lists = temp_contacts_lists.loc[:, res_cols].fillna('')
                # save res
                self.output_file(temp_contacts_lists, c_lists_path, temp_contacts_lists.columns)

    # DEALS
    def get_deals(self, client: HubspotClientService, start_time):
        logging.info('Extracting Companies from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'deals.csv')
        for res in client.get_deals(start_time):
            self.output_file(res, res_file_path, res.columns)
            self._store_deals_stage_hist_and_list(res)

        # store manifests
        self.configuration.write_table_manifest(file_name=res_file_path, primary_key=DEAL_PK, incremental=True)
        stage_hist_path = os.path.join(self.tables_out_path, 'deals_stage_history.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'deals_contacts_list.csv')

        if os.path.isfile(stage_hist_path):
            self.configuration.write_table_manifest(file_name=stage_hist_path, primary_key=DEAL_STAGE_HIST_PK,
                                                    incremental=True)
        if os.path.isfile(c_lists_path):
            self.configuration.write_table_manifest(file_name=c_lists_path, primary_key=DEAL_C_LIST_PK,
                                                    incremental=True)

    def _store_deals_stage_hist_and_list(self, deals):

        stage_hist_path = os.path.join(self.tables_out_path, 'deals_stage_history.csv')
        c_lists_path = os.path.join(self.tables_out_path, 'deals_contacts_list.csv')
        # Create table with Deals' Stage History & Deals' Contacts List

        for index, row in deals.iterrows():

            if row.get('properties.dealstage.versions') and str(
                    row['properties.dealstage.versions']) != 'nan' and len(row['properties.dealstage.versions']) > 0:
                temp_stage_history = pd.DataFrame(row['properties.dealstage.versions'])
                temp_stage_history['DEAL_ID'] = row['dealId']
                # fix columns - sometimes there are some missing in the response
                temp_stage_history = temp_stage_history.loc[:, DEAL_STAGE_HIST_COLS].fillna('')

                self.output_file(temp_stage_history, stage_hist_path, temp_stage_history.columns)

            if row.get('associations.associatedVids') and len(row['associations.associatedVids']) != '[]':
                temp_deals_contacts_list = pd.DataFrame(row['associations.associatedVids'],
                                                        columns=['Contact_ID'])
                temp_deals_contacts_list['Deal_ID'] = row['dealId']
                self.output_file(temp_deals_contacts_list, c_lists_path, temp_deals_contacts_list.columns)

    # PIPELINES
    def get_pipelines(self, client: HubspotClientService):
        logging.info('Extracting Companies from HubSpot CRM')
        res_file_path = os.path.join(self.tables_out_path, 'pipelines.csv')
        for res in client.get_pipelines():
            self.output_file(res, res_file_path, res.columns)
            self._store_pipeline_stages(res)

        # store manifests
        self.configuration.write_table_manifest(file_name=res_file_path, primary_key=PIPELINE_PK, incremental=True)

        stage_hist_path = os.path.join(self.tables_out_path, 'pipeline_stages.csv')
        self.configuration.write_table_manifest(file_name=stage_hist_path, primary_key=PIPELINE_STAGE_PK,
                                                incremental=True)

    def _store_pipeline_stages(self, pipelines):

        stage_hist_path = os.path.join(self.tables_out_path, 'pipeline_stages.csv')
        # Create table with Pipelines' Stages.
        for index, row in pipelines.iterrows():

            if len(row['stages']) > 0:
                temp_pipelines_stages = pd.DataFrame(row['stages'])
                temp_pipelines_stages['PIPELINE_ID'] = row['pipelineId']
                self.output_file(temp_pipelines_stages, stage_hist_path, temp_pipelines_stages.columns)

    def output_file(self, data_output, file_output, column_headers):
        """
        Output the dataframe input to destination file
        Append to the file if file does not exist
        * row by row
        """
        if not os.path.isfile(file_output):
            with open(file_output, 'w+', encoding='utf-8') as b:
                data_output.to_csv(b, index=False, columns=column_headers)
            b.close()
        else:
            with open(file_output, 'a', encoding='utf-8') as b:
                data_output.to_csv(b, index=False, header=False, columns=column_headers)
            b.close()


"""
        Main entrypoint
"""
if __name__ == "__main__":
    comp = Component()
    comp.run()
