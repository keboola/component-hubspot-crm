# Hubspot extractor

KBC Component for data retrieval from [Hubspot API](https://developers.hubspot.com/docs/overview).


## Functionality
Supports retrieval from several endpoints. Some endpoints allow retrieval of recently updated records, 
this is set by `Date From` parameter. In most of the cases maximum of last 30 days can be retrieved.

### Supported Endpoints
#### Companies
    
 [All companies](https://developers.hubspot.com/docs/methods/companies/get-all-companies) or 
 [recently modified (last 30 days) ](https://developers.hubspot.com/docs/methods/companies/get_companies_modified) can be retrieved. 
 NOTE: Fetches always 30 day period
 
 Following Company properties are fetched by default:
  
 ```json
 ['about_us', 'name', 'phone', 'facebook_company_page', 'city', 'country', 'website', 
 'industry', 'annualrevenue', 'linkedin_company_page', 'hs_lastmodifieddate', 'hubspot_owner_id', 'notes_last_updated', 
 'description', 'createdate', 'numberofemployees', 'hs_lead_status', 'founded_year', 'twitterhandle', 'linkedinbio']
```
 
Custom properties may be specified in configuration, names must match with api names as specified by [Company Properties](https://developers.hubspot.com/docs/methods/companies/company-properties-overview)
 

#### Contacts
    
 [All contacts](https://developers.hubspot.com/docs/methods/contacts/get_contacts) or 
 [recently modified (max last 30 days) ](https://developers.hubspot.com/docs/methods/contacts/get_recently_updated_contacts) can be retrieved. 
 Recently modified period can be limited by `Date From` parameter 
 
 Following Contact properties are fetched by default:
  
 ```json
 ['hs_facebookid', 'hs_linkedinid', 'ip_city', 'ip_country', 'ip_country_code', 'newsletter_opt_in', 'firstname', 
 'linkedin_profile', 'lastname', 'email', 'mobilephone', 'phone', 'city', 'country', 'region', 'jobtitle', 
 'company', 'website', 'numemployees', 'industry', 'associatedcompanyid', 'hs_lead_status', 
 'lastmodifieddate', 'source', 'hs_email_optout', 'twitterhandle', 'lead_type', 'hubspot_owner_id', 
 'notes_last_updated', 'hs_analytics_source', 'opt_in', 'createdate', 'hs_twitterid', 'lifecyclestage']
```
 **Note:** Following properties will be fetched each time, regardless configuration and with option `propertyMode=value_and_history`. This is currently hardcoded and 
 all other properties are fetched with value only:
 
 ```json
 ["company",
  "firstname",
  "lastmodifieddate",
  "lastname"]
```
 
Custom properties may be specified in configuration, names must match with api names as specified by [Contact Properties](https://developers.hubspot.com/docs/methods/contacts/contact-properties-overview)
 
#### Deals
    
 [All deals](https://developers.hubspot.com/docs/methods/deals/get-all-deals) or 
 [recently modified (last 30 days) ](https://developers.hubspot.com/docs/methods/deals/get_deals_modified) can be retrieved. 
 NOTE: Fetches max 30 day period, larger periods are cut to match the limit.
 
 Following Deal properties are fetched by default:
  
 ```json
['authority', 'budget', 'campaign_source', 'hs_analytics_source', 'hs_campaign', 
'hs_lastmodifieddate', 'need', 'timeframe', 'dealname', 'amount', 'closedate', 'pipeline', 
'createdate', 'engagements_last_meeting_booked', 'dealtype', 'hs_createdate', 'description', 
'start_date', 'closed_lost_reason', 'closed_won_reason', 'end_date', 'lead_owner', 'tech_owner', 
'service_amount', 'contract_type', 'hubspot_owner_id', 'partner_name', 'notes_last_updated']
```
 
Custom properties may be specified in configuration, names must match with api names as specified by [Deal Properties](https://developers.hubspot.com/docs/methods/deals/deal_properties_overview)
 



##Creating a new component
Clone this repository into new folder and remove git history
```bash
git clone https://bitbucket.org:kds_consulting_team/kbc-python-template.git my-new-component
cd my-new-component
rm -rf .git
git init
git remote add origin PATH_TO_YOUR_BB_REPO
git update-index --chmod=+x deploy.sh
git update-index --chmod=+x scripts/update_dev_portal_properties.sh
git add .
git commit -m 'initial'
git push -u origin master
```


##Setting up CI
 - Enable [pipelines](https://confluence.atlassian.com/bitbucket/get-started-with-bitbucket-pipelines-792298921.html) in the repository.
 - Set `KBC_DEVELOPERPORTAL_APP` env variable in Bitbucket (dev portal app id)
 
 ![picture](docs/imgs/ci_variable.png)
 
 
## Development
 
This example contains runnable container with simple unittest. For local testing it is useful to include `data` folder in the root
and use docker-compose commands to run the container or execute tests. 

If required, change local data folder path to your custom:
```yaml
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
```

Clone this repository and init the workspace with following command:

```
git clone https://bitbucket.org:kds_consulting_team/kbc-python-template.git my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
```

Run the test suite and lint check using this command:

```
docker-compose run --rm test
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 