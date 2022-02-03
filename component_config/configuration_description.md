**Working with the `Period from date`**

Only some endpoints support this parameter and different rules applies for each of them:

- `contacts` - The endpoint only scrolls back in time 30 days. **NOTE** that it may happen that returned data contain contacts updated before the specified date, 
this can happen when there is <100 contacts modified since that date. The pagination since date is simulated by the app, so always at least first page (100) of contacts is returned.
- `deals` - This endpoint will only return records modified in the last 30 days, or the 10k most recently modified records.
- `email_events`- not limited
- `activities` - this is [Engagements API](https://developers.hubspot.com/docs/methods/engagements/engagements-overview) in Hubspot jargon. This endpoint will only return records updated in the last 30 days, or the 10k most recently updated records.
- `companies` - This endpoint will only return records updated in the last 30 days, or the 10k most recently updated records.

#### Supported Endpoints

- [Companies](#Companies)  
  **Result tables** : `companies`
- [Contacts](#Contacts)  
  **Result tables** : `contacts`, `contacts_form_submissions`, `contacts_lists`, `contacts_identity_profile_identities`
  , `contacts_identity_profiles`

- [Deals](#Deals)  
  **Result tables** : `deals`, `deals_stage_history`, `deals_contacts_list`, `deals_assoc_deals_list`
  , `deals_assoc_companies_list`
- [Pipelines](#Pipelines)  
  **Result tables** : `pipelines`, `pipeline_stages`

- [Campaigns](#Campaigns)  
  **Result tables** : `campaigns`

- [Email Events](#Email Events)  
  **Result tables** : `email_events`
- [Activities (Engagements)](#Engagements)    
  **Result tables** : `activities`
- [Lists](#Contact Lists)    
  **Result tables** : `lists`
- [Owners](#Owners)    
  **Result tables** : `owners`
- [Meetings](https://developers.hubspot.com/docs/api/crm/meetings) - engagement meetings
- [Calls](https://developers.hubspot.com/docs/api/crm/calls) - engagement calls
- [Emails](https://developers.hubspot.com/docs/api/crm/emails) - engagement emails
- [Forms](https://developers.hubspot.com/docs/api/marketing/forms) - Marketing api Forms
- [Marketing Emails Statistics](https://legacydocs.hubspot.com/docs/methods/cms_email/get-all-marketing-email-statistics) - CMS Marketing Emails Statistics