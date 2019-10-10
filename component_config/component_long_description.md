This component uses the [HubSpot API](https://developers.hubspot.com/docs/overview) to download the following 
data from [HubSpot](http://www.hubspot.com/):

- Companies
- Contacts
- Deals & Pipelines
- Campaigns
- Email Events
- Engagements
- Contact Lists
- Owners

To configure the extractor, you need to have

a working HubSpot account, and
a [HubSpot API Key](https://app.hubspot.com/keys/get).


The Contacts, Companies, Engagements, Email Events  and Deals support incremental load to limit the API calls.