**Working with the `Period from date`**

Only some endpoints support this parameter and different rules applies for each of them:

- `contacts` - The endpoint only scrolls back in time 30 days. **NOTE** that it may happen that returned data contain contacts updated before the specified date, 
this can happen when there is <100 contacts modified since that date. The pagination since date is simulated by the app, so always at least first page (100) of contacts is returned.
- `deals` - This endpoint will only return records modified in the last 30 days, or the 10k most recently modified records.
- `email_events`- not limited
- `activities` - this is [Engagements API](https://developers.hubspot.com/docs/methods/engagements/engagements-overview) in Hubspot jargon. This endpoint will only return records updated in the last 30 days, or the 10k most recently updated records.
- `companies` - This endpoint will only return records updated in the last 30 days, or the 10k most recently updated records.