# Hubspot extractor

KBC Component for data retrieval from [Hubspot API](https://developers.hubspot.com/docs/overview).


# Functionality
Supports retrieval from several endpoints

- **Companies**
    
 [All companies](https://developers.hubspot.com/docs/methods/companies/get-all-companies) or 
 [recently modified (last 30 days) ](https://developers.hubspot.com/docs/methods/companies/get_companies_modified) can be retrieved. 
 Following Company properties are fetched by default
 

 




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