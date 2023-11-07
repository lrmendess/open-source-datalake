Set of documentation for DBT projects.

### Using the docs project

Try running the following commands:
- dbt deps
- dbt docs generate
- dbt docs serve
- Access the website presented in stdout


### How to execute DBT Project
```bash
docker run \
--network=datalake-network \
--mount type=bind,source=dbt_project_path/,target=/usr/app \
--mount type=bind,source=dbt_profile_path/profiles.yml,target=/root/.dbt/ \ # Optional
dbt-trino run

dbt run --project-dir /usr/app
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
