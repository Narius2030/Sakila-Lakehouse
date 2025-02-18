# This stage will run transformations periodically by DBT and Airflow

![image](https://github.com/user-attachments/assets/6741cf76-6298-4c06-83a4-cda8b3b6148e)


## Setup DBT project before run

Install these packages by `dbt deps` command:
```yaml
packages:
  - package: calogica/dbt_expectations
    version: 0.10.4

  - package: dbt-labs/dbt_utils
    version: 1.3.0
```

Configure the database engine in `profile.yml`
```yaml
streamify:
  target: delta-dev
  outputs:
    delta-dev:
      type: trino
      method: none  # optional
      user: <TRINO-USER>
      database: delta
      schema: streamify
      host: <TRINO-HOST>
      port: <TRINO-PORT>
      threads: 1
```
## Validate the configurations

Run this command to ensure the correctness
```bash
dbt debug
```

Run this command to check available sources
```bash
dbt source freshness
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
