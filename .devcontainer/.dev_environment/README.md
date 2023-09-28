# A1 dbt enviroment setup

How to setup your environment for dbt development at A1.

## Instructions:

1. Download docker
2. Create a directory called `creds` within the `docker_env` directory (`docker_env/creds`)
3. Copy your profile.yml to the `docker_env/creds` directory, and and make sure the file is named `profiles.yml`
4. Make sure you are in the `docker_env` directory, and then build the Docker image:
```bash
docker build -t [image name] .
```
5. Add this to your `~/.bashrc` (or `~/.zshrc` or whatever shell you use):
```bash
alias ddbt="\
    docker run --rm -v [absolute path to dbt folder]:/aveone_dbt aveone_dbt_env deps \
    && docker run --rm -v [absolute path to dbt folder]:/aveone_dbt aveone_dbt_env"
```

This will allow you to use dbt exactly how you would normally from the command line, except rather than typing dbt, you'll just use the alias ddbt.

For example, instead of using:
```bash
dbt compile
```
You'd use:
```bash
ddbt compile
```
And the command will run in your dbt docker container.

Remember commands:
```bash
docker-compose -f dev_env/docker-compose.yml build
docker-compose -f dev_env/docker-compose.yml run --rm dbt_container_test
```