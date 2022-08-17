<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

# AD-to-MO Sync

This repository contains an OS2mo AMQP Trigger that synchronizes data from AD to MO.

## Usage

Adjust the `AMQP_URL` variable to OS2mo's running message-broker, either;
* directly in `docker-compose.yml` or
* by creating a `docker-compose.override.yaml` file.

Now start the container using `docker-compose`:
```
docker-compose up -d
```

You should see the following:
```
[info     ] Starting metrics server        port=8000
[info     ] Register called                function=ad2mosync_callback routing_key=org_unit.org_unit.*
[info     ] Starting AMQP system
[info     ] Establishing AMQP connection   host=msg_broker path=/ port=5672 scheme=amqp user=guest
[info     ] Creating AMQP channel
[info     ] Attaching AMQP exchange to channel exchange=os2mo
[info     ] Declaring unique message queue function=ad2mosync_callback queue_name=os2mo-amqp-trigger-ad2mosync_callback
[info     ] Starting message listener      function=ad2mosync_callback
[info     ] Binding routing keys           function=ad2mosync_callback
[info     ] Binding routing-key            function=ad2mosync_callback routing_key=org_unit.org_unit.*
```
After which each message will add:
```
[debug    ] Received message               function=ad2mosync_callback routing_key=org_unit.org_unit.edit
[info     ] Message received               object_type=org_unit payload=... request_type=edit service_type=org_unit
```
At which point metrics should be available at `localhost:8000`, and ADGUIDs should be synced.

## Development

### Prerequisites

- [Poetry](https://github.com/python-poetry/poetry)

### Getting Started

1. Clone the repository:
```
git clone git@git.magenta.dk:rammearkitektur/os2mo-triggers/OS2mo-amqp-trigger-ad-to-mo-sync.git
```

2. Install all dependencies:
```
poetry install
```

3. Set up pre-commit:
```
poetry run pre-commit install
```

### Running the tests

You use `poetry` and `pytest` to run the tests:

`poetry run pytest`

You can also run specific files

`poetry run pytest tests/<test_folder>/<test_file.py>`

and even use filtering with `-k`

`poetry run pytest -k "Manager"`

You can use the flags `-vx` where `v` prints the test & `x` makes the test stop if any tests fails (Verbose, X-fail)

#### Running the integration tests

To run the integration tests, an AMQP instance must be available.

If an instance is already available, it can be used by configuring the `AMQP_URL`
environmental variable. Alternatively a RabbitMQ can be started in docker, using:
```
docker run -d -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

## Versioning

This project uses [Semantic Versioning](https://semver.org/) with the following strategy:
- MAJOR: Incompatible changes to existing data models
- MINOR: Backwards compatible updates to existing data models OR new models added
- PATCH: Backwards compatible bug fixes

## Authors

Magenta ApS <https://magenta.dk>

## License

This project uses: [MPL-2.0](MPL-2.0.txt)

This project uses [REUSE](https://reuse.software) for licensing.
All licenses can be found in the [LICENSES folder](LICENSES/) of the project.
