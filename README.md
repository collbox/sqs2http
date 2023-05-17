# sqs2http

Repeatedly takes messages from an SQS queue and POSTs them to an HTTP
endpoint of your choosing.

## Introduction

*TODO: Write project introduction*

## Basic Usage

```sh
clojure -X:run :post-url '"http://localhost:8080"' :queue-url '"http://sqs.us-west-2.amazonaws.com/1234567890/my-queue"'
```

To find your queue URL, you can run:

```sh
aws sqs list-queues
```

## Status

*Alpha status*.  Still working out the kinks.  Not suitable to be used
in a production environment.

## Installation

*TODO: How does a user install the project?*

## Documentation

*TODO: Is there more documentation somewhere?*

## Testing

*TODO: How is the project tested?*

## Contributing

*TODO: How can someone else contribute?*

## License

Copyright © Collbox Inc, 2023

Distributed under the MIT License.

