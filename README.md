# Sample Scala Event Consumer for Amazon Kinesis

## Introduction

This is an example event consumer for [Amazon Kinesis] [kinesis]
written in Scala and packaged as an SBT project.

This was built by the [Snowplow Analytics] [snowplow] team,
as part of a proof of concept for porting our event collection and
enrichment processes to run on Kinesis.

## Building

Assuming you already have [SBT 0.13.0] [sbt] installed:

    $ git clone git://github.com/snowplow/kinesis-example-scala-consumer.git
    $ cd kinesis-example-scala-consumer
    $ sbt compile
    
## Unit testing

To come.

## Usage

The event producer has the following command-line interface:

```
kinesis-example-scala-consumer: Version 0.0.1. Copyright (c) 2013, Snowplow
Analytics Ltd.

Usage: kinesis-example-scala-consumer[OPTIONS]

OPTIONS
--config filename  Configuration file. Defaults to "resources/default.conf"
                   (within .jar) if not set
```

## Running

Create your own config file:

    $ cp src/main/resources/default.conf my.conf

Now edit it and update the AWS credentials:

```js
aws {
  access-key: "cpf"
  secret-key: "cpf"
}
```

You can leave the rest of the settings for now.

Next, run the event consumer, making sure to specify your new config file:

    $ sbt "run --config my.conf"

## Next steps

Fork this project and adapt it into your own custom Kinesis event consumer.

## Copyright and license

Copyright 2013-2014 Snowplow Analytics Ltd, with portions copyright
2013 Amazon.com, Inc or its affiliates.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[kinesis]: http://aws.amazon.com/kinesis/
[snowplow]: http://snowplowanalytics.com
[sbt]: http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.0/sbt-launch.jar

[kinesis-ui]: https://console.aws.amazon.com/kinesis/?

[license]: http://www.apache.org/licenses/LICENSE-2.0
