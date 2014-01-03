/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.kinesis.consumer

// Java
import java.io.File

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{Config, ConfigFactory}

/**
 * The CLI entrypoint for the ConsumerApp
 */
object ConsumerApp extends App {

    // Argument specifications
  import ArgotConverters._

  // General bumf for our app
  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](List("config"),
                                     "filename",
                                     "Configuration file. Defaults to \"resources/default.conf\" (within .jar) if not set") {
    (c, opt) =>

      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }

  // Grab the command line arguments
  parser.parse(args)
  val conf = config.value.getOrElse(ConfigFactory.load("default")) // Fall back to the /resources/default.conf

  val sp = StreamConsumer(conf)

  sp.printRecords()
}
