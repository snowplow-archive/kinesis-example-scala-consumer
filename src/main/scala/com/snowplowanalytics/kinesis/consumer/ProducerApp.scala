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
package com.snowplowanalytics.kinesis.producer

// Java
import java.io.File

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{Config, ConfigFactory}

/**
 * The CLI entrypoint for the ProducerApp
 */
object ProducerApp {

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

  // Optional argument to create the stream
  val create = parser.flag[Boolean](List("create"),
                                    "Create the stream before producing events")

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

  /**
   * Main Producer program
   */
  def main(args:Array[String]) {

    // Grab the command line arguments
    parser.parse(args)
    val crte = create.value.getOrElse(false)
    val conf = config.value.getOrElse(ConfigFactory.load("default")) // Fall back to the /resources/default.conf

    val sp = StreamProducer(conf)

    // If we have --create, then create the stream first
    if (crte) {
      sp.createStream()
    }

    // Define and run our pricing mechanism
    sp.produceStream()
    
    sp.printRecords()
  }
}
