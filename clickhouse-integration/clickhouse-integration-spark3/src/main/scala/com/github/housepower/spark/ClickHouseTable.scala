/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.housepower.spark

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class Shard()

class ClickHouseTable(override val name: String,
                      override val schema: StructType,
                      engine: String,
                      partition: Seq[String],
                      shards: Seq[Shard]
                     ) extends Table with SupportsRead with SupportsWrite {

  override def capabilities(): util.Set[TableCapability] =
    Set(BATCH_READ, BATCH_WRITE, TRUNCATE).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ClickHouseScanBuilder = ???

  override def newWriteBuilder(info: LogicalWriteInfo): ClickHouseWriteBuilder = ???
//  {
//    engine match {
//      case "Distribute" =>
//
//      case _ =>
//
//    }
//  }

}
