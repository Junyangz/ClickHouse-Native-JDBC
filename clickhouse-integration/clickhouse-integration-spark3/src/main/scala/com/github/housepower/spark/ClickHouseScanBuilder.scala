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

import com.github.housepower.protocol.grpc.ClickHouseGrpc
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util

class ClickHouseScanBuilder(ident: Identifier,
                            tableSchema: StructType,
                            properties: util.Map[String, String],
                            blockingStub: ClickHouseGrpc.ClickHouseBlockingStub) extends ScanBuilder
  with SupportsPushDownFilters
  with SupportsPushDownRequiredColumns {

  val readSchema: StructType = tableSchema

  override def build(): ClickHouseBatchScan = ???

  override def pushFilters(filters: Array[Filter]): Array[Filter] = Array()

  override def pushedFilters(): Array[Filter] = Array()

  override def pruneColumns(requiredSchema: StructType): Unit = {}
}
