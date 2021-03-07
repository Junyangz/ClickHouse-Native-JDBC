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

import com.github.housepower.client.GrpcConnection
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsTruncate, WriteBuilder}

class ClickHouseWriteBuilder(val info: LogicalWriteInfo,
                             val grpcConn: GrpcConnection,
                             database: String,
                             table: String,
                            ) extends WriteBuilder with SupportsTruncate {

  override def buildForBatch(): ClickHouseBatchWrite = {
    new ClickHouseBatchWrite(grpcConn, info, database, table)
  }

  override def truncate(): WriteBuilder = ???
}
