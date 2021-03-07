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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.util

class ClickHouseBatchWriter(val grpcConn: GrpcConnection,
                            val queryId: String,
                            val database: String,
                            val tables: String,
                            val schema: StructType,
                            val batchSize: Int = 1000
                           ) extends DataWriter[InternalRow] with Logging {

  val buf: util.ArrayList[Array[Byte]] = new util.ArrayList[Array[Byte]](batchSize)

  override def write(record: InternalRow): Unit = {

    //    grpcConn.syncInsert()
  }

  override def commit(): WriterCommitMessage = new WriterCommitMessage {}

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
