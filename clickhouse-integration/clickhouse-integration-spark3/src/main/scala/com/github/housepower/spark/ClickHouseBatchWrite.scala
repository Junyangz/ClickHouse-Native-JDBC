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
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

class ClickHouseBatchWrite(val grpcConn: GrpcConnection) extends BatchWrite {

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {

    new DataWriterFactory() {
      override def createWriter(partitionId: Int, taskId: Long): ClickHouseBatchWriter = {
        ???
      }
    }
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
