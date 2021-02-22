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
import java.util.UUID

import com.github.housepower.client.GrpcConnection
import com.github.housepower.protocol.grpc.{ClickHouseGrpc, QueryInfo}
import com.github.housepower.settings.ClickHouseConfig
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.ClickHouseAnalysisException
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

class ClickHouseCatalog extends TableCatalog with SupportsNamespaces with Logging {

  private var catalogName: String = _

  private var currentDb: String = _

  private var grpcConn: GrpcConnection = _
  private var blockingStub: ClickHouseGrpc.ClickHouseBlockingStub = _
  private var futureStub: ClickHouseGrpc.ClickHouseFutureStub = _

  private var baseQueryInfo: QueryInfo = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    val host = options.getOrDefault("host", "localhost")
    val port = options.getInt("port", 9100)
    val user = options.getOrDefault("user", "default")
    val password = options.getOrDefault("password", "")
    this.currentDb = options.getOrDefault("database", "default")
    val cfg = ClickHouseConfig.Builder.builder()
      .host(host)
      .port(port)
      .user(user)
      .password(password)
      .database(currentDb)
      .build()
    this.grpcConn = GrpcConnection.create(cfg)
    this.blockingStub = grpcConn.blockingStub()
    this.futureStub = grpcConn.futureStub()

    this.baseQueryInfo = QueryInfo.newBuilder
      .setUserName(user)
      .setPassword(password)
      .buildPartial

    val ex = blockingStub.executeQuery(
      QueryInfo.newBuilder(baseQueryInfo)
        .setQueryId(UUID.randomUUID().toString)
        .setQuery("select now()")
        .build()
    ).getException

    if (ex.getCode != 0)
      throw new ClickHouseAnalysisException(s"Error[${ex.getCode}] ${ex.getDisplayText}")
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = namespace match {
    case Array(database) =>
      val result = blockingStub.executeQuery(
        QueryInfo.newBuilder(baseQueryInfo)
          .setQueryId(UUID.randomUUID().toString)
          .setQuery(s"show tables in $database")
          .setOutputFormat("JSONCompact")
          .build()
      )

      Option(result.getException)
        .filterNot { ex => ex.getCode == 0 }
        .foreach {
          case ex if ex.getCode == 81 => throw new NoSuchDatabaseException(namespace.mkString("."))
          case ex =>
            throw new ClickHouseAnalysisException(s"Error[${ex.getCode}] ${ex.getDisplayText}")
        }

      val output = JacksonUtil.om.readValue[JSONCompactOutput](result.getOutput)
      output.data.map { row => row.get(0).asText() }
        .map { table => Identifier.of(namespace, table) }
        .toArray

    case _ => throw new NoSuchDatabaseException(namespace.mkString("."))
  }

  override def loadTable(ident: Identifier): Table = ???

  override def createTable(ident: Identifier,
                           schema: StructType,
                           partitions: Array[Transform],
                           properties: util.Map[String, String]): Table = {
    ???
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = (ident.namespace() match {
    case Array() => Some(ident.name())
    case Array(database) => Some(s"$database.${ident.name()}")
    case _ => None
  }).exists { table =>
    val result = blockingStub.executeQuery(
      QueryInfo.newBuilder(baseQueryInfo)
        .setQuery(s"drop table $table")
        .setQueryId(UUID.randomUUID().toString)
        .setOutputFormat("JSONCompact")
        .build()
    )
    result.getException == null
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def defaultNamespace(): Array[String] = Array(currentDb)

  override def listNamespaces(): Array[Array[String]] = {
    val result = blockingStub.executeQuery(
      QueryInfo.newBuilder(baseQueryInfo)
        .setQuery("show databases")
        .setQueryId(UUID.randomUUID().toString)
        .setOutputFormat("JSONCompact")
        .build()
    )
    val output = JacksonUtil.om.readValue[JSONCompactOutput](result.getOutput)
    output.data.map { row => Array(row.get(0).asText()) }.toArray
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = namespace match {
    case Array() => listNamespaces()
    case _ => throw new NoSuchNamespaceException(namespace.mkString("."))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = namespace match {
    case Array(database) =>
      listNamespaces()
        .map { case Array(db) => db }
        .find { db => database == db }
        .map { _ => Map.empty[String, String].asJava }
        .getOrElse {
          throw new NoSuchDatabaseException(namespace.mkString("."))
        }
    case _ => throw new NoSuchDatabaseException(namespace.mkString("."))
  }

  override def createNamespace(namespace: Array[String],
                               metadata: util.Map[String, String]): Unit = namespace match {
    case Array(database) =>
      blockingStub.executeQuery(
        QueryInfo.newBuilder(baseQueryInfo)
          .setQuery(s"create database $database")
          .setQueryId(UUID.randomUUID().toString)
          .setOutputFormat("JSONCompact")
          .build()
      )
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = namespace match {
    case Array(database) =>
      val result = blockingStub.executeQuery(
        QueryInfo.newBuilder(baseQueryInfo)
          .setQuery(s"drop database $database")
          .setQueryId(UUID.randomUUID().toString)
          .setOutputFormat("JSONCompact")
          .build()
      )
      result.getException == null
    case _ => false
  }
}
