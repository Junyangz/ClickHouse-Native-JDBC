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

package com.github.housepower.client;

import com.github.housepower.jdbc.AbstractITest;
import com.github.housepower.log.Logger;
import com.github.housepower.log.LoggerFactory;
import com.github.housepower.protocol.grpc.ClickHouseGrpc;
import com.github.housepower.protocol.grpc.QueryInfo;
import com.github.housepower.protocol.grpc.Result;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class GRPCClientITest extends AbstractITest {

    private static final Logger LOG = LoggerFactory.getLogger(GRPCClientITest.class);

    @Test
    public void test() throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(CK_HOST, CK_GRPC_PORT).usePlaintext().build();
        ClickHouseGrpc.ClickHouseBlockingStub stub = ClickHouseGrpc.newBlockingStub(channel);
        QueryInfo queryInfo = QueryInfo.newBuilder()
                .setUserName(CLICKHOUSE_USER)
                .setPassword(CLICKHOUSE_PASSWORD)
                .setQuery("select now()")
                .setQueryId(UUID.randomUUID().toString())
                .build();
        Result result = stub.executeQuery(queryInfo);
        LOG.info("execute: select now()");
        LOG.info(result.toString());
    }
}
