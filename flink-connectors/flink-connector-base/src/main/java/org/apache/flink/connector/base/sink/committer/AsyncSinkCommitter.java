/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.committer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

public class AsyncSinkCommitter implements Committer<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(AsyncSinkCommitter.class);

    private final MailboxExecutor mailboxExecutor;

    public AsyncSinkCommitter(Sink.CommitterInitContext context) {
        this.mailboxExecutor = context.getMailboxExecutor();
    }

    @Override
    public List<Void> commit(List<Void> committables)
            throws IOException, InterruptedException {

        checkArgument(committables.size() == 0);

        //TODO: wait for all outstanding requests to complete

        //magic happens here

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}
