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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.metrics.MetricGroup;

import static org.apache.flink.util.Preconditions.checkNotNull;

class CommitterInitContextImpl implements Sink.CommitterInitContext {
    private final MailboxExecutor mailboxExecutor;
    private final MetricGroup metricGroup;

    CommitterInitContextImpl(MailboxExecutor mailboxExecutor, MetricGroup metricGroup) {
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.metricGroup = checkNotNull(metricGroup);
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        return mailboxExecutor;
    }

    @Override
    public MetricGroup metricGroup() {
        return metricGroup;
    }
}
