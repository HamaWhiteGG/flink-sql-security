/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.security.flink.visitor.basic;

import com.hw.security.flink.PolicyManager;
import com.hw.security.flink.SecurityContext;

import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * @description: AbstractBasicVisitor
 * @author: HamaWhite
 */
public abstract class AbstractBasicVisitor extends SqlBasicVisitor<Void> {

    protected final SecurityContext securityContext;

    protected final PolicyManager policyManager;

    protected final String username;

    protected AbstractBasicVisitor(SecurityContext securityContext, String username) {
        this.securityContext = securityContext;
        this.policyManager = securityContext.getPolicyManager();
        this.username = username;
    }

    protected ObjectIdentifier toObjectIdentifier(String tablePath) {
        String[] items = tablePath.split("\\.");
        return ObjectIdentifier.of(items[0], items[1], items[2]);
    }
}
