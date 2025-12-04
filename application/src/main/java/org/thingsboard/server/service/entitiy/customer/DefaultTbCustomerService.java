/**
 * Copyright © 2016-2025 The Thingsboard Authors
 *
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
package org.thingsboard.server.service.entitiy.customer;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.Customer;
import org.thingsboard.server.common.data.EntityType;
import org.thingsboard.server.common.data.NameConflictStrategy;
import org.thingsboard.server.common.data.User;
import org.thingsboard.server.common.data.audit.ActionType;
import org.thingsboard.server.common.data.id.CustomerId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.service.entitiy.AbstractTbEntityService;
import org.thingsboard.server.service.install.InstallScripts;
import org.thingsboard.server.service.security.model.SecurityUser;

@Service
@AllArgsConstructor
@Slf4j
public class DefaultTbCustomerService extends AbstractTbEntityService implements TbCustomerService {

    private final InstallScripts installScripts;

    @Override
    public Customer save(Customer customer, SecurityUser user) throws Exception {
        return save(customer, NameConflictStrategy.DEFAULT, user);
    }

    @Override
    public Customer save(Customer customer, NameConflictStrategy nameConflictStrategy, SecurityUser user) throws Exception {
        boolean created = customer.getId() == null;
        ActionType actionType = created ? ActionType.ADDED : ActionType.UPDATED;
        TenantId tenantId = customer.getTenantId();
        try {
            Customer savedCustomer = checkNotNull(customerService.saveCustomer(customer, nameConflictStrategy));
            if (created) {
                // create opinionated minimal dashboards for newly created customers
                try {
                    installScripts.createDefaultCustomerDashboards(savedCustomer.getTenantId(), savedCustomer.getId());
                } catch (Exception e) {
                    // Log and continue; dashboard creation should not block customer creation
                    log.warn("Failed to create default customer dashboards", e);
                }
            }
            autoCommit(user, savedCustomer.getId());
            logEntityActionService.logEntityAction(tenantId, savedCustomer.getId(), savedCustomer, null, actionType, user);
            return savedCustomer;
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.CUSTOMER), customer, actionType, user, e);
            throw e;
        }
    }

    @Override
    public void delete(Customer customer, User user) {
        ActionType actionType = ActionType.DELETED;
        TenantId tenantId = customer.getTenantId();
        CustomerId customerId = customer.getId();
        try {
            customerService.deleteCustomer(tenantId, customerId);
            logEntityActionService.logEntityAction(tenantId, customer.getId(), customer, customerId, actionType,
                    user, customerId.toString());
        } catch (Exception e) {
            logEntityActionService.logEntityAction(tenantId, emptyId(EntityType.CUSTOMER), actionType, user,
                    e, customerId.toString());
            throw e;
        }
    }
}
