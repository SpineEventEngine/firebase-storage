/*
 * Copyright 2018, TeamDev. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.spine.server.firebase;

import com.google.common.collect.ImmutableList;
import io.spine.core.TenantId;
import io.spine.server.firebase.NewTenantEventSubscriber.TenantCallback;
import io.spine.server.tenant.TenantAdded;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.collect.Lists.newLinkedList;
import static org.junit.Assert.assertEquals;

@DisplayName("NewTenantEventSubscriber should")
class NewTenantEventSubscriberTest {

    @Test
    @DisplayName("not trigger callback twice for same tenant")
    void notTriggerForSameTenant() {
        MemoizingTenantCallback callback = new MemoizingTenantCallback();
        NewTenantEventSubscriber subscriber = new NewTenantEventSubscriber(callback);
        TenantId theId = newId();
        TenantAdded event1 = event(theId);
        TenantAdded event2 = event(theId);

        subscriber.on(event1);
        subscriber.on(event1);
        subscriber.on(event2);

        assertEquals(1, callback.getTenants().size());
    }

    private static TenantAdded event(TenantId tenantId) {
        TenantAdded event = TenantAdded
                .newBuilder()
                .setId(tenantId)
                .build();
        return event;
    }

    private static TenantId newId() {
        return TenantId.newBuilder()
                       .setValue(NewTenantEventSubscriberTest.class.getName())
                       .build();
    }

    private static final class MemoizingTenantCallback implements TenantCallback {

        private final List<TenantId> tenants = newLinkedList();

        @Override
        public void onTenant(TenantId tenantId) {
            tenants.add(tenantId);
        }

        private List<TenantId> getTenants() {
            return ImmutableList.copyOf(tenants);
        }
    }
}
