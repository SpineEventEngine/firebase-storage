/*
 * Copyright 2019, TeamDev. All rights reserved.
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

import io.spine.core.Subscribe;
import io.spine.core.TenantId;
import io.spine.server.event.AbstractEventSubscriber;
import io.spine.server.event.EventSubscriber;
import io.spine.server.tenant.TenantAdded;

import java.util.Set;

import static com.google.common.collect.Sets.newConcurrentHashSet;

/**
 * An {@link EventSubscriber} for the {@link TenantAdded} events.
 *
 * <p>Triggers a {@linkplain TenantCallback callback} on new tenant.
 *
 * <p>Not all the events cause the callback invocation, but only those that introduce a new
 * ({@linkplain #knownTenants previously unknown} to this instance of
 * {@code NewTenantEventSubscriber}) tenant ID.
 */
final class NewTenantEventSubscriber extends AbstractEventSubscriber {

    /**
     * Stores the IDs of tenants, which are already known to this instance of
     * {@code NewTenantEventSubscriber}.
     *
     * <p>The tenant is considered known when a {@link TenantAdded} event for this tenant has been
     * received by this instance of {@code NewTenantEventSubscriber}.
     */
    private final Set<TenantId> knownTenants = newConcurrentHashSet();
    private final TenantCallback tenantCallback;

    /**
     * Creates a new instance of {@code NewTenantEventSubscriber}.
     *
     * @param tenantCallback
     *         the callback to be invoked when a new tenant emerges
     */
    NewTenantEventSubscriber(TenantCallback tenantCallback) {
        super();
        this.tenantCallback = tenantCallback;
    }

    @Subscribe(external = true)
    public void on(TenantAdded event) {
        TenantId tenantId = event.getId();
        log().info("Received TenantAdded event. New tenant ID is: {}", tenantId);
        if (!knownTenants.contains(tenantId)) {
            knownTenants.add(tenantId);
            tenantCallback.onTenant(tenantId);
        }
    }

    /**
     * An interface of the callback triggered when a new tenant is discovered.
     *
     * @author Dmytro Dashenkov
     */
    interface TenantCallback {

        /**
         * Reacts on a new tenant.
         *
         * @param tenantId
         *         the new tenant ID
         */
        void onTenant(TenantId tenantId);
    }
}
