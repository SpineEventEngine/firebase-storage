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

import com.google.common.testing.NullPointerTester;
import io.grpc.stub.StreamObserver;
import io.spine.base.Identifier;
import io.spine.client.Subscription;
import io.spine.client.SubscriptionId;
import io.spine.client.SubscriptionUpdate;
import io.spine.server.SubscriptionService;
import org.junit.Before;
import org.junit.Test;

import static io.spine.grpc.StreamObservers.noOpObserver;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Dmytro Dashenkov
 */
public class SubscriptionObserverShould {

    private SubscriptionService service;
    private StreamObserver<SubscriptionUpdate> updateObserver;

    @Before
    public void beforeEach() {
        service = mock(SubscriptionService.class);
        updateObserver = noOpObserver();
    }

    @Test
    public void activate_all_subscriptions() {
        final SubscriptionObserver observer = new SubscriptionObserver(service, updateObserver);
        final Subscription subscription = Subscription.newBuilder()
                                                      .setId(newSubscriptionId())
                                                      .build();
        observer.onNext(subscription);
        verify(service).activate(subscription, updateObserver);
    }

    @Test
    public void throw_ISE_upon_error() {
        final StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        final Throwable throwable = new CustomThrowable();
        try {
            observer.onError(throwable);
            fail("Exception not thrown");
        } catch (IllegalStateException ise) {
            assertTrue(ise.getCause() instanceof CustomThrowable);
        }
    }

    @Test
    public void do_nothing_upon_successful_completion() {
        final StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        observer.onCompleted();
        observer.onCompleted();
    }

    @Test
    public void not_accept_nulls_on_construction() {
        new NullPointerTester()
                .setDefault(SubscriptionService.class, service)
                .setDefault(StreamObserver.class, noOpObserver())
                .testAllPublicConstructors(SubscriptionObserver.class);
    }

    @Test
    public void not_accept_null_arguments() throws NoSuchMethodException {
        final StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        new NullPointerTester()
                .ignore(SubscriptionObserver.class.getMethod("onError", Throwable.class))
                .testAllPublicInstanceMethods(observer);
    }

    private static SubscriptionId newSubscriptionId() {
        return SubscriptionId.newBuilder()
                             .setValue(Identifier.newUuid())
                             .build();
    }

    /**
     * A custom {@code Throwable} for tests.
     *
     * <p>Instances of this throwable are used to test the {@link StreamObserver#onError(Throwable)}
     * method. The tests makes sure that the exact type of throwable is thrown.
     */
    private static final class CustomThrowable extends Throwable {
        private static final long serialVersionUID = 0L;
    }
}
