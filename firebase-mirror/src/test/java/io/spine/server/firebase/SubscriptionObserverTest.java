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

import com.google.common.testing.NullPointerTester;
import io.grpc.stub.StreamObserver;
import io.spine.base.Identifier;
import io.spine.client.Subscription;
import io.spine.client.SubscriptionId;
import io.spine.client.SubscriptionUpdate;
import io.spine.client.Topic;
import io.spine.client.TopicVBuilder;
import io.spine.server.SubscriptionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.spine.grpc.StreamObservers.noOpObserver;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SuppressWarnings("DuplicateStringLiteralInspection") // Test display names.
@DisplayName("SubscriptionObserver should")
class SubscriptionObserverTest {

    private SubscriptionService service;
    private StreamObserver<SubscriptionUpdate> updateObserver;

    @BeforeEach
    void beforeEach() {
        service = mock(SubscriptionService.class);
        updateObserver = noOpObserver();
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() throws NoSuchMethodException {
        StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        new NullPointerTester()
                .ignore(SubscriptionObserver.class.getMethod("onError", Throwable.class))
                .testAllPublicInstanceMethods(observer);
    }

    @Test
    @DisplayName("not accept nulls on construction")
    void rejectNullsOnConstruction() {
        new NullPointerTester()
                .setDefault(SubscriptionService.class, service)
                .setDefault(StreamObserver.class, noOpObserver())
                .testAllPublicConstructors(SubscriptionObserver.class);
    }

    @Test
    @DisplayName("activate all subscriptions")
    void activateAllSubscriptions() {
        SubscriptionObserver observer = new SubscriptionObserver(service, updateObserver);
        Subscription subscription = Subscription
                .newBuilder()
                .setId(newSubscriptionId())
                .build();
        observer.onNext(subscription);
        verify(service).activate(subscription, updateObserver);
    }

    @Test
    @DisplayName("throw ISE upon error")
    void throwISEUponError() {
        StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        Throwable throwable = new CustomThrowable();
        try {
            observer.onError(throwable);
            fail("Exception not thrown");
        } catch (IllegalStateException ise) {
            assertTrue(ise.getCause() instanceof CustomThrowable);
        }
    }

    @Test
    @DisplayName("do nothing upon successful completion")
    void doNothingUponSuccess() {
        StreamObserver<?> observer = new SubscriptionObserver(service, updateObserver);
        observer.onCompleted();
        observer.onCompleted();
    }

    private static SubscriptionId newSubscriptionId() {
        return SubscriptionId
                .vBuilder()
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
