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

import com.google.cloud.firestore.CollectionReference;
import io.grpc.stub.StreamObserver;
import io.spine.base.EventMessage;
import io.spine.client.SubscriptionUpdate;
import io.spine.client.Topic;
import io.spine.logging.Logging;
import io.spine.type.TypeUrl;

import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * An implementation of {@link StreamObserver} publishing the received {@link SubscriptionUpdate}s
 * to the given {@link CollectionReference Cloud Firestore location}.
 *
 * <p>The implementation logs a message upon either
 * {@linkplain StreamObserver#onCompleted() successful} or
 * {@linkplain StreamObserver#onError faulty} stream completion.
 *
 * @see FirestoreSubscriptionPublisher
 */
abstract class SubscriptionUpdateObserver<U>
        implements StreamObserver<SubscriptionUpdate>, Logging {

    private final String path;
    private final FirestoreSubscriptionPublisher<U> publisher;

    SubscriptionUpdateObserver(CollectionReference target,
                               FirestoreSubscriptionPublisher<U> publisher) {
        checkNotNull(target);
        this.path = target.getPath();
        this.publisher = publisher;
    }

    static SubscriptionUpdateObserver<?> create(Topic topic, CollectionReference target) {
        if (hasEventAsTarget(topic)) {
            return new EventUpdateObserver(target);
        }
        return new EntityUpdateObserver(target);
    }

    @Override
    public void onNext(SubscriptionUpdate value) {
        Collection<U> payload = extractUpdates(value);
        publisher.publish(payload);
    }

    @Override
    public void onError(Throwable error) {
        log().error(format("Subscription with target `%s` has been completed with an error.", path),
                    error);
    }

    @Override
    public void onCompleted() {
        log().info("Subscription with target `{}` has been completed.", path);
    }

    protected abstract Collection<U> extractUpdates(SubscriptionUpdate value);

    private static boolean hasEventAsTarget(Topic topic) {
        String typeUrl = topic.getTarget()
                              .getType();
        Class<?> targetClass = TypeUrl.parse(typeUrl)
                                      .getJavaClass();
        boolean result = EventMessage.class.isAssignableFrom(targetClass);
        return result;
    }
}
