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
 * <p>The descendants decide which part of the {@code SubscriptionUpdate} should be published and
 * how.
 *
 * <p>The implementation logs a message upon either
 * {@linkplain StreamObserver#onCompleted() successful} or
 * {@linkplain StreamObserver#onError faulty} stream completion.
 *
 * @param <U>
 *         the type of the observed update
 *
 * @see FirestorePublisher
 */
abstract class UpdateObserver<U>
        implements StreamObserver<SubscriptionUpdate>, Logging {

    private final String path;
    private final FirestorePublisher<U> publisher;

    UpdateObserver(CollectionReference target,
                   FirestorePublisher<U> publisher) {
        checkNotNull(target);
        this.path = target.getPath();
        this.publisher = publisher;
    }

    static UpdateObserver<?> create(Topic topic, CollectionReference target) {
        if (hasEventAsTarget(topic)) {
            return new EventObserver(target);
        }
        return new EntityUpdateObserver(target);
    }

    @Override
    public void onNext(SubscriptionUpdate value) {
        Collection<U> payload = extractUpdatePayload(value);
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

    /**
     * Extracts the part which should be published from a {@code SubscriptionUpdate} message.
     */
    protected abstract Collection<U> extractUpdatePayload(SubscriptionUpdate value);

    private static boolean hasEventAsTarget(Topic topic) {
        String typeUrl = topic.getTarget()
                              .getType();
        Class<?> targetClass = TypeUrl.parse(typeUrl)
                                      .toJavaClass();
        boolean result = EventMessage.class.isAssignableFrom(targetClass);
        return result;
    }
}
