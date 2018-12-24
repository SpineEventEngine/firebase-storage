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

import com.google.cloud.firestore.CollectionReference;
import com.google.common.testing.NullPointerTester;
import io.grpc.stub.StreamObserver;
import io.spine.server.entity.rejection.EntityAlreadyArchived;
import org.junit.Test;

import java.io.IOException;

import static io.spine.protobuf.TypeConverter.toAny;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.getFirestore;

/**
 * @author Dmytro Dashenkov
 */
public class SubscriptionUpdateObserverShould {

    @Test
    public void ignore_error() {
        final StreamObserver<?> observer = new SubscriptionUpdateObserver(target());
        final String testMessage = SubscriptionUpdateObserverShould.class.getSimpleName();
        EntityAlreadyArchived rejection = EntityAlreadyArchived
                .newBuilder()
                .setEntityId(toAny(testMessage))
                .build();
        observer.onError(new IllegalArgumentException(testMessage)); // Unchecked exception.
        observer.onError(new IOException(testMessage)); // Checked exception.
        observer.onError(new OutOfMemoryError(testMessage)); // JVM error.
        observer.onError(rejection); // Rejection throwable.
    }

    @Test
    public void ignore_completion() {
        final StreamObserver<?> observer = new SubscriptionUpdateObserver(target());
        observer.onCompleted();
        observer.onCompleted();
    }

    @Test
    public void not_accept_nulls_on_construction() {
        new NullPointerTester().testAllPublicConstructors(SubscriptionUpdateObserver.class);
    }

    @Test
    public void not_accept_null_arguments() throws NoSuchMethodException {
        final StreamObserver<?> observer = new SubscriptionUpdateObserver(target());
        new NullPointerTester()
                .ignore(SubscriptionUpdateObserver.class.getMethod("onError", Throwable.class))
                .testAllPublicInstanceMethods(observer);
    }

    private static CollectionReference target() {
        return getFirestore().collection("test");
    }
}
