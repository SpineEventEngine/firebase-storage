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
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.WriteBatch;
import io.spine.logging.Logging;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Publishes {@link io.spine.client.SubscriptionUpdate}s to <a target="_blank"
 * href="https://firebase.google.com/docs/firestore/">Cloud Firestore^</a>.
 *
 * <p>The descendants elaborate on how to compose the document identifier and body for this
 * particular subscription.
 *
 * @param <U>
 *         the type of the published update
 */
abstract class FirestorePublisher<U> implements Logging {

    /**
     * The collection in the Cloud Firestore dedicated for storing subscription updates.
     */
    private final CollectionReference collection;

    FirestorePublisher(CollectionReference databaseSlice) {
        this.collection = checkNotNull(databaseSlice);
    }

    /**
     * Publishes the given updates into the Cloud Firestore.
     *
     * <p>Each update causes either a new document creation or an existent document update under
     * the given {@code CollectionReference}.
     *
     * @param updates
     *         updates to publish to the Firestore
     */
    void publish(Iterable<U> updates) {
        checkNotNull(updates);
        WriteBatch batch = collection.getFirestore()
                                     .batch();
        for (U update : updates) {
            write(batch, update);
        }
        Future<?> writeResult = batch.commit();
        waitFor(writeResult);
    }

    private void write(WriteBatch batch, U update) {
        String identifier = composeDocumentIdentifier(update);
        Map<String, Object> data = composeDocumentBody(update);
        DocumentReference document = documentFor(identifier);
        log().info("Writing a subscription update with identifier {} into the Firestore " +
                           "location {}.", identifier, document.getPath());
        batch.set(document, data);
    }

    private DocumentReference documentFor(String identifier) {
        String documentKey = DocumentKeys.escape(identifier);
        DocumentReference result = collection.document(documentKey);
        return result;
    }

    /**
     * Composes an identifier for the Firestore document from the given update.
     */
    protected abstract String composeDocumentIdentifier(U update);

    /**
     * Composes a Firestore document body which reflects the given update.
     */
    protected abstract Map<String, Object> composeDocumentBody(U update);

    /**
     * Blocks the current thread waiting for the given {@link Future} and logs all the caught
     * exceptions.
     *
     * @param future
     *         the future to wait for
     */
    private void waitFor(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log().error("Unable to publish update:", e);
        }
    }
}
