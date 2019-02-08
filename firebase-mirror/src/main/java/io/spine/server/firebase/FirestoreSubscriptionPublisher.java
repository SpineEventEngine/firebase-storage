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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.spine.base.Identifier;
import io.spine.client.EntityStateUpdate;
import io.spine.logging.Logging;
import io.spine.server.storage.StorageField;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.google.cloud.firestore.Blob.fromBytes;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.firebase.FirestoreSubscriptionPublisher.EntityStateField.bytes;
import static io.spine.server.firebase.FirestoreSubscriptionPublisher.EntityStateField.id;

/**
 * Publishes {@link EntityStateUpdate}s to <a target="_blank"
 * href="https://firebase.google.com/docs/firestore/">Cloud Firestore^</a>.
 */
final class FirestoreSubscriptionPublisher {

    /**
     * The collection in the Cloud Firestore dedicated for the entity state update storage.
     */
    private final CollectionReference collection;

    FirestoreSubscriptionPublisher(CollectionReference databaseSlice) {
        this.collection = checkNotNull(databaseSlice);
    }

    /**
     * Publishes the given {@link EntityStateUpdate}s into the Cloud Firestore.
     *
     * <p>Each {@code EntityStateUpdate} causes either a new document creation or an existent
     * document update under the given {@code CollectionReference}.
     *
     * @param updates
     *         updates to publish to the Firestore
     */
    void publish(Iterable<EntityStateUpdate> updates) {
        checkNotNull(updates);
        WriteBatch batch = collection.getFirestore()
                                     .batch();
        for (EntityStateUpdate update : updates) {
            write(batch, update);
        }
        Future<?> writeResult = batch.commit();
        waitFor(writeResult);
    }

    private void write(WriteBatch batch, EntityStateUpdate update) {
        Any updateId = update.getId();
        Any updateState = update.getState();
        Message entityId = unpack(updateId);
        Message message = unpack(updateState);
        String stringId = Identifier.toString(entityId);
        byte[] stateBytes = message.toByteArray();
        Map<String, Object> data = ImmutableMap.of(
                bytes.toString(), fromBytes(stateBytes),
                id.toString(), stringId
        );
        DocumentReference targetDocument = documentFor(stringId);
        log().info("Writing a state update of the type {} (id: {}) into the Firestore location {}.",
                   updateState.getTypeUrl(), stringId, targetDocument.getPath());
        batch.set(targetDocument, data);
    }

    private DocumentReference documentFor(String entityId) {
        String documentKey = DocumentKeys.escape(entityId);
        DocumentReference result = collection.document(documentKey);
        return result;
    }

    /**
     * Blocks the current thread waiting for the given {@link Future} and logs all the caught
     * exceptions.
     *
     * @param future
     *         the future to wait for
     */
    private static void waitFor(Future<?> future) {
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            log().error("Unable to publish update:", e);
        }
    }

    /**
     * The list of fields of the entity state as it is stored to Firestore.
     *
     * <p>The enum value names represent the names of the fields of an entity state record.
     */
    @VisibleForTesting
    enum EntityStateField implements StorageField {

        /**
         * The string field for the entity ID.
         *
         * <p>The ID is converted to {@code String} by the rules of
         * {@link io.spine.base.Identifier#toString(Object) Identifier.toString(id)}.
         */
        id,

        /**
         * The byte array representation of the entity state.
         *
         * @see Message#toByteArray()
         */
        bytes
    }

    private static Logger log() {
        return Logging.get(FirestoreSubscriptionPublisher.class);
    }
}
