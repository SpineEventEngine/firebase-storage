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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.Message;
import io.spine.base.Identifier;
import io.spine.client.EntityId;
import io.spine.client.EntityStateUpdate;
import io.spine.server.storage.StorageField;

import java.util.Map;

import static com.google.cloud.firestore.Blob.fromBytes;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.firebase.EntityUpdatePublisher.EntityStateField.bytes;
import static io.spine.server.firebase.EntityUpdatePublisher.EntityStateField.id;

/**
 * An update publisher for the entity state updates.
 *
 * <p>The published document identifier is an entity ID and the document body stores a new entity
 * state.
 *
 * <p>For details on storage format, see {@link EntityStateField}.
 */
final class EntityUpdatePublisher extends FirestorePublisher<EntityStateUpdate> {

    EntityUpdatePublisher(CollectionReference databaseSlice) {
        super(databaseSlice);
    }

    @Override
    protected String composeDocumentIdentifier(EntityStateUpdate update) {
        Any updateId = update.getId();
        EntityId entityId = (EntityId) unpack(updateId);
        Any id = entityId.getId();
        Message originalId = unpack(id);
        String result = Identifier.toString(originalId);
        return result;
    }

    @Override
    protected Map<String, Object> composeDocumentBody(EntityStateUpdate update) {
        String stringId = composeDocumentIdentifier(update);
        Any updateState = update.getState();
        Message message = unpack(updateState);
        byte[] stateBytes = message.toByteArray();
        Map<String, Object> result = ImmutableMap.of(
                bytes.toString(), fromBytes(stateBytes),
                id.toString(), stringId
        );
        return result;
    }

    /**
     * The list of fields of the entity state as it is published to Firestore.
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
}
