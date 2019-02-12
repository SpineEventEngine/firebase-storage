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
import com.google.protobuf.Message;
import com.google.protobuf.util.Timestamps;
import io.spine.base.Identifier;
import io.spine.core.Event;
import io.spine.core.EventContext;
import io.spine.server.storage.StorageField;

import java.util.Map;

import static com.google.cloud.firestore.Blob.fromBytes;
import static io.spine.protobuf.AnyPacker.unpack;
import static io.spine.server.firebase.EntitySubscriptionPublisher.EntityStateField.bytes;
import static io.spine.server.firebase.EntitySubscriptionPublisher.EntityStateField.id;
import static io.spine.server.firebase.EventSubscriptionPublisher.EventStateField.producer_id;
import static io.spine.server.firebase.EventSubscriptionPublisher.EventStateField.timestamp;

/**
 * A subscription update publisher for the event subscriptions.
 *
 * <p>The document identifier is an event ID and the composed document body contains the serialized
 * event message as well as some meta-information.
 *
 * <p>See {@link EventStateField} for the details on storage format.
 */
final class EventSubscriptionPublisher extends FirestoreSubscriptionPublisher<Event> {

    EventSubscriptionPublisher(CollectionReference databaseSlice) {
        super(databaseSlice);
    }

    @Override
    protected String composeDocumentIdentifier(Event update) {
        String result = update.getId()
                              .getValue();
        return result;
    }

    @Override
    protected Map<String, Object> composeDocumentBody(Event update) {
        String eventId = composeDocumentIdentifier(update);
        EventContext context = update.getContext();
        Object producerId = Identifier.unpack(context.getProducerId());
        String producerIdString = Identifier.toString(producerId);
        String timestampString = Timestamps.toString(context.getTimestamp());
        Message eventMessage = unpack(update.getMessage());
        byte[] messageBytes = eventMessage.toByteArray();

        Map<String, Object> result = ImmutableMap.of(
                bytes.toString(), fromBytes(messageBytes),
                timestamp.toString(), timestampString,
                producer_id.toString(), producerIdString,
                id.toString(), eventId
        );
        return result;
    }

    /**
     * The format in which the occurred event is stored to Firestore.
     *
     * <p>The enum value names represent the names of the fields of an event record.
     */
    @VisibleForTesting
    enum EventStateField implements StorageField {

        /**
         * The raw event ID.
         */
        id,

        /**
         * The ID of the entity which produced the event.
         *
         * <p>The ID is converted to {@code String} by the rules of
         * {@link io.spine.base.Identifier#toString(Object) Identifier.toString(id)}.
         */
        producer_id,

        /**
         * The timestamp of the event in a {@code String} form.
         */
        timestamp,

        /**
         * The byte array representation of the event message.
         *
         * @see Message#toByteArray()
         */
        bytes
    }
}
