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

import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import io.spine.base.Identifier;
import io.spine.client.EntityId;
import io.spine.client.EntityStateUpdate;
import io.spine.logging.Logging;
import io.spine.server.firebase.EntitySubscriptionPublisher.EntityStateField;
import io.spine.server.firebase.given.FirebaseMirrorTestEnv;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.getFirestore;
import static io.spine.util.Exceptions.newIllegalStateException;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@DisplayName("FirebaseSubscriptionPublisher should")
class FirebaseSubscriptionPublisherTest implements Logging {

    @SuppressWarnings("FutureReturnValueIgnored") // OK for test clean up.
    @Test
    @DisplayName("escape illegal chars in a document key")
    void escapeIllegalDocumentKey() throws ExecutionException,
                                           InterruptedException,
                                           InvalidProtocolBufferException {
        CollectionReference targetCollection = getFirestore().collection("test_records");
        EntitySubscriptionPublisher publisher =
                new EntitySubscriptionPublisher(targetCollection);
        String rawId = "___&$id001%-_foobar";
        String expectedId = "id001_foobar";
        Any id = Identifier.pack(rawId);
        EntityId entityId = EntityId
                .newBuilder()
                .setId(id)
                .build();
        Any packedEntityId = Identifier.pack(entityId);
        FMCustomer expectedState = FMCustomer
                .newBuilder()
                .setId(FirebaseMirrorTestEnv.newId())
                .build();
        Any state = pack(expectedState);
        EntityStateUpdate update = EntityStateUpdate
                .newBuilder()
                .setId(packedEntityId)
                .setState(state)
                .build();
        publisher.publish(singleton(update));
        DocumentSnapshot document = getDocumentSafe(targetCollection, expectedId);
        String entityStateId = document.getString(EntityStateField.id.toString());
        assertEquals(rawId, entityStateId);

        Blob stateBlob = document.getBlob(EntityStateField.bytes.toString());
        assertNotNull(state);
        FMCustomer actualState = FMCustomer.parseFrom(stateBlob.toBytes());
        assertEquals(expectedState, actualState);

        document.getReference()
                .delete();
    }

    private DocumentSnapshot getDocumentSafe(CollectionReference targetCollection,
                                             String expectedId) {
        try {
            log().warn("Trying to obtain the document for target collection.");
            return targetCollection.document(expectedId)
                                   .get()
                                   .get();
        } catch (Throwable t) {
            String errorMessage = format("Failed to obtain the document for target " +
                                           "collection, cause: %s", t.getLocalizedMessage());
            log().warn(errorMessage);
            throw newIllegalStateException(t, errorMessage);
        }
    }
}
