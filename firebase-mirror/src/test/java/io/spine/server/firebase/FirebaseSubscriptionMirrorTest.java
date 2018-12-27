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

import com.google.cloud.firestore.Blob;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.testing.NullPointerTester;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.spine.base.Identifier;
import io.spine.client.ActorRequestFactory;
import io.spine.client.Topic;
import io.spine.core.Ack;
import io.spine.core.ActorContext;
import io.spine.core.BoundedContextName;
import io.spine.core.Event;
import io.spine.core.TenantId;
import io.spine.grpc.StreamObservers;
import io.spine.net.EmailAddress;
import io.spine.net.InternetDomain;
import io.spine.server.BoundedContext;
import io.spine.server.SubscriptionService;
import io.spine.server.firebase.given.FirebaseMirrorTestEnv;
import io.spine.server.integration.ExternalMessage;
import io.spine.server.tenant.TenantAdded;
import io.spine.string.Stringifier;
import io.spine.string.StringifierRegistry;
import io.spine.testing.server.TestEventFactory;
import io.spine.type.TypeUrl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Sets.newHashSet;
import static io.spine.base.Identifier.newUuid;
import static io.spine.protobuf.AnyPacker.pack;
import static io.spine.server.firebase.FirestoreSubscriptionPublisher.EntityStateField.bytes;
import static io.spine.server.firebase.FirestoreSubscriptionPublisher.EntityStateField.id;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.createBoundedContext;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.createCustomer;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.getFirestore;
import static io.spine.server.firebase.given.FirebaseMirrorTestEnv.newId;
import static io.spine.testing.DisplayNames.NOT_ACCEPT_NULLS;
import static io.spine.testing.client.TestActorRequestFactory.newInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * The {@link FirebaseSubscriptionMirror} tests.
 *
 * <p>These tests should be executed on CI only, as they rely on the {@code serviceAccount.json}
 * which is stored encrypted in the Git repository and is decrypted on CI with private environment
 * keys.
 *
 * <p>To run the tests locally, go to the Firebase console, create a new service account and save
 * the generated {@code .json} file as
 * {@code firebase-mirror/src/test/resources/serviceAccount.json}. Then run the tests from IDE.
 */
@SuppressWarnings("ClassWithTooManyMethods")
@DisplayName("FirebaseSubscriptionMirror should")
class FirebaseSubscriptionMirrorTest {

    /**
     * The {@link Firestore} instance to access from the mirror.
     *
     * <p>This field is not {@code final} to make it possible to initialize it in
     * {@link org.junit.Before \@Before} methods.
     *
     * <p>This field is declared {@code static} to make it accessible in {@link org.junit.AfterClass
     * \@AfterClass} methods for the test data clean up.
     */
    private static Firestore firestore;

    private static final TypeUrl CUSTOMER_TYPE = TypeUrl.of(FMCustomer.class);
    private static final TypeUrl SESSION_TYPE = TypeUrl.of(FMSession.class);

    private final ActorRequestFactory requestFactory =
            newInstance(FirebaseSubscriptionMirrorTest.class);
    private FirebaseSubscriptionMirror mirror;
    private BoundedContext boundedContext;
    private SubscriptionService subscriptionService;

    /**
     * Stores all the {@link DocumentReference} instances used for the test suite.
     *
     * <p>It is required to clean up all the data in Cloud Firestore to avoid test failures.
     */
    private static final Collection<DocumentReference> documents = newHashSet();
    private final TestEventFactory eventFactory =
            TestEventFactory.newInstance(FirebaseSubscriptionMirrorTest.class);

    @AfterAll
    static void afterAll() throws ExecutionException, InterruptedException {
        final WriteBatch batch = getFirestore().batch();
        for (DocumentReference document : documents) {
            batch.delete(document);
        }
        // Submit the deletion operations and ensure execution.
        batch.commit()
             .get();
        documents.clear();
    }

    @BeforeEach
    void beforeEach() {
        firestore = getFirestore();
        initializeEnvironment(false);
    }

    @Test
    @DisplayName(NOT_ACCEPT_NULLS)
    void passNullToleranceCheck() {
        new NullPointerTester()
                .setDefault(TenantId.class, TenantId.getDefaultInstance())
                .setDefault(Topic.class, Topic.getDefaultInstance())
                .testAllPublicInstanceMethods(mirror);
    }

    @Test
    @DisplayName("not allow nulls on construction")
    void rejectNullsOnConstruction() {
        new NullPointerTester()
                .testAllPublicInstanceMethods(FirebaseSubscriptionMirror.newBuilder());
    }

    @Test
    @DisplayName("accept either Firestore or DocumentReference on construction")
    void acceptFirestoreOrDocument() {
        final DocumentReference location = firestore.collection("test_collection")
                                                    .document("test_document");
        final FirebaseSubscriptionMirror.Builder builder =
                FirebaseSubscriptionMirror.newBuilder()
                                          .setSubscriptionService(subscriptionService)
                                          .setFirestore(firestore)
                                          .addBoundedContext(boundedContext)
                                          .setFirestoreDocument(location);
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    @DisplayName("require at least one BoundedContext on construction")
    void requireBoundedContext() {
        final FirebaseSubscriptionMirror.Builder builder =
                FirebaseSubscriptionMirror.newBuilder()
                                          .setSubscriptionService(subscriptionService)
                                          .setFirestore(firestore);
        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    @DisplayName("allow not specifying a SubscriptionService")
    void allowNoSubscriptionService() {
        final FirebaseSubscriptionMirror mirror =
                FirebaseSubscriptionMirror.newBuilder()
                                          .addBoundedContext(boundedContext)
                                          .setFirestore(firestore)
                                          .build();
        mirror.reflect(CUSTOMER_TYPE);
    }

    @Test
    @DisplayName("allow to specify a custom SubscriptionService")
    void allowCustomSubscriptionService() {
        final SubscriptionService spy = spy(subscriptionService);
        final FirebaseSubscriptionMirror mirror =
                FirebaseSubscriptionMirror.newBuilder()
                                          .addBoundedContext(boundedContext)
                                          .setSubscriptionService(spy)
                                          .setFirestore(firestore)
                                          .build();
        mirror.reflect(CUSTOMER_TYPE);
        verify(spy).subscribe(any(Topic.class), any(SubscriptionObserver.class));
    }

    @Test
    @DisplayName("deliver the entity state updates")
    void deliverTheEntityStateUpdates() throws ExecutionException, InterruptedException {
        mirror.reflect(CUSTOMER_TYPE);
        final FMCustomerId customerId = newId();
        final FMCustomer expectedState = createCustomer(customerId, boundedContext);
        final FMCustomer actualState = findCustomer(customerId, inRoot());
        assertEquals(expectedState, actualState);
    }

    @Test
    @DisplayName("transform ID to string with the proper Stringifier")
    void transformIDToString() throws ExecutionException, InterruptedException {
        FirebaseMirrorTestEnv.registerSessionIdStringifier();
        mirror.reflect(SESSION_TYPE);
        final FMSessionId sessionId = FirebaseMirrorTestEnv.newSessionId();
        FirebaseMirrorTestEnv.createSession(sessionId, boundedContext);
        final DocumentSnapshot document = findDocument(FMSession.class,
                                                       sessionId,
                                                       inRoot());
        final String actualId = document.getString(id.toString());
        final Stringifier<FMSessionId> stringifier =
                StringifierRegistry.getInstance()
                                   .<FMSessionId>get(FMSessionId.class)
                                   .orElse(null);
        assertNotNull(stringifier);
        final FMSessionId readId = stringifier.reverse().convert(actualId);
        assertEquals(sessionId, readId);
    }

    @Test
    @DisplayName("partition records of different tenants")
    void partitionRecords() throws ExecutionException, InterruptedException {
        initializeEnvironment(true);
        final InternetDomain tenantDomain = InternetDomain.newBuilder()
                                                          .setValue("example.org")
                                                          .build();
        final EmailAddress tenantEmail = EmailAddress.newBuilder()
                                                     .setValue("user@example.org")
                                                     .build();
        final TenantId firstTenant = TenantId.newBuilder()
                                             .setDomain(tenantDomain)
                                             .build();
        final TenantId secondTenant = TenantId.newBuilder()
                                              .setEmail(tenantEmail)
                                              .build();
        boundedContext.getTenantIndex()
                      .keep(firstTenant);
        mirror.reflect(CUSTOMER_TYPE);
        final FMCustomerId customerId = newId();
        createCustomer(customerId, boundedContext, secondTenant);
        final com.google.common.base.Optional<?> document =
                tryFindDocument(CUSTOMER_TYPE.getMessageClass(),
                                customerId,
                                inRoot());
        assertFalse(document.isPresent());
    }

    @Test
    @DisplayName("allow to specify a custom document to work with")
    void allowCustomDocument() throws ExecutionException,
                                                                  InterruptedException {
        final DocumentReference customLocation = firestore.document("custom/location");
        final FirebaseSubscriptionMirror mirror =
                FirebaseSubscriptionMirror.newBuilder()
                                          .setSubscriptionService(subscriptionService)
                                          .setFirestoreDocument(customLocation)
                                          .addBoundedContext(boundedContext)
                                          .build();
        mirror.reflect(CUSTOMER_TYPE);
        final FMCustomerId customerId = newId();
        final FMCustomer expectedState = createCustomer(customerId, boundedContext);
        final FMCustomer actualState = findCustomer(customerId, inDoc(customLocation));
        assertEquals(expectedState, actualState);
    }

    @Test
    @DisplayName("allow to specify custom document per topic")
    void allowCustomDocumentPerTopic() throws ExecutionException,
                                                             InterruptedException {
        final Function<Topic, DocumentReference> rule = new Function<Topic, DocumentReference>() {
            @Override
            public DocumentReference apply(@Nullable Topic topic) {
                return firestore.collection("custom_subscription")
                                .document("location");
            }
        };
        final FirebaseSubscriptionMirror mirror =
                FirebaseSubscriptionMirror.newBuilder()
                                          .setSubscriptionService(subscriptionService)
                                          .setReflectionRule(rule)
                                          .addBoundedContext(boundedContext)
                                          .build();
        mirror.reflect(CUSTOMER_TYPE);
        final FMCustomerId customerId = newId();
        final FMCustomer expectedState = createCustomer(customerId, boundedContext);
        final Topic topic = requestFactory.topic().allOf(CUSTOMER_TYPE.getMessageClass());
        final DocumentReference expectedDocument = rule.apply(topic);
        final FMCustomer actualState = findCustomer(customerId, inDoc(expectedDocument));
        assertEquals(expectedState, actualState);
    }

    @Test
    @DisplayName("start reflecting for newly created tenants")
    void reflectForNewTenants() throws ExecutionException, InterruptedException {
        initializeEnvironment(true);
        mirror.reflect(CUSTOMER_TYPE);
        assertTrue(boundedContext.getTenantIndex()
                                 .getAll()
                                 .isEmpty());
        final TenantId newTenant = TenantId.newBuilder()
                                           .setValue(newUuid())
                                           .build();
        addTenant(newTenant);
        final FMCustomerId id = newId();
        createCustomer(id, boundedContext, newTenant);
        final FMCustomer readState = findCustomer(id, inRoot());
        assertNotNull(readState);
    }

    private void addTenant(TenantId tenantId) {
        final TenantAdded eventMsg = TenantAdded.newBuilder()
                                                .setId(tenantId)
                                                .build();
        final Event event = eventFactory.createEvent(eventMsg);
        final ActorContext actorContext = event.getContext()
                                               .getCommandContext()
                                               .getActorContext();
        final BoundedContextName contextName = boundedContext.getName();
        final Any id = pack(event.getId());
        final ExternalMessage externalMessage = ExternalMessage.newBuilder()
                                                               .setBoundedContextName(contextName)
                                                               .setId(id)
                                                               .setOriginalMessage(pack(event))
                                                               .setActorContext(actorContext)
                                                               .build();
        boundedContext.getIntegrationBus()
                      .post(externalMessage, StreamObservers.<Ack>noOpObserver());
    }

    private void initializeEnvironment(boolean multitenant) {
        final String name = FirebaseSubscriptionMirrorTest.class.getSimpleName();
        boundedContext = createBoundedContext(name, multitenant);
        subscriptionService = SubscriptionService.newBuilder()
                                                 .add(boundedContext)
                                                 .build();
        mirror = FirebaseSubscriptionMirror.newBuilder()
                                           .setFirestore(firestore)
                                           .setSubscriptionService(subscriptionService)
                                           .addBoundedContext(boundedContext)
                                           .build();
    }

    /**
     * Finds a {@code FMCustomer} with the given ID.
     *
     * <p>The collection of {@code FMCustomer} records is retrieved with the given
     * {@code collectionAccess} function.
     *
     * <p>Note that the {@code collectionAccess} accepts a short name of the collection (not
     * the whole path).
     *
     * @param id               the {@code FMCustomer} ID to search by
     * @param collectionAccess a function retrieving
     *                         the {@linkplain CollectionReference collection} which holds the
     *                         {@code FMCustomer}
     * @return the found {@code FMCustomer}
     */
    private static FMCustomer findCustomer(FMCustomerId id,
                                           Function<String, CollectionReference> collectionAccess)
            throws ExecutionException,
                   InterruptedException {
        final DocumentSnapshot document = findDocument(FMCustomer.class, id, collectionAccess);
        final FMCustomer customer = deserialize(document);
        return customer;
    }

    /**
     * Finds a {@link DocumentReference} containing the given ID.
     *
     * <p>Unlike {@link #tryFindDocument(Class, Message, Function)}, this method throws
     * a {@link NoSuchElementException} if the searched document is not found.
     *
     * @see #tryFindDocument(Class, Message, Function)
     */
    private static DocumentSnapshot
    findDocument(Class<? extends Message> msgClass, Message id,
                 Function<String, CollectionReference> collectionAccess)
            throws ExecutionException,
                   InterruptedException {
        final Optional<DocumentSnapshot> result = tryFindDocument(msgClass, id, collectionAccess);
        assertTrue(result.isPresent());
        return result.get();
    }

    /**
     * Finds a {@link DocumentReference} containing the given ID.
     *
     * <p>The document is looked up in the {@linkplain CollectionReference collection} returned by
     * the given {@code collectionAccess} function. The collection should have the Protobuf type
     * name of the message of the specified {@code msgClass}.
     *
     * @param msgClass         the type of the message stored in the searched document
     * @param id               the ID of the message stored in the searched document
     * @param collectionAccess a function retrieving
     *                         the {@linkplain CollectionReference collection} which holds the
     *                         document
     * @return the searched document or {@code Optional.empty()} if no such document is found
     */
    private static Optional<DocumentSnapshot>
    tryFindDocument(Class<? extends Message> msgClass, Message id,
                    Function<String, CollectionReference> collectionAccess)
            throws ExecutionException,
                   InterruptedException {
        final TypeUrl typeUrl = TypeUrl.of(msgClass);
        final String collectionName = typeUrl.getPrefix() + '_' + typeUrl.getTypeName();
        final QuerySnapshot collection = collectionAccess.apply(collectionName)
                                                         .get().get();
        Optional<DocumentSnapshot> result = Optional.absent();
        for (DocumentSnapshot doc : collection.getDocuments()) {
            documents.add(doc.getReference());
            if (idEquals(doc, id)) {
                result = Optional.of(doc);
            }
        }
        return result;
    }

    private static boolean idEquals(DocumentSnapshot document, Message customerId) {
        final Object actualId = document.get(id.toString());
        final String expectedIdString = Identifier.toString(customerId);
        return expectedIdString.equals(actualId);
    }

    private static FMCustomer deserialize(DocumentSnapshot document) {
        final Blob blob = document.getBlob(bytes.toString());
        assertNotNull(blob);
        final byte[] bytes = blob.toBytes();
        try {
            final FMCustomer result = FMCustomer.parseFrom(bytes);
            return result;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Function<String, CollectionReference> inRoot() {
        return new Function<String, CollectionReference>() {
            @Override
            public CollectionReference apply(@Nullable String path) {
                assertNotNull(path);
                final CollectionReference result = firestore.collection(path);
                return result;
            }
        };
    }

    private static Function<String, CollectionReference> inDoc(final DocumentReference doc) {
        return new Function<String, CollectionReference>() {
            @Override
            public CollectionReference apply(@Nullable String path) {
                assertNotNull(path);
                final CollectionReference result = doc.collection(path);
                return result;
            }
        };
    }
}
