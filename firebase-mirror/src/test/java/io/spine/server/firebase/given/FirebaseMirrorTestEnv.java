/*
 * Copyright 2018, TeamDev Ltd. All rights reserved.
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

package io.spine.server.firebase.given;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.common.base.Supplier;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.client.ActorRequestFactory;
import io.spine.client.CommandFactory;
import io.spine.core.BoundedContextName;
import io.spine.core.Command;
import io.spine.core.CommandEnvelope;
import io.spine.core.Event;
import io.spine.core.EventContext;
import io.spine.core.Subscribe;
import io.spine.core.TenantId;
import io.spine.core.UserId;
import io.spine.people.PersonName;
import io.spine.server.BoundedContext;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateMessageDispatcher;
import io.spine.server.aggregate.AggregateRepository;
import io.spine.server.aggregate.Apply;
import io.spine.server.command.Assign;
import io.spine.server.command.TestEventFactory;
import io.spine.server.entity.Entity;
import io.spine.server.entity.Repository;
import io.spine.server.firebase.FMChangeCustomerName;
import io.spine.server.firebase.FMCreateCustomer;
import io.spine.server.firebase.FMCustomer;
import io.spine.server.firebase.FMCustomerCreated;
import io.spine.server.firebase.FMCustomerId;
import io.spine.server.firebase.FMCustomerNameChanged;
import io.spine.server.firebase.FMCustomerVBuilder;
import io.spine.server.firebase.FMSession;
import io.spine.server.firebase.FMSessionId;
import io.spine.server.firebase.FMSessionVBuilder;
import io.spine.server.firebase.FirebaseSubscriptionMirror;
import io.spine.server.projection.Projection;
import io.spine.server.projection.ProjectionRepository;
import io.spine.server.stand.Stand;
import io.spine.server.storage.StorageFactory;
import io.spine.string.Stringifier;
import io.spine.string.StringifierRegistry;
import io.spine.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.ofInstance;
import static io.spine.Identifier.newUuid;
import static io.spine.client.TestActorRequestFactory.newInstance;
import static io.spine.server.BoundedContext.newName;
import static io.spine.server.projection.ProjectionEventDispatcher.dispatch;
import static io.spine.server.storage.memory.InMemoryStorageFactory.newInstance;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNotNull;

/**
 * Test environment for the {@link FirebaseSubscriptionMirror FirebaseSubscriptionMirror} tests.
 *
 * @author Dmytro Dashenkov
 */
public final class FirebaseMirrorTestEnv {

    private static final String FIREBASE_SERVICE_ACC_SECRET = "serviceAccount.json";
    private static final String DATABASE_URL = "https://spine-firestore-test.firebaseio.com";

    private static final UserId TEST_ACTOR = UserId.newBuilder()
                                                   .setValue("Firebase mirror test")
                                                   .build();
    private static final ActorRequestFactory defaultRequestFactory = newInstance(TEST_ACTOR);
    private static final TestEventFactory eventFactory =
            TestEventFactory.newInstance(FirebaseMirrorTestEnv.class);

    @Nullable
    private static final Firestore firestore = tryCreateFirestore();

    // Prevent utility class instantiation.
    private FirebaseMirrorTestEnv() {
    }

    public static FMCustomerId newId() {
        return FMCustomerId.newBuilder()
                           .setUid(newUuid())
                           .build();
    }

    public static FMSessionId newSessionId() {
        return FMSessionId.newBuilder()
                          .setCustomerId(newId())
                          .setStartTime(Time.getCurrentTime())
                          .build();
    }

    public static Firestore getFirestore() {
        assumeNotNull(firestore);
        return firestore;
    }

    @Nullable
    private static Firestore tryCreateFirestore() {
        final InputStream firebaseSecret = FirebaseMirrorTestEnv.class
                .getClassLoader()
                .getResourceAsStream(FIREBASE_SERVICE_ACC_SECRET);
        if (firebaseSecret == null) {
            return null;
        } else {
            final GoogleCredentials credentials;
            try {
                credentials = GoogleCredentials.fromStream(firebaseSecret);
            } catch (IOException e) {
                log().error("Error while reading Firebase service account file.", e);
                throw new IllegalStateException(e);
            }
            return createFirestore(credentials);
        }
    }

    private static Firestore createFirestore(GoogleCredentials credentials) {
        final FirebaseOptions options = new FirebaseOptions.Builder()
                .setDatabaseUrl(DATABASE_URL)
                .setCredentials(credentials)
                .build();
        FirebaseApp.initializeApp(options);
        final Firestore firestore = FirestoreClient.getFirestore();
        return firestore;
    }

    public static void registerSessionIdStringifier() {
        final Stringifier<FMSessionId> stringifier = new Stringifier<FMSessionId>() {
            private static final String SEPARATOR = "::";

            @Override
            protected String toString(FMSessionId genericId) {
                final String customerUid = genericId.getCustomerId()
                                                    .getUid();
                final String timestamp = Timestamps.toString(genericId.getStartTime());
                return customerUid + SEPARATOR + timestamp;
            }

            @Override
            protected FMSessionId fromString(String stringId) {
                @SuppressWarnings("DynamicRegexReplaceableByCompiledPattern") // OK for tests.
                final String[] parts = stringId.split(SEPARATOR);
                checkArgument(parts.length == 2);
                final FMCustomerId customerId = FMCustomerId.newBuilder()
                                                            .setUid(parts[0])
                                                            .build();
                final Timestamp timestamp;
                try {
                    timestamp = Timestamps.parse(parts[1]);
                } catch (ParseException e) {
                    throw new IllegalArgumentException(e);
                }
                final FMSessionId result = FMSessionId.newBuilder()
                                                      .setCustomerId(customerId)
                                                      .setStartTime(timestamp)
                                                      .build();
                return result;
            }
        };
        StringifierRegistry.getInstance()
                           .register(stringifier, FMSessionId.class);
    }

    public static FMCustomer createCustomer(FMCustomerId customerId,
                                            BoundedContext boundedContext) {
        return createCustomer(customerId, boundedContext, defaultRequestFactory);
    }

    public static void createSession(FMSessionId sessionId, BoundedContext boundedContext) {
        final SessionProjection projection =
                createEntity(sessionId, boundedContext, FMSession.class);
        final FMCustomerCreated eventMsg = createdEvent(sessionId.getCustomerId());
        final Event event = eventFactory.createEvent(eventMsg);
        dispatch(projection, event);
        final Stand stand = boundedContext.getStand();
        stand.post(defaultTenant(), projection);
    }

    public static FMCustomer createCustomer(FMCustomerId customerId,
                                            BoundedContext boundedContext,
                                            TenantId tenantId) {
        return createCustomer(customerId, boundedContext, requestFactory(tenantId));
    }

    private static FMCustomer createCustomer(FMCustomerId customerId,
                                             BoundedContext boundedContext,
                                             ActorRequestFactory requestFactory) {
        final CustomerAggregate aggregate =
                createEntity(customerId, boundedContext, FMCustomer.class);
        final CommandFactory commandFactory = requestFactory.command();

        final FMCreateCustomer createCmd = createCommand(customerId);
        final FMChangeCustomerName updateCmd = updateCommand(customerId);
        dispatchCommand(aggregate, createCmd, commandFactory);
        dispatchCommand(aggregate, updateCmd, commandFactory);
        final Stand stand = boundedContext.getStand();
        final TenantId tenantId = requestFactory.getTenantId();
        stand.post(tenantId == null ? defaultTenant() : tenantId, aggregate);
        return aggregate.getState();
    }

    private static void dispatchCommand(Aggregate<?, ?, ?> aggregate,
                                        Message command,
                                        CommandFactory factory) {
        final Command cmd = factory.create(command);
        final CommandEnvelope envelope = CommandEnvelope.of(cmd);
        AggregateMessageDispatcher.dispatchCommand(aggregate, envelope);
    }

    private static <I, E extends Entity<I, S>, S extends Message> E
    createEntity(I id, BoundedContext boundedContext, Class<S> stateClass) {
        @SuppressWarnings("unchecked") final Repository<I, E> repository =
                boundedContext.findRepository(stateClass)
                              .orNull();
        assertNotNull(repository);
        final E projection = repository.create(id);
        return projection;
    }

    private static ActorRequestFactory requestFactory(TenantId tenantId) {
        final ActorRequestFactory factory = newInstance(TEST_ACTOR, tenantId);
        return factory;
    }

    private static FMCreateCustomer createCommand(FMCustomerId id) {
        final FMCreateCustomer createCmd = FMCreateCustomer.newBuilder()
                                                           .setId(id)
                                                           .build();
        return createCmd;
    }

    private static FMChangeCustomerName updateCommand(FMCustomerId id) {
        final PersonName newName = PersonName.newBuilder()
                                             .setGivenName("John")
                                             .setFamilyName("Doe")
                                             .build();
        final FMChangeCustomerName updateCmd = FMChangeCustomerName.newBuilder()
                                                                   .setId(id)
                                                                   .setNewName(newName)
                                                                   .build();
        return updateCmd;
    }

    private static FMCustomerCreated createdEvent(FMCustomerId id) {
        final FMCustomerCreated createCmd = FMCustomerCreated.newBuilder()
                                                             .setId(id)
                                                             .build();
        return createCmd;
    }

    private static TenantId defaultTenant() {
        return TenantId.newBuilder()
                       .setValue("Default tenant")
                       .build();
    }

    public static BoundedContext createBoundedContext(String name, boolean multitenant) {
        final BoundedContextName contextName = newName(name);
        final StorageFactory storageFactory = newInstance(contextName, multitenant);
        final Supplier<StorageFactory> storageSupplier = ofInstance(storageFactory);
        final BoundedContext result = BoundedContext.newBuilder()
                                                    .setName(name)
                                                    .setMultitenant(multitenant)
                                                    .setStorageFactorySupplier(storageSupplier)
                                                    .build();
        result.register(new CustomerRepository());
        result.register(new SessionRepository());
        return result;
    }

    public static class CustomerAggregate
            extends Aggregate<FMCustomerId, FMCustomer, FMCustomerVBuilder> {

        protected CustomerAggregate(FMCustomerId id) {
            super(id);
        }

        @SuppressWarnings("unused") // Reflective access.
        @Assign
        FMCustomerCreated handle(FMCreateCustomer command) {
            return createdEvent(command.getId());
        }

        @SuppressWarnings("unused") // Reflective access.
        @Assign
        FMCustomerNameChanged handle(FMChangeCustomerName command) {
            return FMCustomerNameChanged.newBuilder()
                                        .setNewName(command.getNewName())
                                        .build();
        }

        @SuppressWarnings("unused") // Reflective access.
        @Apply
        private void on(FMCustomerCreated event) {
            getBuilder().setId(event.getId());
        }

        @SuppressWarnings("unused") // Reflective access.
        @Apply
        private void on(FMCustomerNameChanged event) {
            getBuilder().setName(event.getNewName());
        }
    }

    static class CustomerRepository
            extends AggregateRepository<FMCustomerId, CustomerAggregate> {}

    public static class SessionProjection
            extends Projection<FMSessionId, FMSession, FMSessionVBuilder> {

        protected SessionProjection(FMSessionId id) {
            super(id);
        }

        @SuppressWarnings("unused") // Reflective access.
        @Subscribe
        void on(FMCustomerCreated event, EventContext context) {
            getBuilder().setDuration(mockLogic(context));
        }

        private static Duration mockLogic(EventContext context) {
            final Timestamp currentTime = Time.getCurrentTime();
            final Timestamp eventTime = context.getTimestamp();
            final long durationSeconds = eventTime.getSeconds() - currentTime.getSeconds();
            final Duration duration = Duration.newBuilder()
                                              .setSeconds(durationSeconds)
                                              .build();
            return duration;
        }
    }

    static class SessionRepository
            extends ProjectionRepository<FMSessionId, SessionProjection, FMSession> {}

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(FirebaseMirrorTestEnv.class);
    }
}
