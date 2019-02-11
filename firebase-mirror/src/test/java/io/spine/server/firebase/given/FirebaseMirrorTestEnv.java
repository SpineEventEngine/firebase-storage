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

package io.spine.server.firebase.given;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.common.base.Splitter;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.protobuf.Duration;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.spine.base.CommandMessage;
import io.spine.base.Time;
import io.spine.client.ActorRequestFactory;
import io.spine.client.CommandFactory;
import io.spine.core.BoundedContextName;
import io.spine.core.Command;
import io.spine.core.CommandEnvelope;
import io.spine.core.Event;
import io.spine.core.EventContext;
import io.spine.core.EventId;
import io.spine.core.Subscribe;
import io.spine.core.TenantId;
import io.spine.core.UserId;
import io.spine.logging.Logging;
import io.spine.people.PersonName;
import io.spine.server.BoundedContext;
import io.spine.server.aggregate.Aggregate;
import io.spine.server.aggregate.AggregateRepository;
import io.spine.server.aggregate.Apply;
import io.spine.server.command.Assign;
import io.spine.server.entity.Entity;
import io.spine.server.entity.EntityLifecycle;
import io.spine.server.entity.Repository;
import io.spine.server.event.EventBus;
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
import io.spine.server.tenant.TenantAwareOperation;
import io.spine.string.Stringifier;
import io.spine.string.StringifierRegistry;
import io.spine.testing.server.TestEventFactory;
import io.spine.testing.server.aggregate.AggregateMessageDispatcher;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.spine.base.Identifier.newUuid;
import static io.spine.core.BoundedContextNames.newName;
import static io.spine.server.storage.memory.InMemoryStorageFactory.newInstance;
import static io.spine.testing.client.TestActorRequestFactory.newInstance;
import static io.spine.testing.server.projection.ProjectionEventDispatcher.dispatch;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeNotNull;

/**
 * Test environment for the {@link FirebaseSubscriptionMirror FirebaseSubscriptionMirror} tests.
 */
@SuppressWarnings({"unused" /* A lot of methods with reflective access only. */,
        "deprecation" /* Deprecated `Stand.post(...)` will become test-only in the future. */})
public final class FirebaseMirrorTestEnv {

    private static final String FIREBASE_SERVICE_ACC_SECRET = "serviceAccount.json";
    private static final String DATABASE_URL = "https://spine-firestore-test.firebaseio.com";

    private static final UserId TEST_ACTOR = UserId
            .newBuilder()
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
        InputStream firebaseSecret = FirebaseMirrorTestEnv.class
                .getClassLoader()
                .getResourceAsStream(FIREBASE_SERVICE_ACC_SECRET);
        if (firebaseSecret == null) {
            return null;
        } else {
            GoogleCredentials credentials;
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
        FirebaseOptions options = new FirebaseOptions
                .Builder()
                .setCredentials(credentials)
                .build();
        FirebaseApp.initializeApp(options);
        Firestore firestore = FirestoreClient.getFirestore();
        return firestore;
    }

    public static void registerSessionIdStringifier() {
        Stringifier<FMSessionId> stringifier = new Stringifier<FMSessionId>() {
            private static final String SEPARATOR = "::";

            @Override
            protected String toString(FMSessionId genericId) {
                String customerUid = genericId.getCustomerId()
                                              .getUid();
                String timestamp = Timestamps.toString(genericId.getStartTime());
                return customerUid + SEPARATOR + timestamp;
            }

            @Override
            protected FMSessionId fromString(String stringId) {
                List<String> parts = Splitter.onPattern(SEPARATOR)
                                             .splitToList(stringId);
                checkArgument(parts.size() == 2);
                FMCustomerId customerId = FMCustomerId
                        .newBuilder()
                        .setUid(parts.get(0))
                        .build();
                Timestamp timestamp;
                try {
                    timestamp = Timestamps.parse(parts.get(1));
                } catch (ParseException e) {
                    throw new IllegalArgumentException(e);
                }
                FMSessionId result = FMSessionId
                        .newBuilder()
                        .setCustomerId(customerId)
                        .setStartTime(timestamp)
                        .build();
                return result;
            }
        };
        StringifierRegistry.getInstance()
                           .register(stringifier, FMSessionId.class);
    }

    public static EventId postCustomerNameChanged(FMCustomerId customerId,
                                                  BoundedContext boundedContext) {
        FMCustomerNameChanged eventMsg = FMCustomerNameChanged
                .newBuilder()
                .setId(customerId)
                .build();
        TestEventFactory factoryWithProducer =
                TestEventFactory.newInstance(customerId, FirebaseMirrorTestEnv.class);
        Event event = factoryWithProducer.createEvent(eventMsg);
        EventBus eventBus = boundedContext.getEventBus();
        eventBus.post(event);
        return event.getId();
    }

    public static FMCustomer createCustomer(FMCustomerId customerId,
                                            BoundedContext boundedContext) {
        return createCustomer(customerId, boundedContext, defaultRequestFactory);
    }

    public static void createSession(FMSessionId sessionId, BoundedContext boundedContext) {
        SessionRepository repository = findRepository(boundedContext, FMSession.class);
        SessionProjection projection = repository.create(sessionId);
        FMCustomerCreated eventMsg = createdEvent(sessionId.getCustomerId());
        Event event = eventFactory.createEvent(eventMsg);
        dispatch(projection, event);
        Stand stand = boundedContext.getStand();
        TenantAwareOperation op = new TenantAwareOperation(defaultTenant()) {
            @Override
            public void run() {
                stand.post(projection, repository.lifecycleOf(sessionId));
            }
        };
        op.execute();
    }

    @CanIgnoreReturnValue
    public static FMCustomer createCustomer(FMCustomerId customerId,
                                            BoundedContext boundedContext,
                                            TenantId tenantId) {
        return createCustomer(customerId, boundedContext, requestFactory(tenantId));
    }

    private static FMCustomer createCustomer(FMCustomerId customerId,
                                             BoundedContext boundedContext,
                                             ActorRequestFactory requestFactory) {
        CustomerRepository repository = findRepository(boundedContext, FMCustomer.class);
        CustomerAggregate aggregate = repository.create(customerId);
        CommandFactory commandFactory = requestFactory.command();

        FMCreateCustomer createCmd = createCommand(customerId);
        FMChangeCustomerName updateCmd = updateCommand(customerId);
        dispatchCommand(aggregate, createCmd, commandFactory);
        dispatchCommand(aggregate, updateCmd, commandFactory);
        Stand stand = boundedContext.getStand();
        TenantId tenantId = requestFactory.getTenantId();
        TenantId realTenantId = tenantId == null ? defaultTenant() : tenantId;
        TenantAwareOperation op = new TenantAwareOperation(realTenantId) {
            @Override
            public void run() {
                stand.post(aggregate, repository.lifecycleOf(customerId));
            }
        };
        op.execute();
        return aggregate.getState();
    }

    private static void dispatchCommand(Aggregate<?, ?, ?> aggregate,
                                        CommandMessage command,
                                        CommandFactory factory) {
        Command cmd = factory.create(command);
        CommandEnvelope envelope = CommandEnvelope.of(cmd);
        AggregateMessageDispatcher.dispatchCommand(aggregate, envelope);
    }

    @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
    private static <I, S extends Message, E extends Entity<I, S>, R extends Repository<I, E>> R
    findRepository(BoundedContext boundedContext, Class<S> stateClass) {
        R repository = (R) boundedContext.findRepository(stateClass)
                                         .orElseThrow(null);
        assertNotNull(repository);
        return repository;
    }

    private static ActorRequestFactory requestFactory(TenantId tenantId) {
        ActorRequestFactory factory = newInstance(TEST_ACTOR, tenantId);
        return factory;
    }

    private static FMCreateCustomer createCommand(FMCustomerId id) {
        FMCreateCustomer createCmd = FMCreateCustomer
                .newBuilder()
                .setId(id)
                .build();
        return createCmd;
    }

    private static FMChangeCustomerName updateCommand(FMCustomerId id) {
        PersonName newName = PersonName
                .newBuilder()
                .setGivenName("John")
                .setFamilyName("Doe")
                .build();
        FMChangeCustomerName updateCmd = FMChangeCustomerName
                .newBuilder()
                .setId(id)
                .setNewName(newName)
                .build();
        return updateCmd;
    }

    private static FMCustomerCreated createdEvent(FMCustomerId id) {
        FMCustomerCreated createCmd = FMCustomerCreated
                .newBuilder()
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
        BoundedContextName contextName = newName(name);
        StorageFactory storageFactory = newInstance(contextName, multitenant);
        Supplier<StorageFactory> storageSupplier = () -> storageFactory;
        BoundedContext result = BoundedContext
                .newBuilder()
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

        @Assign
        FMCustomerCreated handle(FMCreateCustomer command) {
            return createdEvent(command.getId());
        }

        @Assign
        FMCustomerNameChanged handle(FMChangeCustomerName command) {
            return FMCustomerNameChanged.newBuilder()
                                        .setNewName(command.getNewName())
                                        .build();
        }

        @Apply
        private void on(FMCustomerCreated event) {
            getBuilder().setId(event.getId());
        }

        @Apply
        private void on(FMCustomerNameChanged event) {
            getBuilder().setName(event.getNewName());
        }
    }

    private static class CustomerRepository
            extends AggregateRepository<FMCustomerId, CustomerAggregate> {

        @Override
        protected EntityLifecycle lifecycleOf(FMCustomerId id) {
            return super.lifecycleOf(id);
        }
    }

    public static class SessionProjection
            extends Projection<FMSessionId, FMSession, FMSessionVBuilder> {

        protected SessionProjection(FMSessionId id) {
            super(id);
        }

        @Subscribe
        void on(FMCustomerCreated event, EventContext context) {
            getBuilder().setDuration(mockLogic(context));
        }

        private static Duration mockLogic(EventContext context) {
            Timestamp currentTime = Time.getCurrentTime();
            Timestamp eventTime = context.getTimestamp();
            long durationSeconds = eventTime.getSeconds() - currentTime.getSeconds();
            Duration duration = Duration
                    .newBuilder()
                    .setSeconds(durationSeconds)
                    .build();
            return duration;
        }
    }

    private static class SessionRepository
            extends ProjectionRepository<FMSessionId, SessionProjection, FMSession> {

        @Override
        protected EntityLifecycle lifecycleOf(FMSessionId id) {
            return super.lifecycleOf(id);
        }
    }

    private static Logger log() {
        return Logging.get(FirebaseMirrorTestEnv.class);
    }
}
