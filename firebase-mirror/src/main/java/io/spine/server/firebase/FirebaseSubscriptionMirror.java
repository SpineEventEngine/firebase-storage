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
import com.google.cloud.firestore.Firestore;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import io.grpc.stub.StreamObserver;
import io.spine.client.ActorRequestFactory;
import io.spine.client.Subscription;
import io.spine.client.SubscriptionUpdate;
import io.spine.client.Target;
import io.spine.client.Topic;
import io.spine.client.TopicFactory;
import io.spine.core.ActorContext;
import io.spine.core.TenantId;
import io.spine.core.UserId;
import io.spine.server.BoundedContext;
import io.spine.server.SubscriptionService;
import io.spine.server.event.AbstractEventSubscriber;
import io.spine.server.firebase.NewTenantEventSubscriber.TenantCallback;
import io.spine.server.integration.IntegrationBus;
import io.spine.server.tenant.TenantAwareOperation;
import io.spine.server.tenant.TenantIndex;
import io.spine.type.TypeUrl;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.collect.Sets.newHashSet;

/**
 * A client of {@link SubscriptionService} that mirrors the received messages to Firebase.
 *
 * <h1>Usage</h1>
 *
 * <p>{@code FirebaseSubscriptionMirror} reflects the received messages into the specified
 * <a target="_blank" href="https://firebase.google.com/docs/firestore/">Cloud Firestore^</a>
 * database.
 *
 * <p>Use {@code FirebaseSubscriptionMirror} as a subscription update delivery system. When
 * dealing with a number of subscribers (e.g. browser clients), the mirror should be considered
 * a more reliable solution comparing to delivering the updates over gRPC with
 * {@code SubscriptionService}.
 *
 * <p>To start using the mirror, follow these steps:
 * <ol>
 *     <li>Go to the
 *         <a target="_blank" href="https://console.firebase.google.com">Firebase Console^</a>,
 *         select the desired project and enable Cloud Firestore.
 *     <li>Create an instance of {@link Firestore} and pass it to a
 *         {@code FirebaseSubscriptionMirror}.
 *     <li>{@linkplain #reflect(TypeUrl) Reflect} the selected event/entity types to Firestore.
 * </ol>
 *
 * <a name="protocol"></a>
 *
 * <h1>Protocol</h1>
 *
 * <h2>Location</h2>
 *
 * <p>The mirror writes the received updates to the given Firestore by the following rules:
 * <ul>
 *     <li>A {@linkplain CollectionReference collection} is created per event/entity type. The name
 *         of the collection is equal to the Protobuf type URL of the event message or entity
 *         state. The underscore symbol ({@code "_"}) is used instead of slash ({@code "/"}).
 *     <li>The {@linkplain DocumentReference documents} in that collection represent the received
 *         events/entity state updates.
 *     <li>For each observed {@code Entity} instance there is a single document containing the
 *         entity ID and its latest state.
 *     <li>For each observed {@code Event}, there is a single document containing event ID, event
 *         message as well as the timestamp and the producer ID.
 *     <li>When the entity is updated or event of the observed type is received, the appropriate
 *         collection is updated as well.
 * </ul>
 *
 * <p><b>Example:</b>
 * <p>Assume there is the
 * {@code CustomerAggregate extends Aggregate<CustomerId, Customer, CustomerVBuilder>} entity.
 *
 * <p>After a call to {@code mirror.reflect(TypeUrl.of(Customer.class))}, the entity state updates
 * will be written under {@code /type.example.org_example.customer.Customer/document-key}.
 * The {@code document-key} here is a unique key assigned to this instance of {@code Customer} by
 * the mirror. Note that the {@code document-key} is not a valid {@code CustomerAggregate} ID.
 * Do NOT depend any business logic on its value.
 *
 * <h2>Document structure</h2>
 *
 * <p>The documents containing observed entity states have the following structure:
 * <ul>
 *     <li>{@code id}: string;
 *     <li>{@code bytes}: BLOB.
 * </ul>
 *
 * <p>The documents with information about occurred events have the following structure:
 * <ul>
 *     <li>{@code id}: string;
 *     <li>{@code producer_id}: string;
 *     <li>{@code timestamp}: string;
 *     <li>{@code bytes}: BLOB.
 * </ul>
 *
 * <p>For details, see {@link EntityUpdatePublisher} and {@link EventPublisher}.
 *
 * <p>If the entity ID is compound (i.e. has one that one field), a custom
 * {@link io.spine.string.Stringifier Stringifier} for the ID type may be useful for simple querying
 * purposes.
 *
 * <a name="multitenancy"></a>
 *
 * <h1>Multitenancy</h1>
 *
 * <p>When working with multitenant {@code BoundedContext}s, reflecting is started for all
 * the tenants in the specified bounded contexts.
 *
 * <p>The most convenient way to separate the records of different tenants is to use a custom
 * {@link Builder#setReflectionRule(Function) reflection rule}:
 * <pre>
 *     {@code
 *     CollectionReference root = // Dedicate a collection to all the subscriptions.
 *     Function<Topic, Document> reflectionRule = topic -> {
 *         // Each tenant has own document.
 *         final String userId = topic.getContext().getTenant().getValue();
 *         return root.document(userId);
 *     };
 *     FirebaseSubscriptionMirror mirror = FirebaseSubscriptionMirror
 *             .newBuilder()
 *             .setReflectionRule(reflectionRule)
 *             .addBoundedContext(myBoundedContext)
 *             .build();
 *     mirror.reflect(TypeUrl.of(MyEntity.class));
 *     }
 * </pre>
 *
 * <p>In this example, each tenant gets a dedicated document. Note the Firestore document
 * <a target="_blank" href="https://firebase.google.com/docs/firestore/quotas">name limitations^</a>
 * when generating custom documents.
 *
 * <p>Note that the {@code reflectionRule} function accepts a {@link Topic} formed by
 * the {@code FirebaseSubscriptionMirror}. Some fields (e.g. Topic.context.actor) may be absent or
 * carry no meaningful for locating a destination document information.
 *
 * <p>It may be beneficial to have a separate Firebase project for each tenant. This can be done
 * with the {@code reflectionRule} function as well.
 *
 * <p>If the destination document is predefined, it can be specified directly:
 * <pre>
 *     {@code
 *     DocumentReference document = // Dedicate a document to all the subscriptions.
 *     FirebaseSubscriptionMirror mirror = FirebaseSubscriptionMirror
 *             .newBuilder()
 *             .setFirestoreDocument(document)
 *             .addBoundedContext(myBoundedContext)
 *             .build();
 *     mirror.reflect(TypeUrl.of(MyEntity.class));
 *     }
 * </pre>
 *
 * <p>When using a custom firestore document, all the <a href="#protocol">generated collections</a>
 * will be placed under the given document (i.e. be <a target="_blank"
 * href="https://firebase.google.com/docs/firestore/data-model#subcollections">sub-collections^</a>
 * of the given document).
 *
 * @see SubscriptionService
 */
public final class FirebaseSubscriptionMirror {

    private static final String ACTOR_ID = FirebaseSubscriptionMirror.class.getSimpleName();
    private static final UserId ACTOR = UserId
            .newBuilder()
            .setValue(ACTOR_ID)
            .build();
    private static final ActorRequestFactory requestFactory = ActorRequestFactory
            .newBuilder()
            .setActor(ACTOR)
            .build();
    private static final TopicFactory topics = requestFactory.topic();

    private final SubscriptionService subscriptionService;

    /**
     * An instance of {@link Firestore} to store the data in.
     *
     * <p>If specified, the data is stored in the database root.
     *
     * <p>Only one of {@code firestore}, {@link #document}, of {@link #reflectionRule} should be
     * set for an instance of {@code FirebaseSubscriptionMirror}.
     */
    private final @Nullable Firestore firestore;

    /**
     * An instance of {@link DocumentReference} to store the data in.
     *
     * <p>If specified, the data is stored in the specified document.
     *
     * <p>Only one of {@link #firestore}, {@code document}, of {@link #reflectionRule} should be
     * set for an instance of {@code FirebaseSubscriptionMirror}.
     */
    private final @Nullable DocumentReference document;

    /**
     * A function mapping a {@link Topic} to a {@link DocumentReference} in which the records
     * matching that topic should be stored.
     *
     * <p>If specified, the data is distributed amongst the produced documents.
     *
     * <p>Only one of {@link #firestore}, {@link #document}, of {@code reflectionRule} should be
     * set for an instance of {@code FirebaseSubscriptionMirror}.
     */
    private final @Nullable Function<Topic, DocumentReference> reflectionRule;

    private final Set<TenantId> knownTenants;
    private final Set<Topic> subscriptionTopics;

    private FirebaseSubscriptionMirror(Builder builder) {
        this.subscriptionTopics = newConcurrentHashSet();
        this.firestore = builder.firestore;
        this.subscriptionService = builder.subscriptionService;
        this.document = builder.document;
        this.reflectionRule = builder.reflectionRule;
        this.knownTenants = newConcurrentHashSet(builder.knownTenants);
    }

    /**
     * Creates and registers a {@linkplain NewTenantEventSubscriber tenant event subscriber} in
     * the passed {@code contexts}.
     *
     * <p>This method is called in {@link Builder#build()} once after the constructor and should
     * not be called ever more.
     *
     * @param contexts
     *         the bounded contexts to subscribe to the events from
     */
    private void initEventSubscriber(Iterable<BoundedContext> contexts) {
        AbstractEventSubscriber tenantEventSubscriber = createTenantEventSubscriber();
        registerEventSubscriber(contexts, tenantEventSubscriber);
    }

    private AbstractEventSubscriber createTenantEventSubscriber() {
        TenantCallback tenantCallback = new ReflectExistingTopics();
        AbstractEventSubscriber result = new NewTenantEventSubscriber(tenantCallback);
        return result;
    }

    private static void registerEventSubscriber(Iterable<BoundedContext> contexts,
                                                AbstractEventSubscriber eventSubscriber) {
        for (BoundedContext context : contexts) {
            IntegrationBus integrationBus = context.integrationBus();
            integrationBus.register(eventSubscriber);
        }
    }

    /**
     * Starts reflecting updates on entities/events of the specified {@linkplain TypeUrl type} into
     * the Cloud Firestore.
     *
     * <p>After this method invocation, the underlying {@link Firestore} will start (eventually)
     * receiving the entity state updates and the events of the observed type which occur in the
     * system.
     *
     * <p>See <a href="#protocol">class level doc</a> for the description of the storage protocol.
     *
     * @param type
     *         the type of the entities to reflect
     */
    public void reflect(TypeUrl type) {
        checkNotNull(type);
        Class<? extends Message> entityClass = type.getMessageClass();
        Topic topic = topics.allOf(entityClass);
        subscriptionTopics.add(topic);
        for (TenantId knownTenant : knownTenants) {
            reflect(topic, knownTenant);
        }
    }

    /**
     * Starts reflecting the given {@link Topic} into the Cloud Firestore for the given tenant.
     *
     * <p>Only the entity state updates and events of the given tenant will be reflected into
     * Firebase.
     *
     * @param topic
     *         the topic to reflect
     * @param tenantId
     *         the tenant whose updates will be reflected
     */
    private void reflect(Topic topic, TenantId tenantId) {
        Topic subscriptionTopic = forTenant(topic, tenantId);
        new TenantAwareOperation(tenantId) {
            @Override
            public void run() {
                doReflect(subscriptionTopic);
            }
        }.execute();
    }

    /**
     * Starts reflecting {@code topic} for the current tenant.
     *
     * @param topic
     *         the topic to reflect
     */
    private void doReflect(Topic topic) {
        CollectionReference collectionReference = collection(topic);
        StreamObserver<SubscriptionUpdate> updateObserver =
                UpdateObserver.create(topic, collectionReference);
        StreamObserver<Subscription> subscriptionObserver =
                new SubscriptionObserver(subscriptionService, updateObserver);
        subscriptionService.subscribe(topic, subscriptionObserver);
    }

    private CollectionReference collection(Topic topic) {
        String collectionKey = toKey(topic.getTarget());
        CollectionReference result;
        if (reflectionRule != null) {
            DocumentReference document = reflectionRule.apply(topic);
            result = document.collection(collectionKey);
        } else if (document != null) {
            result = document.collection(collectionKey);
        } else {
            checkNotNull(firestore);
            result = firestore.collection(collectionKey);
        }
        return result;
    }

    private static String toKey(Target target) {
        TypeUrl typeUrl = TypeUrl.parse(target.getType());
        String type = typeUrl.prefix() + '_' + typeUrl.toTypeName().value();
        return type;
    }

    private static Topic forTenant(Topic topic, TenantId tenant) {
        ActorContext context = topic.getContext()
                                    .toBuilder()
                                    .setTenantId(tenant)
                                    .build();
        Topic result = topic.toBuilder()
                            .setContext(context)
                            .build();
        return result;
    }

    /**
     * Creates a new instance of {@code Builder} for {@code FirebaseSubscriptionMirror}
     * instances.
     *
     * @return new instance of {@code Builder}
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * A builder for the {@code FirebaseSubscriptionMirror} instances.
     */
    public static final class Builder {

        private final Set<BoundedContext> boundedContexts = newHashSet();

        private SubscriptionService subscriptionService;

        private @Nullable Firestore firestore;
        private @Nullable DocumentReference document;

        private Function<Topic, DocumentReference> reflectionRule;

        private Collection<TenantId> knownTenants;

        /** Prevents direct instantiation. */
        private Builder() {
        }

        /**
         * Sets a {@link SubscriptionService} to use in the built mirror.
         *
         * <p>Note that the service is not a gRPC stub but the implementation itself. This allows
         * not to deploy a {@code SubscriptionService} at all, but just use it in memory.
         *
         * <p>If no {@code SubscriptionService} is specified, an instance is built with the passed
         * {@linkplain #addBoundedContext(BoundedContext) bounded contexts}.
         *
         * @param subscriptionService
         *         the {@link SubscriptionService} to get the subscription updates from
         * @return self for method chaining
         */
        public Builder setSubscriptionService(SubscriptionService subscriptionService) {
            this.subscriptionService = checkNotNull(subscriptionService);
            return this;
        }

        /**
         * Sets the Firestore to the built mirror.
         *
         * <p>Either this method or {@link #setReflectionRule(Function)} or
         * {@link #setFirestoreDocument(DocumentReference)} must be called, but only one of them.
         *
         * <p>In case this method is called, the generated collections will be located in the
         * Firestore root.
         *
         * @param database
         *         the {@link Firestore} to use
         * @return self for method chaining
         * @see #setFirestoreDocument(DocumentReference)
         */
        public Builder setFirestore(Firestore database) {
            this.firestore = checkNotNull(database);
            return this;
        }

        /**
         * Sets the custom Firestore document to the built mirror.
         *
         * <p>Either this method or {@link #setFirestore(Firestore)} or
         * {@link #setReflectionRule(Function)} must be called, but only one of them.
         *
         * <p>In case this method is called, the generated collections will be located under the
         * specified document.
         *
         * @param document
         *         the {@link DocumentReference} to write the data into
         * @return self for method chaining
         * @see #setFirestore(Firestore)
         */
        public Builder setFirestoreDocument(DocumentReference document) {
            this.document = checkNotNull(document);
            return this;
        }

        /**
         * Sets a custom function mapping a {@code Topic} to the {@code DocumentReference} which
         * should be the parent to the collections of state updates.
         *
         * <p>Either this method or {@link #setFirestore(Firestore)} or
         * {@link #setFirestoreDocument(DocumentReference)} must be called, but only one of them.
         *
         * <p>In case this method is called, the generated collections will be distributed amongst
         * the documents produced by the function .
         *
         * @param reflectionRule
         *         the topic to document function
         * @return self for method chaining
         */
        public Builder setReflectionRule(Function<Topic, DocumentReference> reflectionRule) {
            checkNotNull(reflectionRule);
            this.reflectionRule = reflectionRule;
            return this;
        }

        /**
         * Adds a bounded context to this mirror.
         *
         * <p>At least one {@code BoundedContext} should be specified to build a mirror.
         *
         * @param boundedContext
         *         the {@link BoundedContext} to reflect entities and events from
         * @return self for method chaining
         */
        public Builder addBoundedContext(BoundedContext boundedContext) {
            checkNotNull(boundedContext);
            this.boundedContexts.add(boundedContext);
            return this;
        }

        /**
         * Creates a new instance of {@code FirebaseSubscriptionMirror}.
         *
         * <p>Note that this method also registers a {@linkplain NewTenantEventSubscriber tenant
         * event subscriber} in the present bounded contexts.
         *
         * @return new instance of {@code FirebaseSubscriptionMirror} with the given parameters
         */
        @SuppressWarnings("CheckReturnValue") // OK for builder.
        public FirebaseSubscriptionMirror build() {
            checkState(!boundedContexts.isEmpty(),
                       "At least one BoundedContext should be specified.");
            checkLocationSettings();
            if (subscriptionService == null) {
                SubscriptionService.Builder builder = SubscriptionService.newBuilder();
                for (BoundedContext context : boundedContexts) {
                    builder.add(context);
                }
                subscriptionService = builder.build();
            }
            Collection<BoundedContext> contexts = ImmutableSet.copyOf(boundedContexts);
            this.knownTenants = getAllTenants(contexts);
            FirebaseSubscriptionMirror mirror = new FirebaseSubscriptionMirror(this);
            mirror.initEventSubscriber(contexts);
            return mirror;
        }

        private static Collection<TenantId> getAllTenants(Iterable<BoundedContext> contexts) {
            Collection<TenantId> tenants = newLinkedList();
            for (BoundedContext context : contexts) {
                TenantIndex tenantIndex = context.tenantIndex();
                tenants.addAll(tenantIndex.getAll());
            }
            ImmutableSet<TenantId> result = ImmutableSet.copyOf(tenants);
            return result;
        }

        private void checkLocationSettings() {
            checkState(
                    (firestore != null) ^ (document != null) ^ (reflectionRule != null),
                    "Only one of Firestore or Firestore Document or reflection rule must be set."
            );
        }
    }

    /**
     * A {@link TenantCallback} which starts {@linkplain #reflect(Topic, TenantId) reflecting}
     * all the previously requested {@code Topic}s for the new tenant.
     */
    private class ReflectExistingTopics implements TenantCallback {

        @Override
        public void onTenant(TenantId tenantId) {
            for (Topic topic : subscriptionTopics) {
                reflect(topic, tenantId);
            }
            knownTenants.add(tenantId);
        }
    }
}
