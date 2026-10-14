package com.acme.finance.domains.collections;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.collections.v1.BrowsePaymentPromisesRequest;
import com.acme.collections.v1.BrowsePaymentPromisesResponse;
import com.acme.collections.v1.CheckWriteOffEligibilityRequest;
import com.acme.collections.v1.CheckWriteOffEligibilityResponse;
import com.acme.collections.v1.CollectionCaseQueryServiceGrpc;
import com.acme.collections.v1.CountPaymentPromisesRequest;
import com.acme.collections.v1.CountPaymentPromisesResponse;
import com.acme.collections.v1.ExportPaymentPromisesRequest;
import com.acme.collections.v1.ExportPaymentPromisesResponse;
import com.acme.collections.v1.FindCollectionCasesRequest;
import com.acme.collections.v1.FindCollectionCasesResponse;
import com.acme.collections.v1.FindPaymentPromisesRequest;
import com.acme.collections.v1.FindPaymentPromisesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.LookupPaymentPromiseRequest;
import com.acme.collections.v1.LookupPaymentPromiseResponse;
import com.acme.collections.v1.NormalizeWriteOffRequest;
import com.acme.collections.v1.NormalizeWriteOffResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PeekPaymentPromiseRequest;
import com.acme.collections.v1.PeekPaymentPromiseResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ResolvePaymentPromisesRequest;
import com.acme.collections.v1.ResolvePaymentPromisesResponse;
import com.acme.collections.v1.SuggestPaymentPromisesRequest;
import com.acme.collections.v1.SuggestPaymentPromisesResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityRequest;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.sponsorships.v1.QuoteSponsorRequest;
import com.acme.sponsorships.v1.QuoteSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about payment promises, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class PaymentPromiseQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(PaymentPromiseQueryApplication.class);

    private final AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    private final SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking;

    public PaymentPromiseQueryApplication(
            AuditEventServiceGrpc.AuditEventServiceStub auditAuditEventServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            CollectionCaseQueryServiceGrpc.CollectionCaseQueryServiceBlockingStub collectionsCollectionCaseQueryServiceBlocking,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SponsorCalculationServiceGrpc.SponsorCalculationServiceBlockingStub sponsorshipsSponsorCalculationServiceBlocking) {
        this.auditAuditEventServiceAsync = auditAuditEventServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.collectionsCollectionCaseQueryServiceBlocking = collectionsCollectionCaseQueryServiceBlocking;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.sponsorshipsSponsorCalculationServiceBlocking = sponsorshipsSponsorCalculationServiceBlocking;
    }

    /**
     * Returns a payment promise.
     */
    public LookupPaymentPromiseResponse lookupPaymentPromise(LookupPaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupPaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getPaymentPromiseId(), "LookupPaymentPromise");

        var outbound0 = BatchCreateAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateAuditEventsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector0 = new StreamCollector<>(BatchCreateAuditEventsResponse.getDefaultInstance());
        var input0 = auditAuditEventServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateAuditEvents(collector0);
        input0.onNext(outbound0);
        input0.onCompleted();
        var reply0 = collector0.await();

        var outbound1 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input1 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        return LookupPaymentPromiseResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .build();
    }

    /**
     * Lists payment promises a page at a time.
     */
    public BrowsePaymentPromisesResponse browsePaymentPromises(BrowsePaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowsePaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "BrowsePaymentPromises");

        var outbound0 = JsonCodec.roundTrip(PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), PreviewWriteOffRequest.class);
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewWriteOff(outbound0);

        var outbound1 = QuoteSponsorRequest.newBuilder()
                .setContext(request.getContext())
                .setSponsor(Sponsor.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getWriteOff().getAudit().getEtag())
                                .setUpdatedBy(reply0.getWriteOff().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(sponsorshipsSponsorCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteSponsor(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowsePaymentPromises", request.getPage().getPageToken());
        return BrowsePaymentPromisesResponse.newBuilder()
                .addPaymentPromises(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply1.getSponsor().getPrimaryAddress().getRegion())
                                .build())
                        .putAllMetadata(reply0.getWriteOff().getMetadataMap())
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPhone(reply1.getSponsor().getContact().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists payment promises a page at a time.
     */
    public FindPaymentPromisesResponse findPaymentPromises(FindPaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindPaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "FindPaymentPromises");

        var outbound0 = BeanCopies.copy(CheckWriteOffEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckWriteOffEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), CheckWriteOffEligibilityRequest.class);
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkWriteOffEligibility(outbound0);

        var outbound1 = CheckMetricSeriesEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.metrics.v1.MetricSeriesValidationService");
        var serviceName1 = "acme.metrics.v1" + "." + "MetricSeriesValidationService";
        var methodName1 = "Che" + "ckMetricSeriesEligibility";
        var descriptor1 = MethodDescriptor.<CheckMetricSeriesEligibilityRequest, CheckMetricSeriesEligibilityResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(CheckMetricSeriesEligibilityRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(CheckMetricSeriesEligibilityResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindPaymentPromises", request.getPage().getPageToken());
        return FindPaymentPromisesResponse.newBuilder()
                .addPaymentPromises(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getMetricSeries().getAudit().getCreatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(reply0.getWriteOff().getBillingAddress().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getWriteOff().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts payment promises by status.
     */
    public CountPaymentPromisesResponse countPaymentPromises(CountPaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountPaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "CountPaymentPromises");

        var outbound0 = ValidateWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateWriteOff(value));
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountPaymentPromises", request.getPage().getPageToken());
        return CountPaymentPromisesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns several payment promises at once.
     */
    public ResolvePaymentPromisesResponse resolvePaymentPromises(ResolvePaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolvePaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "ResolvePaymentPromises");

        var outbound0 = LintWriteOffsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintWriteOffsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintWriteOffs(value)).orElseThrow();
        return ResolvePaymentPromisesResponse.newBuilder()
                .addPaymentPromises(PaymentPromise.newBuilder()
                        .putAllMetadata(reply0.getWriteOff().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Streams every matching payment promise.
     */
    public ExportPaymentPromisesResponse exportPaymentPromises(ExportPaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportPaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "ExportPaymentPromises");

        var outbound0 = NormalizeWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeWriteOff(outbound0), rpcExecutor);

        var outbound1 = FindCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCollectionCasesRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> collectionsCollectionCaseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findCollectionCases(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getFilter().getOwnerId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportPaymentPromises", request.getFilter().getOwnerId());
        return ExportPaymentPromisesResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Lists payment promises a page at a time.
     */
    public SuggestPaymentPromisesResponse suggestPaymentPromises(SuggestPaymentPromisesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestPaymentPromises")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getContext().getTraceId(), "SuggestPaymentPromises");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestPaymentPromises", request.getPage().getPageToken());
        return SuggestPaymentPromisesResponse.getDefaultInstance();
    }

    /**
     * Returns a payment promise.
     */
    public PeekPaymentPromiseResponse peekPaymentPromise(PeekPaymentPromiseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekPaymentPromise")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PaymentPromise", request.getPaymentPromiseId(), "PeekPaymentPromise");
        return PeekPaymentPromiseResponse.newBuilder()
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setPaymentPromiseId(request.getPaymentPromiseId())
                        .build())
                .build();
    }
}
