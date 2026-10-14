package com.acme.finance.domains.collections;

import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.banking.v1.UpdateBankAccountRequest;
import com.acme.banking.v1.UpdateBankAccountResponse;
import com.acme.collections.v1.BrowseCollectionCasesRequest;
import com.acme.collections.v1.BrowseCollectionCasesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseRef;
import com.acme.collections.v1.CountCollectionCasesRequest;
import com.acme.collections.v1.CountCollectionCasesResponse;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.ExportCollectionCasesRequest;
import com.acme.collections.v1.ExportCollectionCasesResponse;
import com.acme.collections.v1.FindCollectionCasesRequest;
import com.acme.collections.v1.FindCollectionCasesResponse;
import com.acme.collections.v1.LintWriteOffsRequest;
import com.acme.collections.v1.LintWriteOffsResponse;
import com.acme.collections.v1.LookupCollectionCaseRequest;
import com.acme.collections.v1.LookupCollectionCaseResponse;
import com.acme.collections.v1.PeekCollectionCaseRequest;
import com.acme.collections.v1.PeekCollectionCaseResponse;
import com.acme.collections.v1.PreviewWriteOffRequest;
import com.acme.collections.v1.PreviewWriteOffResponse;
import com.acme.collections.v1.ResolveCollectionCasesRequest;
import com.acme.collections.v1.ResolveCollectionCasesResponse;
import com.acme.collections.v1.SuggestCollectionCasesRequest;
import com.acme.collections.v1.SuggestCollectionCasesResponse;
import com.acme.collections.v1.SummarizeCollectionCasesRequest;
import com.acme.collections.v1.SummarizeCollectionCasesResponse;
import com.acme.collections.v1.ValidateWriteOffRequest;
import com.acme.collections.v1.ValidateWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffLine;
import com.acme.collections.v1.WriteOffLineTax;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.WatchPoliciesRequest;
import com.acme.insurance.v1.WatchPoliciesResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.ValidateNotificationRequest;
import com.acme.notifications.v1.ValidateNotificationResponse;
import com.acme.venues.v1.SearchVenuesRequest;
import com.acme.venues.v1.SearchVenuesResponse;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about collection cases, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CollectionCaseQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(CollectionCaseQueryApplication.class);

    private final BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking;

    private final MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking;

    private final CollectionsRepository repository;

    private final Executor rpcExecutor;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public CollectionCaseQueryApplication(
            BankAccountServiceGrpc.BankAccountServiceBlockingStub bankingBankAccountServiceBlocking,
            MandateValidationServiceGrpc.MandateValidationServiceBlockingStub bankingMandateValidationServiceBlocking,
            ChannelRegistry channels,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyServiceGrpc.PolicyServiceBlockingStub insurancePolicyServiceBlocking,
            CollectionsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.bankingBankAccountServiceBlocking = bankingBankAccountServiceBlocking;
        this.bankingMandateValidationServiceBlocking = bankingMandateValidationServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyServiceBlocking = insurancePolicyServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Returns a collection case.
     */
    public LookupCollectionCaseResponse lookupCollectionCase(LookupCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "LookupCollectionCase");
        return LookupCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .build();
    }

    /**
     * Lists collection cases a page at a time.
     */
    public BrowseCollectionCasesResponse browseCollectionCases(BrowseCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "BrowseCollectionCases");

        var outbound0 = WatchPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .build();
        var stream0 = insurancePolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchPolicies(outbound0);
        var reply0 = WatchPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var adapter1 = new Function<PreviewWriteOffRequest, PreviewWriteOffResponse>() {
            @Override
            public PreviewWriteOffResponse apply(PreviewWriteOffRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewWriteOff(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseCollectionCases", request.getPage().getPageToken());
        return BrowseCollectionCasesResponse.newBuilder()
                .addCollectionCases(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getWriteOff().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists collection cases a page at a time.
     */
    public FindCollectionCasesResponse findCollectionCases(FindCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "FindCollectionCases");

        var outbound0 = LintWriteOffsRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<LintWriteOffsRequest, LintWriteOffsResponse>() {
            @Override
            public LintWriteOffsResponse apply(LintWriteOffsRequest input) {
                return collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintWriteOffs(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindCollectionCases", request.getPage().getPageToken());
        return FindCollectionCasesResponse.newBuilder()
                .addCollectionCases(CollectionCase.newBuilder()
                        .setCollectionCaseId(reply0.getWriteOff().getCounterparty().getCollectionCaseId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Counts collection cases by status.
     */
    public CountCollectionCasesResponse countCollectionCases(CountCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "CountCollectionCases");

        var outbound0 = SearchVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setPage(PageRequest.newBuilder()
                        .setPageSize(request.getPage().getPageSize())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchVenues(outbound0), rpcExecutor);

        var outbound1 = UpdateBankAccountRequest.newBuilder()
                .setBankAccount(BankAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> bankingBankAccountServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateBankAccount(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountCollectionCases", request.getPage().getPageToken());
        return CountCollectionCasesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns several collection cases at once.
     */
    public ResolveCollectionCasesResponse resolveCollectionCases(ResolveCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "ResolveCollectionCases");

        var outbound0 = RpcSupport.unpack(Any.pack(ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), ValidateFeatureFlagRequest.class);
        var reply0 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFeatureFlag(outbound0);
        return ResolveCollectionCasesResponse.newBuilder()
                .addCollectionCases(CollectionCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getFeatureFlag().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFeatureFlag().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching collection case.
     */
    public ExportCollectionCasesResponse exportCollectionCases(ExportCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "ExportCollectionCases");

        var outbound0 = BeanCopies.copy(PreviewWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewWriteOffRequest.Details.newBuilder()
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
                .build(), PreviewWriteOffRequest.class);
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewWriteOff(outbound0);

        var outbound1 = ValidateNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setNotification(Notification.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getWriteOff().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getWriteOff().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.notifications.v1.NotificationValidationService");
        var serviceName1 = "acme.notifications.v1" + "." + "NotificationValidationService";
        var methodName1 = "Val" + "idateNotification";
        var descriptor1 = MethodDescriptor.<ValidateNotificationRequest, ValidateNotificationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidateNotificationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidateNotificationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = ScoreMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getNotification().getAudit().getEtag())
                                .setUpdatedAt(reply1.getNotification().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getWriteOff().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply2 = bankingMandateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreMandate(outbound2);

        var outbound3 = ValidateWriteOffRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateWriteOffRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply1.getNotification().getRequester().getName())
                                .setPreferredChannel(reply1.getNotification().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setWriteOff(WriteOff.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getWriteOff().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply3 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateWriteOff(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportCollectionCases", request.getFilter().getQuery());
        return ExportCollectionCasesResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getMandate().getAudit().getCreatedAt())
                                .setEtag(reply0.getWriteOff().getAudit().getEtag())
                                .build())
                        .setCollectionCaseId(reply0.getWriteOff().getCounterparty().getCollectionCaseId())
                        .setPriority(reply3.getWriteOff().getCounterparty().getPriority())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(reply1.getNotification().getRequester().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a collection case.
     */
    public SummarizeCollectionCasesResponse summarizeCollectionCases(SummarizeCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "SummarizeCollectionCases");

        var outbound0 = RpcSupport.unpack(Any.pack(LintWriteOffsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setCounterparty(CollectionCaseRef.newBuilder()
                                .setCollectionCaseId(request.getCollectionCaseId())
                                .build())
                        .build())
                .build()), LintWriteOffsRequest.class);
        var reply0 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintWriteOffs(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(CalculatePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculatePolicyRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply0.getWriteOff().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getWriteOff().getAudit().getEtag())
                                .build())
                        .build())
                .build()), CalculatePolicyRequest.class);
        var reply1 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePolicy(outbound1);

        var outbound2 = NormalizeFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPolicy().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.featureflags.v1.FeatureFlagValidationService");
        var serviceName2 = "acme.featureflags.v1" + "." + "FeatureFlagValidationService";
        var methodName2 = "Nor" + "malizeFeatureFlag";
        var descriptor2 = MethodDescriptor.<NormalizeFeatureFlagRequest, NormalizeFeatureFlagResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeFeatureFlagRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeFeatureFlagResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var builder3 = ExplainWriteOffDecisionRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setWriteOff(WriteOff.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(reply1.getPolicy().getAudit().getEtag())
                        .setUpdatedBy(reply1.getPolicy().getAudit().getUpdatedBy())
                        .build())
                .addLines(WriteOffLine.newBuilder()
                        .setTax(WriteOffLineTax.newBuilder()
                                .setAmount(reply2.getFeatureFlag().getDiscount().getAmount())
                                .build())
                        .build())
                .build());
        var outbound3 = builder3.build();
        var reply3 = collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainWriteOffDecision(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeCollectionCases", request.getDetails().getFormat());
        return SummarizeCollectionCasesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists collection cases a page at a time.
     */
    public SuggestCollectionCasesResponse suggestCollectionCases(SuggestCollectionCasesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestCollectionCases")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getContext().getTraceId(), "SuggestCollectionCases");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestCollectionCases", request.getPage().getPageToken());
        return SuggestCollectionCasesResponse.getDefaultInstance();
    }

    /**
     * Returns a collection case.
     */
    public PeekCollectionCaseResponse peekCollectionCase(PeekCollectionCaseRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekCollectionCase")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CollectionCase", request.getCollectionCaseId(), "PeekCollectionCase");
        return PeekCollectionCaseResponse.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setCollectionCaseId(request.getCollectionCaseId())
                        .build())
                .build();
    }
}
