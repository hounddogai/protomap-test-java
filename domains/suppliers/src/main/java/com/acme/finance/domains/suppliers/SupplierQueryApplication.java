package com.acme.finance.domains.suppliers;

import com.acme.analytics.v1.AnalyticsEventQueryServiceGrpc;
import com.acme.analytics.v1.BrowseAnalyticsEventsRequest;
import com.acme.analytics.v1.BrowseAnalyticsEventsResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.BrowseUsersRequest;
import com.acme.identity.v1.BrowseUsersResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PeekSupplierRequest;
import com.acme.suppliers.v1.PeekSupplierResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.ResolveSuppliersRequest;
import com.acme.suppliers.v1.ResolveSuppliersResponse;
import com.acme.suppliers.v1.ScoreSupplierRiskRequest;
import com.acme.suppliers.v1.ScoreSupplierRiskResponse;
import com.acme.suppliers.v1.SuggestSuppliersRequest;
import com.acme.suppliers.v1.SuggestSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskComment;
import com.acme.suppliers.v1.SupplierRiskSla;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.ValidateSupplierRiskRequest;
import com.acme.suppliers.v1.ValidateSupplierRiskResponse;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about suppliers, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierQueryApplication.class);

    private final AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final SuppliersRepository repository;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public SupplierQueryApplication(
            AnalyticsEventQueryServiceGrpc.AnalyticsEventQueryServiceBlockingStub analyticsAnalyticsEventQueryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BankAccountServiceGrpc.BankAccountServiceStub bankingBankAccountServiceAsync,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            SuppliersRepository repository,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.analyticsAnalyticsEventQueryServiceBlocking = analyticsAnalyticsEventQueryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.bankingBankAccountServiceAsync = bankingBankAccountServiceAsync;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    /**
     * Returns a supplier.
     */
    public LookupSupplierResponse lookupSupplier(LookupSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "LookupSupplier");

        var outbound0 = ValidateSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRisk(value));

        var outbound1 = BatchCreateBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBankAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateBankAccountsResponse.getDefaultInstance());
        var input1 = bankingBankAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBankAccounts(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
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
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupSupplierResponse.newBuilder()
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSupplierRisk().getAudit().getCreatedBy())
                                .build())
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }

    /**
     * Lists suppliers a page at a time.
     */
    public BrowseSuppliersResponse browseSuppliers(BrowseSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "BrowseSuppliers");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseSuppliers", request.getPage().getPageToken());
        return BrowseSuppliersResponse.getDefaultInstance();
    }

    /**
     * Lists suppliers a page at a time.
     */
    public FindSuppliersResponse findSuppliers(FindSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "FindSuppliers");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindSuppliers", request.getPage().getPageToken());
        return FindSuppliersResponse.getDefaultInstance();
    }

    /**
     * Counts suppliers by status.
     */
    public CountSuppliersResponse countSuppliers(CountSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "CountSuppliers");

        var builder0 = ScoreSupplierRiskRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(ScoreSupplierRiskRequest.Details.newBuilder()
                .setDestination(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        builder0.setSupplierRisk(SupplierRisk.newBuilder()
                .setAssignee(UserRef.newBuilder()
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .addComments(SupplierRiskComment.newBuilder()
                        .setAuthorEmail(request.getContext().getActor().getEmail())
                        .build())
                .setSla(SupplierRiskSla.newBuilder()
                        .setDueAt(request.getFilter().getCreatedAfter())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSupplierRisk(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountSuppliers", request.getPage().getPageToken());
        return CountSuppliersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns several suppliers at once.
     */
    public ResolveSuppliersResponse resolveSuppliers(ResolveSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "ResolveSuppliers");

        var outbound0 = JsonCodec.roundTrip(BrowseUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .build(), BrowseUsersRequest.class);
        var reply0 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseUsers(outbound0);
        return ResolveSuppliersResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams every matching supplier.
     */
    public ExportSuppliersResponse exportSuppliers(ExportSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "ExportSuppliers");

        var outbound0 = PreviewSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRiskRequest.Details.newBuilder()
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setSla(SupplierRiskSla.newBuilder()
                                .setDueAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewSupplierRiskRequest, PreviewSupplierRiskResponse> port0 = RpcPort.adapt(suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewSupplierRisk);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ValidateSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(reply0.getSupplierRisk().getAssignee().getUserId())
                                .build())
                        .setCustomer(SupplierContractRef.newBuilder()
                                .setNumber(switch (reply0.getSupplierRisk().getOriginCase()) {
                                    case CUSTOMER -> reply0.getSupplierRisk().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getSupplierRisk().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSupplierRisk(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportSuppliers", request.getFilter().getQuery());
        return ExportSuppliersResponse.newBuilder()
                .setSupplier(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSupplierRisk().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getSupplierRisk().getAudit().getUpdatedAt())
                                .build())
                        .setPrimaryAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setUserId(reply1.getSupplierRisk().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
    }

    /**
     * Summarizes a supplier.
     */
    public SummarizeSuppliersResponse summarizeSuppliers(SummarizeSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "SummarizeSuppliers");

        var outbound0 = (CheckSupplierRiskEligibilityRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRiskEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeSuppliers", request.getSupplierId());
        return SummarizeSuppliersResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Lists suppliers a page at a time.
     */
    public SuggestSuppliersResponse suggestSuppliers(SuggestSuppliersRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestSuppliers")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getContext().getTraceId(), "SuggestSuppliers");

        var outbound0 = CheckSupplierRiskEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .build())
                .build();
        var reply0 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkSupplierRiskEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestSuppliers", request.getPage().getPageToken());
        return SuggestSuppliersResponse.newBuilder()
                .addSuppliers(Supplier.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getSupplierRisk().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
    }

    /**
     * Returns a supplier.
     */
    public PeekSupplierResponse peekSupplier(PeekSupplierRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekSupplier")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Supplier", request.getSupplierId(), "PeekSupplier");

        var outbound0 = JsonCodec.roundTrip(BrowseAnalyticsEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAnalyticsEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getSupplierId())
                        .build())
                .build(), BrowseAnalyticsEventsRequest.class);
        var reply0 = analyticsAnalyticsEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAnalyticsEvents(outbound0);
        return PeekSupplierResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplier(Supplier.newBuilder()
                        .setSupplierId(request.getSupplierId())
                        .build())
                .build();
    }
}
