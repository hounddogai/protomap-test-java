package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseChartOfAccountsesRequest;
import com.acme.accounting.v1.BrowseChartOfAccountsesResponse;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.CountChartOfAccountsesRequest;
import com.acme.accounting.v1.CountChartOfAccountsesResponse;
import com.acme.accounting.v1.ExplainReconciliationDecisionRequest;
import com.acme.accounting.v1.ExplainReconciliationDecisionResponse;
import com.acme.accounting.v1.ExportChartOfAccountsesRequest;
import com.acme.accounting.v1.ExportChartOfAccountsesResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.LookupChartOfAccountsRequest;
import com.acme.accounting.v1.LookupChartOfAccountsResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PeekChartOfAccountsRequest;
import com.acme.accounting.v1.PeekChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResolveChartOfAccountsesRequest;
import com.acme.accounting.v1.ResolveChartOfAccountsesResponse;
import com.acme.accounting.v1.SummarizeChartOfAccountsesRequest;
import com.acme.accounting.v1.SummarizeChartOfAccountsesResponse;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.SuggestBillingAccountsRequest;
import com.acme.billing.v1.SuggestBillingAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.telematics.v1.SearchTelemetryReadingsRequest;
import com.acme.telematics.v1.SearchTelemetryReadingsResponse;
import com.acme.telematics.v1.TelemetryReadingServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about chart of accountses, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class ChartOfAccountsQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(ChartOfAccountsQueryApplication.class);

    private final AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking;

    private final AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking;

    private final ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking;

    private final BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking;

    private final AccountingRepository repository;

    private final TelemetryReadingServiceGrpc.TelemetryReadingServiceBlockingStub telematicsTelemetryReadingServiceBlocking;

    public ChartOfAccountsQueryApplication(
            AccrualValidationServiceGrpc.AccrualValidationServiceBlockingStub accountingAccrualValidationServiceBlocking,
            AllocationValidationServiceGrpc.AllocationValidationServiceBlockingStub accountingAllocationValidationServiceBlocking,
            ReconciliationValidationServiceGrpc.ReconciliationValidationServiceBlockingStub accountingReconciliationValidationServiceBlocking,
            BillingAccountQueryServiceGrpc.BillingAccountQueryServiceBlockingStub billingBillingAccountQueryServiceBlocking,
            ChannelRegistry channels,
            PurchaseRequisitionValidationServiceGrpc.PurchaseRequisitionValidationServiceBlockingStub procurementPurchaseRequisitionValidationServiceBlocking,
            AccountingRepository repository,
            TelemetryReadingServiceGrpc.TelemetryReadingServiceBlockingStub telematicsTelemetryReadingServiceBlocking) {
        this.accountingAccrualValidationServiceBlocking = accountingAccrualValidationServiceBlocking;
        this.accountingAllocationValidationServiceBlocking = accountingAllocationValidationServiceBlocking;
        this.accountingReconciliationValidationServiceBlocking = accountingReconciliationValidationServiceBlocking;
        this.billingBillingAccountQueryServiceBlocking = billingBillingAccountQueryServiceBlocking;
        this.channels = channels;
        this.procurementPurchaseRequisitionValidationServiceBlocking = procurementPurchaseRequisitionValidationServiceBlocking;
        this.repository = repository;
        this.telematicsTelemetryReadingServiceBlocking = telematicsTelemetryReadingServiceBlocking;
    }

    /**
     * Returns a chart of accounts.
     */
    public LookupChartOfAccountsResponse lookupChartOfAccounts(LookupChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "LookupChartOfAccounts");

        var outbound0 = (PreviewAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = accountingAllocationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewAllocation(outbound0);
        return LookupChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getAllocation().getAudit().getCreatedAt())
                                .build())
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .setManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getAllocation().getOwner().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    public BrowseChartOfAccountsesResponse browseChartOfAccountses(BrowseChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "BrowseChartOfAccountses");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseChartOfAccountses", request.getPage().getPageToken());
        return BrowseChartOfAccountsesResponse.getDefaultInstance();
    }

    /**
     * Lists chart of accountses a page at a time.
     */
    public FindChartOfAccountsesResponse findChartOfAccountses(FindChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "FindChartOfAccountses");

        var outbound0 = CalculateReconciliationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateReconciliationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.accounting.v1.ReconciliationCalculationService");
        var selected0 = ReconciliationCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.calculateReconciliation(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindChartOfAccountses", request.getPage().getPageToken());
        return FindChartOfAccountsesResponse.newBuilder()
                .addChartOfAccountses(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setEmail(reply0.getReconciliation().getRequester().getEmail())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Counts chart of accountses by status.
     */
    public CountChartOfAccountsesResponse countChartOfAccountses(CountChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "CountChartOfAccountses");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountChartOfAccountses", request.getPage().getPageToken());
        return CountChartOfAccountsesResponse.getDefaultInstance();
    }

    /**
     * Returns several chart of accountses at once.
     */
    public ResolveChartOfAccountsesResponse resolveChartOfAccountses(ResolveChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "ResolveChartOfAccountses");

        var outbound0 = RpcSupport.unpack(Any.pack(SuggestBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBillingAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .setOwnerId(request.getContext().getActor().getUserId())
                        .build())
                .build()), SuggestBillingAccountsRequest.class);
        var reply0 = billingBillingAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBillingAccounts(outbound0);
        return ResolveChartOfAccountsesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams every matching chart of accounts.
     */
    public ExportChartOfAccountsesResponse exportChartOfAccountses(ExportChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getContext().getTraceId(), "ExportChartOfAccountses");

        var outbound0 = JsonCodec.roundTrip(ExplainReconciliationDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainReconciliationDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReconciliation(Reconciliation.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), ExplainReconciliationDecisionRequest.class);
        var reply0 = accountingReconciliationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainReconciliationDecision(outbound0);

        var outbound1 = SearchTelemetryReadingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchTelemetryReadingsRequest.Filter.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply1 = telematicsTelemetryReadingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchTelemetryReadings(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportChartOfAccountses", request.getFilter().getQuery());
        return ExportChartOfAccountsesResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(reply0.getReconciliation().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes a chart of accounts.
     */
    public SummarizeChartOfAccountsesResponse summarizeChartOfAccountses(SummarizeChartOfAccountsesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeChartOfAccountses")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "SummarizeChartOfAccountses");

        var outbound0 = LintAccrualsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(LintAccrualsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = accountingAccrualValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAccruals(outbound0);

        var outbound1 = NormalizeAllocationRequest.newBuilder()
                .setAllocation(Allocation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getAccrual().getAudit().getEtag())
                                .setUpdatedAt(reply0.getAccrual().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getAccrual().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.accounting.v1.AllocationValidationService");
        var serviceName1 = "acme.accounting.v1" + "." + "AllocationValidationService";
        var methodName1 = "Nor" + "malizeAllocation";
        var descriptor1 = MethodDescriptor.<NormalizeAllocationRequest, NormalizeAllocationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(NormalizeAllocationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(NormalizeAllocationResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getChartOfAccountsId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeChartOfAccountses", request.getChartOfAccountsId());
        return SummarizeChartOfAccountsesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a chart of accounts.
     */
    public PeekChartOfAccountsResponse peekChartOfAccounts(PeekChartOfAccountsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekChartOfAccounts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "ChartOfAccounts", request.getChartOfAccountsId(), "PeekChartOfAccounts");

        var builder0 = CheckPurchaseRequisitionEligibilityRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CheckPurchaseRequisitionEligibilityRequest.Details.newBuilder()
                .setContact(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .setLocale(request.getContext().getLocale())
                .build());
        builder0.setPurchaseRequisition(PurchaseRequisition.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setCreatedAt(request.getContext().getDeadline())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = procurementPurchaseRequisitionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPurchaseRequisitionEligibility(outbound0);
        return PeekChartOfAccountsResponse.newBuilder()
                .setChartOfAccounts(ChartOfAccounts.newBuilder()
                        .setChartOfAccountsId(request.getChartOfAccountsId())
                        .addChildren(ChartOfAccounts.newBuilder()
                                .addChildren(ChartOfAccounts.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedBy(reply0.getPurchaseRequisition().getAudit().getUpdatedBy())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
