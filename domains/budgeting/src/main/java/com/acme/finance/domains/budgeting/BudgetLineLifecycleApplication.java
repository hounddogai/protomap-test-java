package com.acme.finance.domains.budgeting;

import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.FunnelRef;
import com.acme.analytics.v1.UpdateAnalyticsEventRequest;
import com.acme.analytics.v1.UpdateAnalyticsEventResponse;
import com.acme.budgeting.v1.ActivateBudgetLineRequest;
import com.acme.budgeting.v1.ActivateBudgetLineResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.ExpireBudgetLineRequest;
import com.acme.budgeting.v1.ExpireBudgetLineResponse;
import com.acme.budgeting.v1.ForecastReportingServiceGrpc;
import com.acme.budgeting.v1.GetBudgetLineLifecycleRequest;
import com.acme.budgeting.v1.GetBudgetLineLifecycleResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ReconcileVariancesRequest;
import com.acme.budgeting.v1.ReconcileVariancesResponse;
import com.acme.budgeting.v1.ReinstateBudgetLineRequest;
import com.acme.budgeting.v1.ReinstateBudgetLineResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.budgeting.v1.ScheduleForecastReportRequest;
import com.acme.budgeting.v1.ScheduleForecastReportResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.SuspendBudgetLineRequest;
import com.acme.budgeting.v1.SuspendBudgetLineResponse;
import com.acme.budgeting.v1.TerminateBudgetLineRequest;
import com.acme.budgeting.v1.TerminateBudgetLineResponse;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.CreateDocumentRequest;
import com.acme.documents.v1.CreateDocumentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentFolderRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ListPremiumConflictsRequest;
import com.acme.insurance.v1.ListPremiumConflictsResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.SyncPremiumsRequest;
import com.acme.insurance.v1.SyncPremiumsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentApprovalServiceGrpc;
import com.acme.lending.v1.SubmitRepaymentRequest;
import com.acme.lending.v1.SubmitRepaymentResponse;
import com.acme.payments.v1.PaymentQueryServiceGrpc;
import com.acme.payments.v1.SuggestPaymentsRequest;
import com.acme.payments.v1.SuggestPaymentsResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.LintSupplierRisksRequest;
import com.acme.suppliers.v1.LintSupplierRisksResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Moves a budget line between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class BudgetLineLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineLifecycleApplication.class);

    private final AnalyticsEventServiceGrpc.AnalyticsEventServiceFutureStub analyticsAnalyticsEventServiceFuture;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking;

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking;

    private final BudgetLineLifecycleRequestMapper mapper;

    private final PaymentQueryServiceGrpc.PaymentQueryServiceBlockingStub paymentsPaymentQueryServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    public BudgetLineLifecycleApplication(
            AnalyticsEventServiceGrpc.AnalyticsEventServiceFutureStub analyticsAnalyticsEventServiceFuture,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            ForecastReportingServiceGrpc.ForecastReportingServiceBlockingStub budgetingForecastReportingServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceBlockingStub budgetingVarianceSyncServiceBlocking,
            DocumentServiceGrpc.DocumentServiceBlockingStub documentsDocumentServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            RepaymentApprovalServiceGrpc.RepaymentApprovalServiceBlockingStub lendingRepaymentApprovalServiceBlocking,
            BudgetLineLifecycleRequestMapper mapper,
            PaymentQueryServiceGrpc.PaymentQueryServiceBlockingStub paymentsPaymentQueryServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking) {
        this.analyticsAnalyticsEventServiceFuture = analyticsAnalyticsEventServiceFuture;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.budgetingForecastReportingServiceBlocking = budgetingForecastReportingServiceBlocking;
        this.budgetingVarianceSyncServiceBlocking = budgetingVarianceSyncServiceBlocking;
        this.documentsDocumentServiceBlocking = documentsDocumentServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.lendingRepaymentApprovalServiceBlocking = lendingRepaymentApprovalServiceBlocking;
        this.mapper = mapper;
        this.paymentsPaymentQueryServiceBlocking = paymentsPaymentQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
    }

    /**
     * Activates a budget line.
     */
    public ActivateBudgetLineResponse activateBudgetLine(ActivateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "ActivateBudgetLine");

        var outbound0 = RpcSupport.unpack(Any.pack(CreateDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDetails().getEffectiveAt())
                                .build())
                        .setCounterparty(DocumentFolderRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build()), CreateDocumentRequest.class);
        var reply0 = documentsDocumentServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDocument(outbound0);

        var outbound1 = SubmitRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setRepayment(Repayment.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getDocument().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> lendingRepaymentApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitRepayment(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateBudgetLine", request.getDetails().getReasonCode());
        return ActivateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRepayment().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getRepayment().getLocation().getCountryCode())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setTotal(reply0.getDocument().getTotal())
                        .build())
                .setOutcome(ActivateBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Suspends a budget line.
     */
    public SuspendBudgetLineResponse suspendBudgetLine(SuspendBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "SuspendBudgetLine");

        var outbound0 = SuggestPaymentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestPaymentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var adapter0 = new Function<SuggestPaymentsRequest, SuggestPaymentsResponse>() {
            @Override
            public SuggestPaymentsResponse apply(SuggestPaymentsRequest input) {
                return paymentsPaymentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestPayments(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendBudgetLine", request.getDetails().getReasonCode());
        return SuspendBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setOutcome(SuspendBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Reinstates a budget line.
     */
    public ReinstateBudgetLineResponse reinstateBudgetLine(ReinstateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "ReinstateBudgetLine");

        var outbound0 = UpdateAnalyticsEventRequest.newBuilder()
                .setAnalyticsEvent(AnalyticsEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(FunnelRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = analyticsAnalyticsEventServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).updateAnalyticsEvent(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (PreviewSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRiskRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRisk(outbound1);

        var outbound2 = SyncPremiumsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncPremiumsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSupplierRisk().getAudit().getCreatedAt())
                                .setUpdatedBy(reply1.getSupplierRisk().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getSupplierRisk().getAssignee().getDisplayName())
                                .build())
                        .setNumber(switch (reply1.getSupplierRisk().getOriginCase()) {
                            case CUSTOMER -> reply1.getSupplierRisk().getCustomer().getNumber();
                            default -> "";
                        })
                        .build())
                .build();
        var reply2 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncPremiums(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateBudgetLine", request.getDetails().getReasonCode());
        return ReinstateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply2.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setOutcome(ReinstateBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAnalyticsEvent().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Terminates a budget line.
     */
    public TerminateBudgetLineResponse terminateBudgetLine(TerminateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "TerminateBudgetLine");

        var outbound0 = ListBudgetLineReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBudgetLineReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .build())
                .build();
        RpcPort<ListBudgetLineReportsRequest, ListBudgetLineReportsResponse> port0 = RpcPort.adapt(budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listBudgetLineReports);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateBudgetLine", request.getDetails().getReasonCode());
        return TerminateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setOutcome(TerminateBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Expires a budget line.
     */
    public ExpireBudgetLineResponse expireBudgetLine(ExpireBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "ExpireBudgetLine");

        var outbound0 = mapper.expireBudgetLineStage0(request);
        var reply0 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRisks(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getSupplierRisk().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build()), LintSupplierRatingsRequest.class);
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireBudgetLine", request.getDetails().getReasonCode());
        return ExpireBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setOutcome(ExpireBudgetLineResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public GetBudgetLineLifecycleResponse getBudgetLineLifecycle(GetBudgetLineLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetLineLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "GetBudgetLineLifecycle");

        var outbound0 = RpcSupport.unpack(Any.pack(ResolveBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .build()), ResolveBudgetLinesRequest.class);
        var reply0 = budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveBudgetLines(outbound0);
        return GetBudgetLineLifecycleResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules a budget line.
     */
    public ScheduleBudgetLineExpiryResponse scheduleBudgetLineExpiry(ScheduleBudgetLineExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleBudgetLineExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var futures0 = request.getBudgetLine().getAttachmentsList().stream()
                .map(element -> ReconcileVariancesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileVariancesRequest.Details.newBuilder()
                        .setChecksum(element.getChecksum())
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingVarianceSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileVariances(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ReconcileVariancesResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = JsonCodec.roundTrip(ScheduleForecastReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleForecastReportRequest.Details.newBuilder()
                        .putAllFilters(request.getBudgetLine().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build(), ScheduleForecastReportRequest.class);
        var reply1 = budgetingForecastReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleForecastReport(outbound1);

        var outbound2 = SuggestBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBudgetLinesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getVariance().getAudit().getUpdatedAt())
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBudgetLines(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = ListPremiumConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPremiumConflictsRequest.Filter.newBuilder()
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPremiumConflicts(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleBudgetLineExpiry", request.getBudgetLine().getBudgetLineId());
        return ScheduleBudgetLineExpiryResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setIssuedAt(request.getBudgetLine().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
