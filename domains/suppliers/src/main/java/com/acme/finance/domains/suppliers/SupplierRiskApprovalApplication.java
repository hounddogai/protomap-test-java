package com.acme.finance.domains.suppliers;

import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExplainPreferenceDecisionRequest;
import com.acme.customers.v1.ExplainPreferenceDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsRequest;
import com.acme.ledger.v1.ListJournalEntrySubscriptionsResponse;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.StreamPostingBatchNoticesRequest;
import com.acme.ledger.v1.StreamPostingBatchNoticesResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentReportRequest;
import com.acme.lending.v1.ScheduleRepaymentReportResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.suppliers.v1.ApproveSupplierRatingRequest;
import com.acme.suppliers.v1.ApproveSupplierRatingResponse;
import com.acme.suppliers.v1.ApproveSupplierRiskRequest;
import com.acme.suppliers.v1.ApproveSupplierRiskResponse;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalRequest;
import com.acme.suppliers.v1.CommentSupplierRiskApprovalResponse;
import com.acme.suppliers.v1.EscalateSupplierRiskRequest;
import com.acme.suppliers.v1.EscalateSupplierRiskResponse;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusRequest;
import com.acme.suppliers.v1.GetSupplierRiskApprovalStatusResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.ListPendingSupplierRisksRequest;
import com.acme.suppliers.v1.ListPendingSupplierRisksResponse;
import com.acme.suppliers.v1.RecallSupplierRatingRequest;
import com.acme.suppliers.v1.RecallSupplierRatingResponse;
import com.acme.suppliers.v1.RecallSupplierRiskRequest;
import com.acme.suppliers.v1.RecallSupplierRiskResponse;
import com.acme.suppliers.v1.RejectSupplierRiskRequest;
import com.acme.suppliers.v1.RejectSupplierRiskResponse;
import com.acme.suppliers.v1.SubmitSupplierRatingRequest;
import com.acme.suppliers.v1.SubmitSupplierRatingResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskSla;
import java.util.ArrayList;
import java.util.List;
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
 * Routes supplier risks through approval, with escalation and delegation. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class SupplierRiskApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(SupplierRiskApprovalApplication.class);

    private final DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking;

    private final SuppliersRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceFutureStub suppliersSupplierContractQueryServiceFuture;

    private final SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking;

    public SupplierRiskApprovalApplication(
            DepreciationWorkflowServiceGrpc.DepreciationWorkflowServiceBlockingStub assetsDepreciationWorkflowServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            RepaymentReportingServiceGrpc.RepaymentReportingServiceBlockingStub lendingRepaymentReportingServiceBlocking,
            SuppliersRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractApprovalServiceGrpc.SupplierContractApprovalServiceBlockingStub suppliersSupplierContractApprovalServiceBlocking,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceFutureStub suppliersSupplierContractQueryServiceFuture,
            SupplierRatingApprovalServiceGrpc.SupplierRatingApprovalServiceBlockingStub suppliersSupplierRatingApprovalServiceBlocking) {
        this.assetsDepreciationWorkflowServiceBlocking = assetsDepreciationWorkflowServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.customersPreferenceValidationServiceBlocking = customersPreferenceValidationServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.lendingRepaymentReportingServiceBlocking = lendingRepaymentReportingServiceBlocking;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractApprovalServiceBlocking = suppliersSupplierContractApprovalServiceBlocking;
        this.suppliersSupplierContractQueryServiceFuture = suppliersSupplierContractQueryServiceFuture;
        this.suppliersSupplierRatingApprovalServiceBlocking = suppliersSupplierRatingApprovalServiceBlocking;
    }

    /**
     * Submits a supplier risk.
     */
    public SubmitSupplierRiskResponse submitSupplierRisk(SubmitSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = CancelExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CancelExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setName(request.getSupplierRisk().getRequester().getName())
                                .setPhone(request.getSupplierRisk().getRequester().getPhone())
                                .build())
                        .build())
                .setExposure(Exposure.newBuilder()
                        .setHolder(SupplierRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getSupplierRisk().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.risk.v1.ExposureWorkflowService/CancelExposureWorkflow", outbound0);
        var reply0 = riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelExposureWorkflow(command0.payload());

        var outbound1 = JsonCodec.roundTrip(ListPostingBatchSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPostingBatchSubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getExposure().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getExposure().getLabelsMap())
                        .build())
                .build(), ListPostingBatchSubscriptionsRequest.class);
        var reply1 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPostingBatchSubscriptions(outbound1);

        var futures2 = request.getSupplierRisk().getHistoryList().stream()
                .map(element -> ApproveSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveSupplierRatingRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getSupplierRisk().getAssignee().getUserId())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getChangedAt())
                                .setUpdatedBy(request.getSupplierRisk().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveSupplierRating(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? ApproveSupplierRatingResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return SubmitSupplierRiskResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getExposure().getContact().getEmail())
                                .build())
                        .setStatus(request.getSupplierRisk().getStatus())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Approves a supplier risk.
     */
    public ApproveSupplierRiskResponse approveSupplierRisk(ApproveSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApproveSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = SummarizeSupplierContractsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSupplierContractsRequest.Details.newBuilder()
                        .putAllFilters(request.getSupplierRisk().getCustomFieldsMap())
                        .build())
                .setReason(request.getReason())
                .setSupplierContractId(request.getSupplierRisk().getCustomer().getSupplierContractId())
                .build();
        var pending0 = suppliersSupplierContractQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSupplierContracts(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApproveSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return ApproveSupplierRiskResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setPriority(request.getSupplierRisk().getPriority())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Rejects a supplier risk.
     */
    public RejectSupplierRiskResponse rejectSupplierRisk(RejectSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = ListDepreciationWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDepreciationWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRisk().getAudit().getCreatedAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getSupplierRisk().getAssignee().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDepreciationWorkflows(outbound0), rpcExecutor);

        var outbound1 = ListPendingSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRisk().getSla().getDueAt())
                        .setOwnerId(request.getSupplierRisk().getCustomer().getSupplierContractId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingSupplierContracts(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return RejectSupplierRiskResponse.newBuilder()
                .setOutcome(RejectSupplierRiskResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .putAllCustomFields(request.getSupplierRisk().getCustomFieldsMap())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Escalates a supplier risk.
     */
    public EscalateSupplierRiskResponse escalateSupplierRisk(EscalateSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalateSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = SubmitSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitSupplierRatingRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getSupplierRisk().getAssignee().getUserId())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getSupplierRisk().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSupplierRisk().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitSupplierRating(outbound0);

        var builder1 = RecallSupplierRatingRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setSupplierRating(SupplierRating.newBuilder()
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(request.getSupplierRisk().getAudit().getEtag())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallSupplierRating(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalateSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return EscalateSupplierRiskResponse.newBuilder()
                .setOutcome(EscalateSupplierRiskResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .setSla(SupplierRiskSla.newBuilder()
                                .setDueAt(reply1.getSupplierRating().getDueAt())
                                .build())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Lists supplier risks a page at a time.
     */
    public ListPendingSupplierRisksResponse listPendingSupplierRisks(ListPendingSupplierRisksRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingSupplierRisks")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRisk", request.getContext().getTraceId(), "ListPendingSupplierRisks");

        var builder0 = CommentSupplierRatingApprovalRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDetails(CommentSupplierRatingApprovalRequest.Details.newBuilder()
                .setApprover(UserRef.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .build());
        builder0.setSupplierRating(SupplierRating.newBuilder()
                .setBillingAddress(PostalAddress.newBuilder()
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentSupplierRatingApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingSupplierRisks", request.getPage().getPageToken());
        return ListPendingSupplierRisksResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a supplier risk.
     */
    public RecallSupplierRiskResponse recallSupplierRisk(RecallSupplierRiskRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallSupplierRisk")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = ListJournalEntrySubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntrySubscriptionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRisk().getAudit().getUpdatedAt())
                        .putAllLabels(request.getSupplierRisk().getCustomFieldsMap())
                        .setOwnerId(switch (request.getSupplierRisk().getOriginCase()) {
                            case CUSTOMER -> request.getSupplierRisk().getCustomer().getSupplierContractId();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntrySubscriptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallSupplierRisk", request.getSupplierRisk().getSupplierRiskId());
        return RecallSupplierRiskResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setPriority(request.getSupplierRisk().getPriority())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Returns a supplier risk.
     */
    public GetSupplierRiskApprovalStatusResponse getSupplierRiskApprovalStatus(GetSupplierRiskApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetSupplierRiskApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "SupplierRisk", request.getSupplierRiskId(), "GetSupplierRiskApprovalStatus");

        var outbound0 = ScoreHouseholdRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(request.getContext().getActor().getEmail())
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreHousehold(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ExplainPreferenceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getHousehold().getAudit().getCreatedBy())
                                .setUpdatedAt(reply0.getHousehold().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getHousehold().getManager().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> customersPreferenceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainPreferenceDecision(value));
        return GetSupplierRiskApprovalStatusResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                .build())
                        .setPriority(reply1.getPreference().getPriority())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }

    /**
     * Comments a supplier risk.
     */
    public CommentSupplierRiskApprovalResponse commentSupplierRiskApproval(CommentSupplierRiskApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CommentSupplierRiskApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveSupplierRisk(request.getContext().getTenantId(), request.getSupplierRisk());

        var outbound0 = ApproveSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ApproveSupplierRatingRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getSupplierRisk().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ApproveSupplierRatingRequest, ApproveSupplierRatingResponse> port0 = RpcPort.adapt(suppliersSupplierRatingApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::approveSupplierRating);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = JsonCodec.roundTrip(ScheduleRepaymentReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build(), ScheduleRepaymentReportRequest.class);
        var reply1 = lendingRepaymentReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleRepaymentReport(outbound1);

        var results2 = new ArrayList<StreamPostingBatchNoticesResponse>();
        for (var element : request.getSupplierRisk().getCommentsList()) {
            StreamPostingBatchNoticesRequest itemRequest = StreamPostingBatchNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(element.getAuthorEmail())
                        .setPreferredChannel(request.getSupplierRisk().getRequester().getPreferredChannel())
                        .build())
                .build();
            var responses = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamPostingBatchNotices(itemRequest);
            responses.forEachRemaining(results2::add);
        }
        var reply2 = results2.isEmpty() ? StreamPostingBatchNoticesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getSupplierRisk().getSupplierRiskId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CommentSupplierRiskApproval", request.getSupplierRisk().getSupplierRiskId());
        return CommentSupplierRiskApprovalResponse.newBuilder()
                .setOutcome(CommentSupplierRiskApprovalResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .setSupplierRiskId(request.getSupplierRiskId())
                        .build())
                .build();
    }
}
