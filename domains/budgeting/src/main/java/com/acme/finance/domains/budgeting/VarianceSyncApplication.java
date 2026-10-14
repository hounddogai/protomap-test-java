package com.acme.finance.domains.budgeting;

import com.acme.banking.v1.CancelStandingOrderWorkflowRequest;
import com.acme.banking.v1.CancelStandingOrderWorkflowResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetServiceGrpc;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.GetVarianceSyncStateRequest;
import com.acme.budgeting.v1.GetVarianceSyncStateResponse;
import com.acme.budgeting.v1.ListVarianceConflictsRequest;
import com.acme.budgeting.v1.ListVarianceConflictsResponse;
import com.acme.budgeting.v1.ListVarianceImportsRequest;
import com.acme.budgeting.v1.ListVarianceImportsResponse;
import com.acme.budgeting.v1.PullVariancesRequest;
import com.acme.budgeting.v1.PullVariancesResponse;
import com.acme.budgeting.v1.PushVarianceRequest;
import com.acme.budgeting.v1.PushVarianceResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.ReconcileVariancesRequest;
import com.acme.budgeting.v1.ReconcileVariancesResponse;
import com.acme.budgeting.v1.ResolveVarianceConflictRequest;
import com.acme.budgeting.v1.ResolveVarianceConflictResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.SyncVariancesRequest;
import com.acme.budgeting.v1.SyncVariancesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.budgeting.v1.WatchBudgetsRequest;
import com.acme.budgeting.v1.WatchBudgetsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.EscalateClaimRequest;
import com.acme.insurance.v1.EscalateClaimResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.RenewPolicyRequest;
import com.acme.insurance.v1.RenewPolicyResponse;
import com.acme.insurance.v1.StartPolicyWorkflowRequest;
import com.acme.insurance.v1.StartPolicyWorkflowResponse;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.risk.v1.CancelRiskRegisterWorkflowRequest;
import com.acme.risk.v1.CancelRiskRegisterWorkflowResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskStatusChange;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
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
 * Keeps variances in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class VarianceSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceSyncApplication.class);

    private final StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final BudgetServiceGrpc.BudgetServiceBlockingStub budgetingBudgetServiceBlocking;

    private final VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking;

    private final ChannelRegistry channels;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking;

    private final PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final BudgetingRepository repository;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public VarianceSyncApplication(
            StandingOrderWorkflowServiceGrpc.StandingOrderWorkflowServiceBlockingStub bankingStandingOrderWorkflowServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            BudgetServiceGrpc.BudgetServiceBlockingStub budgetingBudgetServiceBlocking,
            VarianceImportServiceGrpc.VarianceImportServiceBlockingStub budgetingVarianceImportServiceBlocking,
            ChannelRegistry channels,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            PolicyLifecycleServiceGrpc.PolicyLifecycleServiceBlockingStub insurancePolicyLifecycleServiceBlocking,
            PolicyWorkflowServiceGrpc.PolicyWorkflowServiceBlockingStub insurancePolicyWorkflowServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            BudgetingRepository repository,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.bankingStandingOrderWorkflowServiceBlocking = bankingStandingOrderWorkflowServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingBudgetServiceBlocking = budgetingBudgetServiceBlocking;
        this.budgetingVarianceImportServiceBlocking = budgetingVarianceImportServiceBlocking;
        this.channels = channels;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.insurancePolicyLifecycleServiceBlocking = insurancePolicyLifecycleServiceBlocking;
        this.insurancePolicyWorkflowServiceBlocking = insurancePolicyWorkflowServiceBlocking;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    /**
     * Syncs a variance.
     */
    public SyncVariancesResponse syncVariances(SyncVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = CancelRiskRegisterWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskRegister(RiskRegister.newBuilder()
                        .addChildren(RiskRegister.newBuilder()
                                .addChildren(RiskRegister.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                                .setEmail(request.getVariance().getRequester().getEmail())
                                                .setUserId(request.getVariance().getAssignee().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelRiskRegisterWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncVariances", request.getVariance().getVarianceId());
        return SyncVariancesResponse.newBuilder()
                .setOutcome(SyncVariancesResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Pushs a variance.
     */
    public PushVarianceResponse pushVariance(PushVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = StartPolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        Function<StartPolicyWorkflowRequest, StartPolicyWorkflowResponse> operation0 = insurancePolicyWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::startPolicyWorkflow;
        var reply0 = operation0.apply(outbound0);

        var outbound1 = CreateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getPolicy().getContact().getName())
                                .build())
                        .setRelationshipManager(UserRef.newBuilder()
                                .setDisplayName(reply0.getPolicy().getHolder().getDisplayName())
                                .setUserId(request.getVariance().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSupplier(value));

        var outbound2 = RenewPolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RenewPolicyRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setUserId(reply1.getSupplier().getRelationshipManager().getUserId())
                                .build())
                        .build())
                .setPolicyId(request.getVariance().getSla().getPolicyId())
                .build();
        Function<RenewPolicyRequest, RenewPolicyResponse> operation2 = insurancePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::renewPolicy;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushVariance", request.getVariance().getVarianceId());
        return PushVarianceResponse.newBuilder()
                .setOutcome(PushVarianceResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .setVariance(Variance.newBuilder()
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply2.getPolicy().getAudit().getCreatedAt())
                                .build())
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply2.getPolicy().getContact().getPreferredChannel())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Pulls a variance.
     */
    public PullVariancesResponse pullVariances(PullVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = EscalateClaimRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EscalateClaimRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getVariance().getCustomer().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).escalateClaim(outbound0);

        var outbound1 = ExplainBudgetLineDecisionRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getClaim().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getClaim().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBudgetLineDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetLineDecision(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullVariances", request.getVariance().getVarianceId());
        return PullVariancesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getClaim().getAssignee().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setPriority(request.getVariance().getPriority())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Reconciles a variance.
     */
    public ReconcileVariancesResponse reconcileVariances(ReconcileVariancesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileVariances")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = BatchGetSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSuppliers(outbound0), rpcExecutor);

        var outbound1 = RecalculateBudgetLinesRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBudgetLines(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileVariances", request.getVariance().getVarianceId());
        return ReconcileVariancesResponse.newBuilder()
                .setOutcome(ReconcileVariancesResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setSubject(request.getVariance().getSubject())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of variances with a peer.
     */
    public StreamVarianceChangesResponse streamVarianceChanges(StreamVarianceChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamVarianceChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveVariance(request.getContext().getTenantId(), request.getChange());
        }

        List<ListCollateralWorkflowsResponse> results0 = request.getChange().getHistoryList().stream()
                .map(element -> ListCollateralWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCollateralWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(request.getChange().getCustomFieldsMap())
                        .setOwnerId(request.getChange().getCustomer().getBudgetLineId())
                        .build())
                .build())
                .flatMap(value -> Stream.of(lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCollateralWorkflows(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? ListCollateralWorkflowsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = WatchBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setPhone(request.getChange().getRequester().getPhone())
                        .build())
                .build();
        var stream1 = budgetingBudgetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchBudgets(outbound1);
        var reply1 = WatchBudgetsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getChange().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamVarianceChanges", request.getChange().getVarianceId());
        return StreamVarianceChangesResponse.newBuilder()
                .setSequence(request.getAck().getSequence())
                .setVariance(Variance.newBuilder()
                        .putAllCustomFields(request.getChange().getCustomFieldsMap())
                        .build())
                .build();
    }

    /**
     * Resolves a variance.
     */
    public ResolveVarianceConflictResponse resolveVarianceConflict(ResolveVarianceConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveVarianceConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        List<PreviewSupplierRiskResponse> results0 = request.getVariance().getHistoryList().stream()
                .map(element -> PreviewSupplierRiskRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .addHistory(SupplierRiskStatusChange.newBuilder()
                                .setToStatus(element.getToStatus())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewSupplierRisk(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? PreviewSupplierRiskResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = RpcSupport.unpack(Any.pack(CancelStandingOrderWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getVariance().getRequester().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()), CancelStandingOrderWorkflowRequest.class);
        var reply1 = bankingStandingOrderWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cancelStandingOrderWorkflow(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveVarianceConflict", request.getVariance().getVarianceId());
        return ResolveVarianceConflictResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceSyncStateResponse getVarianceSyncState(GetVarianceSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceSyncState");

        var outbound0 = RpcSupport.unpack(Any.pack(RecallRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallRiskScenarioRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()), RecallRiskScenarioRequest.class);
        var reply0 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallRiskScenario(outbound0);
        return GetVarianceSyncStateResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .addAllTags(reply0.getRiskScenario().getTagsList())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Lists variances a page at a time.
     */
    public ListVarianceConflictsResponse listVarianceConflicts(ListVarianceConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListVarianceConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getContext().getTraceId(), "ListVarianceConflicts");

        var outbound0 = ListVarianceImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceImportsRequest.Filter.newBuilder()
                        .setQuery(request.getFilter().getQuery())
                        .addAllStatuses(request.getFilter().getStatusesList())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingVarianceImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVarianceImports(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = BatchGetSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel1 = channels.lookup("acme.suppliers.v1.SupplierService");
        var selected1 = SupplierServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.batchGetSuppliers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListVarianceConflicts", request.getPage().getPageToken());
        return ListVarianceConflictsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setNextPageToken(reply0.getPageInfo().getNextPageToken())
                        .build())
                .build();
    }
}
