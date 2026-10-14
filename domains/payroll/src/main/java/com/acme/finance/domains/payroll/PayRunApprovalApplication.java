package com.acme.finance.domains.payroll;

import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseComment;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractWorkflowServiceGrpc;
import com.acme.contracts.v1.RenewalRef;
import com.acme.contracts.v1.ResumeContractWorkflowRequest;
import com.acme.contracts.v1.ResumeContractWorkflowResponse;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.ListPendingClaimsRequest;
import com.acme.insurance.v1.ListPendingClaimsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.payroll.v1.ActivateDeductionRequest;
import com.acme.payroll.v1.ActivateDeductionResponse;
import com.acme.payroll.v1.ApprovePayRunRequest;
import com.acme.payroll.v1.ApprovePayRunResponse;
import com.acme.payroll.v1.CountPayStubsRequest;
import com.acme.payroll.v1.CountPayStubsResponse;
import com.acme.payroll.v1.CreatePayRunRequest;
import com.acme.payroll.v1.CreatePayRunResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionLifecycleServiceGrpc;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeductionRouteAction;
import com.acme.payroll.v1.DelegatePayRunApprovalRequest;
import com.acme.payroll.v1.DelegatePayRunApprovalResponse;
import com.acme.payroll.v1.EscalatePayRunRequest;
import com.acme.payroll.v1.EscalatePayRunResponse;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.payroll.v1.GetPayRunApprovalStatusRequest;
import com.acme.payroll.v1.GetPayRunApprovalStatusResponse;
import com.acme.payroll.v1.ListDeductionReportsRequest;
import com.acme.payroll.v1.ListDeductionReportsResponse;
import com.acme.payroll.v1.ListPendingPayRunsRequest;
import com.acme.payroll.v1.ListPendingPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.RecallPayRunRequest;
import com.acme.payroll.v1.RecallPayRunResponse;
import com.acme.payroll.v1.RejectPayRunRequest;
import com.acme.payroll.v1.RejectPayRunResponse;
import com.acme.payroll.v1.ScheduleDeductionExpiryRequest;
import com.acme.payroll.v1.ScheduleDeductionExpiryResponse;
import com.acme.payroll.v1.SubmitPayRunRequest;
import com.acme.payroll.v1.SubmitPayRunResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingReportingServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminateDeductionRequest;
import com.acme.payroll.v1.TerminateDeductionResponse;
import com.acme.payroll.v1.ValidateTaxWithholdingRequest;
import com.acme.payroll.v1.ValidateTaxWithholdingResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RecallPurchaseOrderRequest;
import com.acme.procurement.v1.RecallPurchaseOrderResponse;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.WatchBuildingsRequest;
import com.acme.workplaces.v1.WatchBuildingsResponse;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Routes pay runs through approval, with escalation and delegation. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class PayRunApprovalApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunApprovalApplication.class);

    private final CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking;

    private final ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking;

    private final DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking;

    private final PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking;

    private final TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking;

    private final TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking;

    private final PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking;

    private final PayrollRepository repository;

    private final BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking;

    public PayRunApprovalApplication(
            CollectionCaseWorkflowServiceGrpc.CollectionCaseWorkflowServiceBlockingStub collectionsCollectionCaseWorkflowServiceBlocking,
            ContractWorkflowServiceGrpc.ContractWorkflowServiceBlockingStub contractsContractWorkflowServiceBlocking,
            ApplicationEventPublisher events,
            ClaimApprovalServiceGrpc.ClaimApprovalServiceBlockingStub insuranceClaimApprovalServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            DeductionLifecycleServiceGrpc.DeductionLifecycleServiceBlockingStub payrollDeductionLifecycleServiceBlocking,
            DeductionReportingServiceGrpc.DeductionReportingServiceBlockingStub payrollDeductionReportingServiceBlocking,
            PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking,
            PayStubQueryServiceGrpc.PayStubQueryServiceBlockingStub payrollPayStubQueryServiceBlocking,
            TaxWithholdingReportingServiceGrpc.TaxWithholdingReportingServiceBlockingStub payrollTaxWithholdingReportingServiceBlocking,
            TaxWithholdingValidationServiceGrpc.TaxWithholdingValidationServiceBlockingStub payrollTaxWithholdingValidationServiceBlocking,
            PurchaseOrderApprovalServiceGrpc.PurchaseOrderApprovalServiceBlockingStub procurementPurchaseOrderApprovalServiceBlocking,
            PayrollRepository repository,
            BuildingServiceGrpc.BuildingServiceBlockingStub workplacesBuildingServiceBlocking) {
        this.collectionsCollectionCaseWorkflowServiceBlocking = collectionsCollectionCaseWorkflowServiceBlocking;
        this.contractsContractWorkflowServiceBlocking = contractsContractWorkflowServiceBlocking;
        this.events = events;
        this.insuranceClaimApprovalServiceBlocking = insuranceClaimApprovalServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.payrollDeductionLifecycleServiceBlocking = payrollDeductionLifecycleServiceBlocking;
        this.payrollDeductionReportingServiceBlocking = payrollDeductionReportingServiceBlocking;
        this.payrollPayRunServiceBlocking = payrollPayRunServiceBlocking;
        this.payrollPayStubQueryServiceBlocking = payrollPayStubQueryServiceBlocking;
        this.payrollTaxWithholdingReportingServiceBlocking = payrollTaxWithholdingReportingServiceBlocking;
        this.payrollTaxWithholdingValidationServiceBlocking = payrollTaxWithholdingValidationServiceBlocking;
        this.procurementPurchaseOrderApprovalServiceBlocking = procurementPurchaseOrderApprovalServiceBlocking;
        this.repository = repository;
        this.workplacesBuildingServiceBlocking = workplacesBuildingServiceBlocking;
    }

    public record RejectPayRunStage0Requested(PreviewNotificationRequest request, CompletableFuture<PreviewNotificationResponse> completion, io.grpc.Context context) {}

    /**
     * Submits a pay run.
     */
    public SubmitPayRunResponse submitPayRun(SubmitPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SubmitPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = ResumeContractWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setContract(Contract.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .setCounterparty(RenewalRef.newBuilder()
                                .setDisplayName(request.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> contractsContractWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resumeContractWorkflow(value)).orElseThrow();

        var outbound1 = WatchBuildingsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchBuildingsRequest.Filter.newBuilder()
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = workplacesBuildingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchBuildings(outbound1);
        var reply1 = WatchBuildingsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SubmitPayRun", request.getPayRun().getPayRunId());
        return SubmitPayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setDueAt(reply0.getContract().getDueAt())
                        .putAllMetadata(request.getPayRun().getMetadataMap())
                        .setNotes(reply0.getContract().getNotes())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Approves a pay run.
     */
    public ApprovePayRunResponse approvePayRun(ApprovePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ApprovePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = TerminateDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                .setDetails(TerminateDeductionRequest.Details.newBuilder()
                        .setNewOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getApprover().getUserId())
                                .build())
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).terminateDeduction(value)).orElseThrow();

        var outbound1 = ScheduleDeductionExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDeduction(Deduction.newBuilder()
                        .setRoute(DeductionRouteAction.newBuilder()
                                .setTarget(switch (reply0.getDeduction().getActionCase()) {
                                    case ROUTE -> reply0.getDeduction().getRoute().getTarget();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleDeductionExpiry(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ApprovePayRun", request.getPayRun().getPayRunId());
        return ApprovePayRunResponse.newBuilder()
                .setOutcome(ApprovePayRunResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getDeduction().getAudit().getCreatedAt())
                                .build())
                        .setNotes(request.getPayRun().getNotes())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }

    /**
     * Rejects a pay run.
     */
    public RejectPayRunResponse rejectPayRun(RejectPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RejectPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = PreviewNotificationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewNotificationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<PreviewNotificationResponse>();
        events.publishEvent(new RejectPayRunStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = ListPendingClaimsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingClaimsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getNotification().getAudit().getUpdatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(switch (reply0.getNotification().getOriginCase()) {
                            case CUSTOMER -> reply0.getNotification().getCustomer().getNotificationTemplateId();
                            default -> "";
                        })
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = insuranceClaimApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPendingClaims(outbound1);

        var outbound2 = LintLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(reply0.getNotification().getRequester().getPhone())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintLedgerAccounts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RejectPayRun", request.getPayRun().getPayRunId());
        return RejectPayRunResponse.newBuilder()
                .setOutcome(RejectPayRunResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply2.getLedgerAccount().getAudit().getUpdatedBy())
                                .build())
                        .setDueAt(request.getPayRun().getDueAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Escalates a pay run.
     */
    public EscalatePayRunResponse escalatePayRun(EscalatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EscalatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = ActivateDeductionRequest.newBuilder()
                .setContext(request.getContext())
                .setDeductionId(request.getPayRun().getCounterparty().getDeductionId())
                .setDetails(ActivateDeductionRequest.Details.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setNewOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getApprover().getDisplayName())
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<ActivateDeductionRequest, ActivateDeductionResponse> port0 = RpcPort.adapt(payrollDeductionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::activateDeduction);
        var reply0 = port0.exchange(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EscalatePayRun", request.getPayRun().getPayRunId());
        return EscalatePayRunResponse.newBuilder()
                .setOutcome(EscalatePayRunResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setPayRunId(request.getPayRunId())
                        .setStatus(request.getPayRun().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists pay runs a page at a time.
     */
    public ListPendingPayRunsResponse listPendingPayRuns(ListPendingPayRunsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListPendingPayRuns")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getContext().getTraceId(), "ListPendingPayRuns");

        var outbound0 = GenerateTaxWithholdingReportRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(GenerateTaxWithholdingReportRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = payrollTaxWithholdingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateTaxWithholdingReport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListPendingPayRuns", request.getPage().getPageToken());
        return ListPendingPayRunsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Delegates a pay run.
     */
    public DelegatePayRunApprovalResponse delegatePayRunApproval(DelegatePayRunApprovalRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DelegatePayRunApproval")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = RecallPurchaseOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallPurchaseOrderRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setPurchaseOrder(PurchaseOrder.newBuilder()
                        .setCard(PurchaseOrderCardSettlement.newBuilder()
                                .setLast4(switch (request.getPayRun().getSettlementCase()) {
                                    case CARD -> request.getPayRun().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(PurchaseRequisitionRef.newBuilder()
                                .setDisplayName(request.getPayRun().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = procurementPurchaseOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallPurchaseOrder(outbound0);

        var outbound1 = JsonCodec.roundTrip(ValidateTaxWithholdingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateTaxWithholdingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getApprover().getEmail())
                                .build())
                        .build())
                .setTaxWithholding(TaxWithholding.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayRun().getAudit().getEtag())
                                .setUpdatedBy(request.getPayRun().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build(), ValidateTaxWithholdingRequest.class);
        var reply1 = payrollTaxWithholdingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateTaxWithholding(outbound1);

        var outbound2 = RetryCollectionCaseWorkflowRequest.newBuilder()
                .setCollectionCase(CollectionCase.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getTaxWithholding().getAudit().getEtag())
                                .setUpdatedBy(reply0.getPurchaseOrder().getAudit().getUpdatedBy())
                                .build())
                        .addComments(CollectionCaseComment.newBuilder()
                                .setCreatedAt(reply0.getPurchaseOrder().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command2 = new DispatchCommand<>("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow", outbound2);
        var reply2 = collectionsCollectionCaseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryCollectionCaseWorkflow(command2.payload());

        var outbound3 = ListDeductionReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListDeductionReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getPayRun().getAudit().getUpdatedAt())
                        .putAllLabels(reply2.getCollectionCase().getCustomFieldsMap())
                        .setOwnerId(switch (reply2.getCollectionCase().getOriginCase()) {
                            case CUSTOMER -> reply2.getCollectionCase().getCustomer().getPaymentPromiseId();
                            default -> "";
                        })
                        .setRegion(reply0.getPurchaseOrder().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = payrollDeductionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listDeductionReports(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DelegatePayRunApproval", request.getPayRun().getPayRunId());
        return DelegatePayRunApprovalResponse.newBuilder()
                .setOutcome(DelegatePayRunApprovalResponse.Outcome.newBuilder()
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getCollectionCase().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getPurchaseOrder().getBillingAddress().getCountryCode())
                                .build())
                        .addLines(PayRunLine.newBuilder()
                                .setAmount(reply1.getTaxWithholding().getDiscount().getAmount())
                                .build())
                        .putAllMetadata(request.getPayRun().getMetadataMap())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalls a pay run.
     */
    public RecallPayRunResponse recallPayRun(RecallPayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecallPayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = JsonCodec.roundTrip(CountPayStubsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountPayStubsRequest.Filter.newBuilder()
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build(), CountPayStubsRequest.class);
        var reply0 = payrollPayStubQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countPayStubs(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecallPayRun", request.getPayRun().getPayRunId());
        return RecallPayRunResponse.newBuilder()
                .setOutcome(RecallPayRunResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setNotes(request.getPayRun().getNotes())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public GetPayRunApprovalStatusResponse getPayRunApprovalStatus(GetPayRunApprovalStatusRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayRunApprovalStatus")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "GetPayRunApprovalStatus");

        var outbound0 = CreatePayRunRequest.newBuilder()
                .setContext(request.getContext())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(PayRunLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
        var adapter0 = new Function<CreatePayRunRequest, CreatePayRunResponse>() {
            @Override
            public CreatePayRunResponse apply(CreatePayRunRequest input) {
                return payrollPayRunServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createPayRun(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        return GetPayRunApprovalStatusResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getPayRun().getAudit().getCreatedBy())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }
}
