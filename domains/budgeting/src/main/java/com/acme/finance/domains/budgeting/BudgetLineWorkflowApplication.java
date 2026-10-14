package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.banking.v1.DelegateStandingOrderApprovalRequest;
import com.acme.banking.v1.DelegateStandingOrderApprovalResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastApprovalServiceGrpc;
import com.acme.budgeting.v1.ForecastBankSettlement;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetLineWorkflowsResponse;
import com.acme.budgeting.v1.ListPendingForecastsRequest;
import com.acme.budgeting.v1.ListPendingForecastsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.ResolveForecastConflictRequest;
import com.acme.budgeting.v1.ResolveForecastConflictResponse;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.SignalBudgetWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetLineWorkflowResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.ScoreUserRequest;
import com.acme.identity.v1.ScoreUserResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodAdminServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RepairFiscalPeriodRequest;
import com.acme.ledger.v1.RepairFiscalPeriodResponse;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralApprovalServiceGrpc;
import com.acme.lending.v1.CollateralComponent;
import com.acme.lending.v1.SubmitCollateralRequest;
import com.acme.lending.v1.SubmitCollateralResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.GetSupplierRequest;
import com.acme.suppliers.v1.GetSupplierResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a budget line from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BudgetLineWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineWorkflowApplication.class);

    private final DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking;

    private final FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final CollateralApprovalServiceGrpc.CollateralApprovalServiceBlockingStub lendingCollateralApprovalServiceBlocking;

    private final BudgetingRepository repository;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public BudgetLineWorkflowApplication(
            DisposalReportingServiceGrpc.DisposalReportingServiceBlockingStub assetsDisposalReportingServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            BudgetWorkflowServiceGrpc.BudgetWorkflowServiceBlockingStub budgetingBudgetWorkflowServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UserValidationServiceGrpc.UserValidationServiceBlockingStub identityUserValidationServiceBlocking,
            FiscalPeriodAdminServiceGrpc.FiscalPeriodAdminServiceBlockingStub ledgerFiscalPeriodAdminServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            CollateralApprovalServiceGrpc.CollateralApprovalServiceBlockingStub lendingCollateralApprovalServiceBlocking,
            BudgetingRepository repository,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.assetsDisposalReportingServiceBlocking = assetsDisposalReportingServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.budgetingBudgetWorkflowServiceBlocking = budgetingBudgetWorkflowServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.identityUserValidationServiceBlocking = identityUserValidationServiceBlocking;
        this.ledgerFiscalPeriodAdminServiceBlocking = ledgerFiscalPeriodAdminServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.lendingCollateralApprovalServiceBlocking = lendingCollateralApprovalServiceBlocking;
        this.repository = repository;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    public record AdvanceBudgetLineWorkflowStage1Requested(UpdateUserRequest request, CompletableFuture<UpdateUserResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a budget line.
     */
    public StartBudgetLineWorkflowResponse startBudgetLineWorkflow(StartBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ResolveForecastConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolveForecastConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setBank(ForecastBankSettlement.newBuilder()
                                .setBic(switch (request.getBudgetLine().getSettlementCase()) {
                                    case BANK -> request.getBudgetLine().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(request.getBudgetLine().getBillingAddress().getCountryCode())
                                .addAllLines(request.getBudgetLine().getBillingAddress().getLinesList())
                                .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveForecastConflict(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return StartBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getForecast().getBillingAddress().getLocality())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setNumber(request.getBudgetLine().getNumber())
                        .build())
                .build();
    }

    /**
     * Advances a budget line.
     */
    public AdvanceBudgetLineWorkflowResponse advanceBudgetLineWorkflow(AdvanceBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = SignalBudgetWorkflowRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudgetLine().getAudit().getCreatedAt())
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setToken(switch (request.getBudgetLine().getSettlementCase()) {
                                    case CARD -> request.getBudgetLine().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalBudgetWorkflow(outbound0);

        var outbound1 = UpdateUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getBudget().getAudit().getCreatedBy())
                                .setUpdatedBy(reply0.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getBudget().getContact().getEmail())
                                .setName(reply0.getBudget().getContact().getName())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<UpdateUserResponse>();
        events.publishEvent(new AdvanceBudgetLineWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = RepairFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RepairFiscalPeriodRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getBudgetLine().getCounterparty().getEmail())
                                        .setPhone(reply1.getUser().getContact().getPhone())
                                        .setPreferredChannel(reply1.getUser().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        Function<RepairFiscalPeriodRequest, RepairFiscalPeriodResponse> operation2 = ledgerFiscalPeriodAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::repairFiscalPeriod;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return AdvanceBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setTotal(request.getBudgetLine().getTotal())
                        .build())
                .setOutcome(AdvanceBudgetLineWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .setReferenceId(reply2.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Cancels a budget line.
     */
    public CancelBudgetLineWorkflowResponse cancelBudgetLineWorkflow(CancelBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = DelegateStandingOrderApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegateStandingOrderApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBudgetLine().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getBudgetLine().getCounterparty().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateStandingOrderApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return CancelBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setStatus(request.getBudgetLine().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a budget line.
     */
    public RetryBudgetLineWorkflowResponse retryBudgetLineWorkflow(RetryBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = (RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotify().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(outbound0);

        var outbound1 = ListPendingForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPendingForecastsRequest.Filter.newBuilder()
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.budgeting.v1.ForecastApprovalService");
        var selected1 = ForecastApprovalServiceGrpc.newBlockingStub(channel1)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply1 = selected1.listPendingForecasts(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return RetryBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setStatus(request.getBudgetLine().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Resumes a budget line.
     */
    public ResumeBudgetLineWorkflowResponse resumeBudgetLineWorkflow(ResumeBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ScoreUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> identityUserValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreUser(value));

        List<SubmitCollateralResponse> results1 = request.getBudgetLine().getLinesList().stream()
                .map(element -> SubmitCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getBudgetLine().getBillingAddress().getPostalCode())
                                .build())
                        .putComponents("primary", CollateralComponent.newBuilder()
                                .setDescription(element.getDescription())
                                .setQuantity(element.getQuantity())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getUser().getRelationshipManager().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(lendingCollateralApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitCollateral(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? SubmitCollateralResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = RevertFiscalPeriodRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertFiscalPeriodRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .addParticipants(FiscalPeriodParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getCollateral().getOwner().getEmail())
                                        .setPhone(request.getDetails().getNotify().getPhone())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertFiscalPeriod(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return ResumeBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getUser().getAudit().getEtag())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .putAllMetadata(request.getBudgetLine().getMetadataMap())
                        .build())
                .setOutcome(ResumeBudgetLineWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public InspectBudgetLineWorkflowResponse inspectBudgetLineWorkflow(InspectBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "InspectBudgetLineWorkflow");

        var outbound0 = ExportSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getBudgetLineId())
                        .build())
                .build();
        var stream0 = suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSuppliers(outbound0);
        var reply0 = ExportSuppliersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        return InspectBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public ListBudgetLineWorkflowsResponse listBudgetLineWorkflows(ListBudgetLineWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBudgetLineWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "ListBudgetLineWorkflows");

        var outbound0 = AdvanceBudgetWorkflowRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.BudgetWorkflowService");
        var selected0 = BudgetWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.advanceBudgetWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBudgetLineWorkflows", request.getPage().getPageToken());
        return ListBudgetLineWorkflowsResponse.newBuilder()
                .addBudgetLines(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a budget line.
     */
    public SignalBudgetLineWorkflowResponse signalBudgetLineWorkflow(SignalBudgetLineWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalBudgetLineWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ExportSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSupplierContractsRequest.Filter.newBuilder()
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSupplierContracts(outbound0);
        var reply0 = ExportSupplierContractsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = BeanCopies.copy(GetSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierId(request.getBudgetLine().getCounterparty().getSupplierId())
                .build(), GetSupplierRequest.class);
        var reply1 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getSupplier(outbound1);

        var outbound2 = ScheduleDisposalReportRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        RpcPort<ScheduleDisposalReportRequest, ScheduleDisposalReportResponse> port2 = RpcPort.adapt(assetsDisposalReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleDisposalReport);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = (ListVarianceReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getSupplier().getAudit().getUpdatedAt())
                        .setRegion(request.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply3 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listVarianceReports(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalBudgetLineWorkflow", request.getBudgetLine().getBudgetLineId());
        return SignalBudgetLineWorkflowResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setStatus(request.getBudgetLine().getStatus())
                        .build())
                .setOutcome(SignalBudgetLineWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
