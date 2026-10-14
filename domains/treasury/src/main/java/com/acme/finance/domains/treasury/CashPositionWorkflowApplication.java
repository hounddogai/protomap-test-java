package com.acme.finance.domains.treasury;

import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.banking.v1.DelegateMandateApprovalRequest;
import com.acme.banking.v1.DelegateMandateApprovalResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateApprovalServiceGrpc;
import com.acme.banking.v1.MandateBankSettlement;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.AttestationReportingServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.CountAttestationTotalsRequest;
import com.acme.compliance.v1.CountAttestationTotalsResponse;
import com.acme.customers.v1.CompareHouseholdOptionsRequest;
import com.acme.customers.v1.CompareHouseholdOptionsResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.NormalizeConsentRecordRequest;
import com.acme.customers.v1.NormalizeConsentRecordResponse;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalRequest;
import com.acme.expenses.v1.DelegateExpensePolicyApprovalResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyApprovalServiceGrpc;
import com.acme.expenses.v1.RecallExpensePolicyRequest;
import com.acme.expenses.v1.RecallExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardCalculationServiceGrpc;
import com.acme.giftcards.v1.GiftCardLimit;
import com.acme.giftcards.v1.SimulateGiftCardRequest;
import com.acme.giftcards.v1.SimulateGiftCardResponse;
import com.acme.identity.v1.UserRef;
import com.acme.travel.v1.CompareTripOptionsRequest;
import com.acme.travel.v1.CompareTripOptionsResponse;
import com.acme.travel.v1.Trip;
import com.acme.travel.v1.TripCalculationServiceGrpc;
import com.acme.travel.v1.TripCardSettlement;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowRequest;
import com.acme.treasury.v1.AdvanceCashPositionWorkflowResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionApprovalServiceGrpc;
import com.acme.treasury.v1.DelegateCashPositionApprovalRequest;
import com.acme.treasury.v1.DelegateCashPositionApprovalResponse;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.GetCashPositionLifecycleRequest;
import com.acme.treasury.v1.GetCashPositionLifecycleResponse;
import com.acme.treasury.v1.InspectCashPositionWorkflowRequest;
import com.acme.treasury.v1.InspectCashPositionWorkflowResponse;
import com.acme.treasury.v1.ListCashPositionWorkflowsRequest;
import com.acme.treasury.v1.ListCashPositionWorkflowsResponse;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ResumeCashPositionWorkflowRequest;
import com.acme.treasury.v1.ResumeCashPositionWorkflowResponse;
import com.acme.treasury.v1.RetryCashPositionWorkflowRequest;
import com.acme.treasury.v1.RetryCashPositionWorkflowResponse;
import com.acme.treasury.v1.SignalCashPositionWorkflowRequest;
import com.acme.treasury.v1.SignalCashPositionWorkflowResponse;
import com.acme.treasury.v1.StartCashPositionWorkflowRequest;
import com.acme.treasury.v1.StartCashPositionWorkflowResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a cash position from start to finish. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class CashPositionWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionWorkflowApplication.class);

    private final MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking;

    private final VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync;

    private final ChannelRegistry channels;

    private final AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking;

    private final ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceBlockingStub expensesExpensePolicyApprovalServiceBlocking;

    private final GiftCardCalculationServiceGrpc.GiftCardCalculationServiceBlockingStub giftcardsGiftCardCalculationServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final TripCalculationServiceGrpc.TripCalculationServiceBlockingStub travelTripCalculationServiceBlocking;

    private final CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceFutureStub treasuryFxHedgeSyncServiceFuture;

    public CashPositionWorkflowApplication(
            MandateApprovalServiceGrpc.MandateApprovalServiceBlockingStub bankingMandateApprovalServiceBlocking,
            VarianceSyncServiceGrpc.VarianceSyncServiceStub budgetingVarianceSyncServiceAsync,
            ChannelRegistry channels,
            AttestationReportingServiceGrpc.AttestationReportingServiceBlockingStub complianceAttestationReportingServiceBlocking,
            ComplianceCaseServiceGrpc.ComplianceCaseServiceStub complianceComplianceCaseServiceAsync,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture,
            ApplicationEventPublisher events,
            ExpensePolicyApprovalServiceGrpc.ExpensePolicyApprovalServiceBlockingStub expensesExpensePolicyApprovalServiceBlocking,
            GiftCardCalculationServiceGrpc.GiftCardCalculationServiceBlockingStub giftcardsGiftCardCalculationServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TripCalculationServiceGrpc.TripCalculationServiceBlockingStub travelTripCalculationServiceBlocking,
            CashPositionApprovalServiceGrpc.CashPositionApprovalServiceBlockingStub treasuryCashPositionApprovalServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceFutureStub treasuryFxHedgeSyncServiceFuture) {
        this.bankingMandateApprovalServiceBlocking = bankingMandateApprovalServiceBlocking;
        this.budgetingVarianceSyncServiceAsync = budgetingVarianceSyncServiceAsync;
        this.channels = channels;
        this.complianceAttestationReportingServiceBlocking = complianceAttestationReportingServiceBlocking;
        this.complianceComplianceCaseServiceAsync = complianceComplianceCaseServiceAsync;
        this.customersConsentRecordValidationServiceFuture = customersConsentRecordValidationServiceFuture;
        this.events = events;
        this.expensesExpensePolicyApprovalServiceBlocking = expensesExpensePolicyApprovalServiceBlocking;
        this.giftcardsGiftCardCalculationServiceBlocking = giftcardsGiftCardCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.travelTripCalculationServiceBlocking = travelTripCalculationServiceBlocking;
        this.treasuryCashPositionApprovalServiceBlocking = treasuryCashPositionApprovalServiceBlocking;
        this.treasuryFxHedgeSyncServiceFuture = treasuryFxHedgeSyncServiceFuture;
    }

    public record ResumeCashPositionWorkflowStage0Requested(GetCashPositionLifecycleRequest request, CompletableFuture<GetCashPositionLifecycleResponse> completion, io.grpc.Context context) {}

    public record SignalCashPositionWorkflowStage1Requested(CompareHouseholdOptionsRequest request, CompletableFuture<CompareHouseholdOptionsResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a cash position.
     */
    public StartCashPositionWorkflowResponse startCashPositionWorkflow(StartCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = RecallExpensePolicyRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecallExpensePolicyRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> expensesExpensePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallExpensePolicy(value));

        var outbound1 = DelegateMandateApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBank(MandateBankSettlement.newBuilder()
                                .setBic(switch (request.getCashPosition().getFundingCase()) {
                                    case BANK -> request.getCashPosition().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = bankingMandateApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateMandateApproval(outbound1);

        var outbound2 = BatchCreateComplianceCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateComplianceCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getCashPosition().getCashPositionId())
                        .setSubmittedBy(request.getCashPosition().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateComplianceCasesResponse.getDefaultInstance());
        var input2 = complianceComplianceCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateComplianceCases(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamVarianceChangesRequest.newBuilder()
                .setChange(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .setEmail(request.getCashPosition().getContact().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamVarianceChangesResponse.getDefaultInstance());
        var input3 = budgetingVarianceSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamVarianceChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getHolder().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartCashPositionWorkflow", request.getCashPosition().getHolder().getFxHedgeId());
        return StartCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getMandate().getAudit().getCreatedBy())
                                .setEtag(reply1.getMandate().getAudit().getEtag())
                                .build())
                        .setAvailable(request.getCashPosition().getAvailable())
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setOutcome(StartCashPositionWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getOutcome().getMessage())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Advances a cash position.
     */
    public AdvanceCashPositionWorkflowResponse advanceCashPositionWorkflow(AdvanceCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = ListAssetTransferConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAssetTransferConflictsRequest.Filter.newBuilder()
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.AssetTransferSyncService");
        var selected0 = AssetTransferSyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.listAssetTransferConflicts(outbound0);

        var outbound1 = ListFxHedgeConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFxHedgeConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getCashPosition().getOpenedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .build();
        var pending1 = treasuryFxHedgeSyncServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listFxHedgeConflicts(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceCashPositionWorkflow", request.getCashPosition().getCashPositionId());
        return AdvanceCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(request.getCashPosition().getContact())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Retrys a cash position.
     */
    public RetryCashPositionWorkflowResponse retryCashPositionWorkflow(RetryCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = DelegateExpensePolicyApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(DelegateExpensePolicyApprovalRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getCashPosition().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> expensesExpensePolicyApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateExpensePolicyApproval(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryCashPositionWorkflow", request.getCashPosition().getCashPositionId());
        return RetryCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getExpensePolicy().getAudit().getCreatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setHolder(request.getCashPosition().getHolder())
                        .build())
                .build();
    }

    /**
     * Resumes a cash position.
     */
    public ResumeCashPositionWorkflowResponse resumeCashPositionWorkflow(ResumeCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = GetCashPositionLifecycleRequest.newBuilder()
                .setCashPositionId(request.getCashPosition().getCashPositionId())
                .setContext(request.getContext())
                .build();
        var completion0 = new CompletableFuture<GetCashPositionLifecycleResponse>();
        events.publishEvent(new ResumeCashPositionWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = DelegateCashPositionApprovalRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getCashPosition().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var command1 = new DispatchCommand<>("acme.treasury.v1.CashPositionApprovalService/DelegateCashPositionApproval", outbound1);
        var reply1 = treasuryCashPositionApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateCashPositionApproval(command1.payload());

        var outbound2 = CompareTripOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setTrip(Trip.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .setCard(TripCardSettlement.newBuilder()
                                .setNetwork(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = travelTripCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareTripOptions(outbound2);

        var outbound3 = NormalizeConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(HouseholdRef.newBuilder()
                                .setDisplayName(request.getCashPosition().getHolder().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeConsentRecordRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getCashPosition().getContact().getName())
                                .build())
                        .build())
                .build();
        var pending3 = customersConsentRecordValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeConsentRecord(outbound3);
        var reply3 = RpcSupport.await(pending3);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCurrencyCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeCashPositionWorkflow", request.getCashPosition().getCurrencyCode());
        return ResumeCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getCashPosition().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .setUpdatedAt(reply2.getTrip().getAudit().getUpdatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setCurrencyCode(reply2.getBreakdown().getCurrencyCode())
                        .setHolder(request.getCashPosition().getHolder())
                        .build())
                .setOutcome(ResumeCashPositionWorkflowResponse.Outcome.newBuilder()
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public InspectCashPositionWorkflowResponse inspectCashPositionWorkflow(InspectCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "InspectCashPositionWorkflow");
        return InspectCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .build();
    }

    /**
     * Lists cash positions a page at a time.
     */
    public ListCashPositionWorkflowsResponse listCashPositionWorkflows(ListCashPositionWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListCashPositionWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "ListCashPositionWorkflows");

        var outbound0 = RpcSupport.unpack(Any.pack(CountAttestationTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAttestationTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setQuery(request.getFilter().getQuery())
                        .build())
                .setOrderBy(request.getOrderBy())
                .build()), CountAttestationTotalsRequest.class);
        var reply0 = complianceAttestationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countAttestationTotals(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListCashPositionWorkflows", request.getPage().getPageToken());
        return ListCashPositionWorkflowsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Signals a cash position.
     */
    public SignalCashPositionWorkflowResponse signalCashPositionWorkflow(SignalCashPositionWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalCashPositionWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var futures0 = request.getCashPosition().getLimitsList().stream()
                .map(element -> SimulateGiftCardRequest.newBuilder()
                .setContext(request.getContext())
                .setGiftCard(GiftCard.newBuilder()
                        .addLimits(GiftCardLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setName(element.getName())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> giftcardsGiftCardCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateGiftCard(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SimulateGiftCardResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = CompareHouseholdOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .addChildren(Household.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setUpdatedAt(request.getCashPosition().getAudit().getUpdatedAt())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<CompareHouseholdOptionsResponse>();
        events.publishEvent(new SignalCashPositionWorkflowStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalCashPositionWorkflow", request.getCashPosition().getCashPositionId());
        return SignalCashPositionWorkflowResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setBalance(request.getCashPosition().getBalance())
                        .setCashPositionId(request.getCashPositionId())
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .build())
                .setOutcome(SignalCashPositionWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
