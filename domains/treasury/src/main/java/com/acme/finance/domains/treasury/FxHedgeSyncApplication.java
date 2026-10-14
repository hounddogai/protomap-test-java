package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.ResolveAuditEventsRequest;
import com.acme.audit.v1.ResolveAuditEventsResponse;
import com.acme.collections.v1.ApprovePaymentPromiseRequest;
import com.acme.collections.v1.ApprovePaymentPromiseResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsRequest;
import com.acme.collections.v1.ListPaymentPromiseWorkflowsResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowRequest;
import com.acme.expenses.v1.AdvanceMileageClaimWorkflowResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimLine;
import com.acme.expenses.v1.MileageClaimLineTax;
import com.acme.expenses.v1.MileageClaimWorkflowServiceGrpc;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubWorkflowServiceGrpc;
import com.acme.payroll.v1.StartPayStubWorkflowRequest;
import com.acme.payroll.v1.StartPayStubWorkflowResponse;
import com.acme.scheduling.v1.BrowseShiftsRequest;
import com.acme.scheduling.v1.BrowseShiftsResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferBankSettlement;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionBankFunding;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CreateCashPositionRequest;
import com.acme.treasury.v1.CreateCashPositionResponse;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeLineTax;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetCashPositionRequest;
import com.acme.treasury.v1.GetCashPositionResponse;
import com.acme.treasury.v1.GetFxHedgeSyncStateRequest;
import com.acme.treasury.v1.GetFxHedgeSyncStateResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.PushFxHedgeRequest;
import com.acme.treasury.v1.PushFxHedgeResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.ReconcileFxHedgesRequest;
import com.acme.treasury.v1.ReconcileFxHedgesResponse;
import com.acme.treasury.v1.ResolveFxHedgeConflictRequest;
import com.acme.treasury.v1.ResolveFxHedgeConflictResponse;
import com.acme.treasury.v1.SimulateCashPositionRequest;
import com.acme.treasury.v1.SimulateCashPositionResponse;
import com.acme.treasury.v1.StreamFxHedgeChangesRequest;
import com.acme.treasury.v1.StreamFxHedgeChangesResponse;
import com.acme.treasury.v1.SyncFxHedgesRequest;
import com.acme.treasury.v1.SyncFxHedgesResponse;
import com.acme.treasury.v1.ValidateFxHedgeRequest;
import com.acme.treasury.v1.ValidateFxHedgeResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Keeps fx hedges in step with the systems of record in other regions. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class FxHedgeSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(FxHedgeSyncApplication.class);

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking;

    private final UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync;

    private final PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking;

    private final LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking;

    public FxHedgeSyncApplication(
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            PaymentPromiseApprovalServiceGrpc.PaymentPromiseApprovalServiceBlockingStub collectionsPaymentPromiseApprovalServiceBlocking,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            MileageClaimWorkflowServiceGrpc.MileageClaimWorkflowServiceBlockingStub expensesMileageClaimWorkflowServiceBlocking,
            UnderwritingSyncServiceGrpc.UnderwritingSyncServiceStub insuranceUnderwritingSyncServiceAsync,
            PayStubWorkflowServiceGrpc.PayStubWorkflowServiceBlockingStub payrollPayStubWorkflowServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            CashPositionServiceGrpc.CashPositionServiceBlockingStub treasuryCashPositionServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking,
            LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking) {
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.collectionsPaymentPromiseApprovalServiceBlocking = collectionsPaymentPromiseApprovalServiceBlocking;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.customersConsentRecordCalculationServiceBlocking = customersConsentRecordCalculationServiceBlocking;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.expensesMileageClaimWorkflowServiceBlocking = expensesMileageClaimWorkflowServiceBlocking;
        this.insuranceUnderwritingSyncServiceAsync = insuranceUnderwritingSyncServiceAsync;
        this.payrollPayStubWorkflowServiceBlocking = payrollPayStubWorkflowServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryCashPositionServiceBlocking = treasuryCashPositionServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.treasuryFxHedgeValidationServiceBlocking = treasuryFxHedgeValidationServiceBlocking;
        this.treasuryLiquidityForecastCalculationServiceBlocking = treasuryLiquidityForecastCalculationServiceBlocking;
    }

    /**
     * Syncs a fx hedge.
     */
    public SyncFxHedgesResponse syncFxHedges(SyncFxHedgesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncFxHedges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = GetCashPositionRequest.newBuilder()
                .setCashPositionId(request.getFxHedge().getCounterparty().getCashPositionId())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> treasuryCashPositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCashPosition(value));

        var results1 = new ArrayList<StartPayStubWorkflowResponse>();
        for (var element : request.getFxHedge().getAttachmentsList()) {
            StartPayStubWorkflowRequest itemRequest = StartPayStubWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .addAttachments(PayStubAttachment.newBuilder()
                                .setChecksum(element.getChecksum())
                                .build())
                        .build())
                .build();
            results1.add(payrollPayStubWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).startPayStubWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? StartPayStubWorkflowResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = LintCashPositionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setNetwork(reply0.getCashPosition().getCard().getNetwork())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<LintCashPositionsRequest, LintCashPositionsResponse> operation2 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::lintCashPositions;
        var reply2 = operation2.apply(outbound2);

        var outbound3 = RecalculateConsentRecordsRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(RecalculateConsentRecordsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getFxHedge().getBillingAddress().getLinesList())
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<RecalculateConsentRecordsRequest, RecalculateConsentRecordsResponse>() {
            @Override
            public RecalculateConsentRecordsResponse apply(RecalculateConsentRecordsRequest input) {
                return customersConsentRecordCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateConsentRecords(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);

        var outbound4 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input4 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamUnderwritingChangesRequest.newBuilder()
                .setChange(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .setEmail(request.getContext().getActor().getEmail())
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamUnderwritingChangesResponse.getDefaultInstance());
        var input5 = insuranceUnderwritingSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamUnderwritingChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncFxHedges", request.getFxHedge().getFxHedgeId());
        return SyncFxHedgesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(reply0.getCashPosition().getCashPositionId())
                                .setCurrencyCode(reply3.getBreakdown().getCurrencyCode())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNotes(request.getFxHedge().getNotes())
                        .setNumber(reply0.getCashPosition().getHolder().getNumber())
                        .build())
                .setOutcome(SyncFxHedgesResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Pushs a fx hedge.
     */
    public PushFxHedgeResponse pushFxHedge(PushFxHedgeRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushFxHedge")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ValidateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryFxHedgeValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateFxHedge(outbound0), rpcExecutor);

        var outbound1 = ResolveAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditEvents(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = SimulateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setBank(CashPositionBankFunding.newBuilder()
                                .setIban(switch (request.getFxHedge().getSettlementCase()) {
                                    case BANK -> request.getFxHedge().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getFxHedge().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                .setRecipient(reply0.getFxHedge().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCashPosition(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushFxHedge", request.getFxHedge().getFxHedgeId());
        return PushFxHedgeResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .setNotes(request.getFxHedge().getNotes())
                        .setNumber(reply2.getCashPosition().getHolder().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reconciles a fx hedge.
     */
    public ReconcileFxHedgesResponse reconcileFxHedges(ReconcileFxHedgesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReconcileFxHedges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = SimulateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .build())
                        .setCashPositionId(request.getFxHedge().getCounterparty().getCashPositionId())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getFxHedge().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<SimulateCashPositionRequest, SimulateCashPositionResponse>() {
            @Override
            public SimulateCashPositionResponse apply(SimulateCashPositionRequest input) {
                return treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateCashPosition(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var results1 = new ArrayList<AdvanceMileageClaimWorkflowResponse>();
        for (var element : request.getFxHedge().getLinesList()) {
            AdvanceMileageClaimWorkflowRequest itemRequest = AdvanceMileageClaimWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setMileageClaim(MileageClaim.newBuilder()
                        .addLines(MileageClaimLine.newBuilder()
                                .setTax(MileageClaimLineTax.newBuilder()
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .build();
            results1.add(expensesMileageClaimWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceMileageClaimWorkflow(itemRequest));
        }
        var reply1 = results1.isEmpty() ? AdvanceMileageClaimWorkflowResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReconcileFxHedges", request.getFxHedge().getFxHedgeId());
        return ReconcileFxHedgesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .putAllMetadata(request.getFxHedge().getMetadataMap())
                        .build())
                .setOutcome(ReconcileFxHedgesResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of fx hedges with a peer.
     */
    public StreamFxHedgeChangesResponse streamFxHedgeChanges(StreamFxHedgeChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamFxHedgeChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveFxHedge(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = BrowseShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseShiftsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getChange().getAudit().getCreatedAt())
                        .setOwnerId(switch (request.getFrameCase()) {
                            case CHANGE -> request.getChange().getCounterparty().getCashPositionId();
                            default -> "";
                        })
                        .setRegion(switch (request.getFrameCase()) {
                            case CHANGE -> request.getChange().getBillingAddress().getRegion();
                            default -> "";
                        })
                        .build())
                .build();
        var reply0 = schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseShifts(outbound0);

        var outbound1 = ExplainBankTransferCalculationRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBank(BankTransferBankSettlement.newBuilder()
                                .setIban(switch (request.getChange().getSettlementCase()) {
                                    case BANK -> request.getChange().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getBillingAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBankTransferCalculation(outbound1);

        var outbound2 = RecalculateLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateLiquidityForecastsRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getBillingAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getAudit().getCreatedBy();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = treasuryLiquidityForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLiquidityForecasts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getChange().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamFxHedgeChanges", request.getChange().getNumber());
        return StreamFxHedgeChangesResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getBankTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .setFxHedgeId(request.getChange().getFxHedgeId())
                        .putAllMetadata(reply2.getLiquidityForecast().getMetadataMap())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setSequence(request.getAck().getSequence())
                .build();
    }

    /**
     * Resolves a fx hedge.
     */
    public ResolveFxHedgeConflictResponse resolveFxHedgeConflict(ResolveFxHedgeConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveFxHedgeConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFxHedge(request.getContext().getTenantId(), request.getFxHedge());

        var outbound0 = ApprovePaymentPromiseRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getFxHedge().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ApprovePaymentPromiseRequest, ApprovePaymentPromiseResponse>() {
            @Override
            public ApprovePaymentPromiseResponse apply(ApprovePaymentPromiseRequest input) {
                return collectionsPaymentPromiseApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approvePaymentPromise(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ListPaymentPromiseWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPaymentPromiseWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPaymentPromise().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getResult().getDetailsMap())
                        .setOwnerId(request.getFxHedge().getCounterparty().getCashPositionId())
                        .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build()), ListPaymentPromiseWorkflowsRequest.class);
        var reply1 = collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPaymentPromiseWorkflows(outbound1);

        var outbound2 = CheckCustomerSegmentEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getPaymentPromise().getAudit().getUpdatedAt())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setDetails(CheckCustomerSegmentEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getPaymentPromise().getOrganizer().getEmail())
                                .build())
                        .build())
                .build();
        var command2 = new DispatchCommand<>("acme.customers.v1.CustomerSegmentValidationService/CheckCustomerSegmentEligibility", outbound2);
        var reply2 = customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCustomerSegmentEligibility(command2.payload());

        var outbound3 = EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFxHedgeRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(request.getFxHedge().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getFxHedge().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFxHedge().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var reply3 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFxHedge().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveFxHedgeConflict", request.getFxHedge().getFxHedgeId());
        return ResolveFxHedgeConflictResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply2.getCustomerSegment().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getPaymentPromise().getLocation().getLinesList())
                                .setPostalCode(reply0.getPaymentPromise().getLocation().getPostalCode())
                                .build())
                        .setFxHedgeId(request.getFxHedgeId())
                        .setIssuedAt(reply3.getFxHedge().getIssuedAt())
                        .addLines(FxHedgeLine.newBuilder()
                                .setTax(FxHedgeLineTax.newBuilder()
                                        .setAmount(reply2.getCustomerSegment().getDiscount().getAmount())
                                        .build())
                                .build())
                        .setStatus(request.getFxHedge().getStatus())
                        .build())
                .setOutcome(ResolveFxHedgeConflictResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns a fx hedge.
     */
    public GetFxHedgeSyncStateResponse getFxHedgeSyncState(GetFxHedgeSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFxHedgeSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getFxHedgeId(), "GetFxHedgeSyncState");
        return GetFxHedgeSyncStateResponse.newBuilder()
                .setFxHedge(FxHedge.newBuilder()
                        .setFxHedgeId(request.getFxHedgeId())
                        .build())
                .build();
    }

    /**
     * Lists fx hedges a page at a time.
     */
    public ListFxHedgeConflictsResponse listFxHedgeConflicts(ListFxHedgeConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListFxHedgeConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FxHedge", request.getContext().getTraceId(), "ListFxHedgeConflicts");

        var outbound0 = CreateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getFilter().getOwnerId())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setOpenedAt(request.getFilter().getCreatedAfter())
                        .build())
                .setContext(request.getContext())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.CashPositionService/CreateCashPosition", outbound0);
        var reply0 = treasuryCashPositionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createCashPosition(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListFxHedgeConflicts", request.getPage().getPageToken());
        return ListFxHedgeConflictsResponse.newBuilder()
                .addFxHedges(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCashPositionId(reply0.getCashPosition().getCashPositionId())
                                .build())
                        .build())
                .build();
    }
}
