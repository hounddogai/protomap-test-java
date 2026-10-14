package com.acme.finance.domains.treasury;

import com.acme.banking.v1.ExplainStandingOrderDecisionRequest;
import com.acme.banking.v1.ExplainStandingOrderDecisionResponse;
import com.acme.banking.v1.PreviewStandingOrderRequest;
import com.acme.banking.v1.PreviewStandingOrderResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.StandingOrderValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.ValidateLoanRequest;
import com.acme.lending.v1.ValidateLoanResponse;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.PrescriptionLine;
import com.acme.prescriptions.v1.QuotePrescriptionRequest;
import com.acme.prescriptions.v1.QuotePrescriptionResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.BrowseCashPositionsRequest;
import com.acme.treasury.v1.BrowseCashPositionsResponse;
import com.acme.treasury.v1.CalculateCashPositionRequest;
import com.acme.treasury.v1.CalculateCashPositionResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CheckFxHedgeEligibilityRequest;
import com.acme.treasury.v1.CheckFxHedgeEligibilityResponse;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.ExportCashPositionsRequest;
import com.acme.treasury.v1.ExportCashPositionsResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.LintFxHedgesRequest;
import com.acme.treasury.v1.LintFxHedgesResponse;
import com.acme.treasury.v1.LookupCashPositionRequest;
import com.acme.treasury.v1.LookupCashPositionResponse;
import com.acme.treasury.v1.PeekCashPositionRequest;
import com.acme.treasury.v1.PeekCashPositionResponse;
import com.acme.treasury.v1.RecalculateBankTransfersRequest;
import com.acme.treasury.v1.RecalculateBankTransfersResponse;
import com.acme.treasury.v1.ResolveCashPositionsRequest;
import com.acme.treasury.v1.ResolveCashPositionsResponse;
import com.acme.treasury.v1.SuggestCashPositionsRequest;
import com.acme.treasury.v1.SuggestCashPositionsResponse;
import com.acme.treasury.v1.SummarizeCashPositionsRequest;
import com.acme.treasury.v1.SummarizeCashPositionsResponse;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about cash positions, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CashPositionQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionQueryApplication.class);

    private final StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final LoanValidationServiceGrpc.LoanValidationServiceFutureStub lendingLoanValidationServiceFuture;

    private final PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking;

    private final TreasuryRepository repository;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceFutureStub treasuryFxHedgeValidationServiceFuture;

    public CashPositionQueryApplication(
            StandingOrderValidationServiceGrpc.StandingOrderValidationServiceBlockingStub bankingStandingOrderValidationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            LoanValidationServiceGrpc.LoanValidationServiceFutureStub lendingLoanValidationServiceFuture,
            PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking,
            TreasuryRepository repository,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceFutureStub treasuryFxHedgeValidationServiceFuture) {
        this.bankingStandingOrderValidationServiceBlocking = bankingStandingOrderValidationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.lendingLoanValidationServiceFuture = lendingLoanValidationServiceFuture;
        this.prescriptionsPrescriptionCalculationServiceBlocking = prescriptionsPrescriptionCalculationServiceBlocking;
        this.repository = repository;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
        this.treasuryFxHedgeValidationServiceBlocking = treasuryFxHedgeValidationServiceBlocking;
        this.treasuryFxHedgeValidationServiceFuture = treasuryFxHedgeValidationServiceFuture;
    }

    /**
     * Returns a cash position.
     */
    public LookupCashPositionResponse lookupCashPosition(LookupCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "LookupCashPosition");
        return LookupCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .build();
    }

    /**
     * Lists cash positions a page at a time.
     */
    public BrowseCashPositionsResponse browseCashPositions(BrowseCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "BrowseCashPositions");

        var outbound0 = ValidateLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateLoanRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setLoan(Loan.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var pending0 = lendingLoanValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).validateLoan(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = QuoteBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getLoan().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply0.getLoan().getFundingCase()) {
                                    case BANK -> reply0.getLoan().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(reply0.getLoan().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudgetLine(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseCashPositions", request.getPage().getPageToken());
        return BrowseCashPositionsResponse.newBuilder()
                .addCashPositions(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLoan().getAudit().getCreatedAt())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setDisplayName(reply1.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts cash positions by status.
     */
    public CountCashPositionsResponse countCashPositions(CountCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "CountCashPositions");

        var outbound0 = QuotePrescriptionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePrescriptionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setPrescription(Prescription.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setCounterparty(PatientRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(PrescriptionLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .build();
        var reply0 = prescriptionsPrescriptionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePrescription(outbound0);

        var outbound1 = RecalculateBankTransfersRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getPrescription().getAudit().getUpdatedBy())
                                .build())
                        .setCard(BankTransferCardSettlement.newBuilder()
                                .setHolderName(reply0.getPrescription().getCard().getHolderName())
                                .setLast4(reply0.getPrescription().getCard().getLast4())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        Function<RecalculateBankTransfersRequest, RecalculateBankTransfersResponse> operation1 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateBankTransfers;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountCashPositions", request.getPage().getPageToken());
        return CountCashPositionsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Returns several cash positions at once.
     */
    public ResolveCashPositionsResponse resolveCashPositions(ResolveCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "ResolveCashPositions");

        var builder0 = PreviewStandingOrderRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setStandingOrder(StandingOrder.newBuilder()
                .setOrganizer(UserRef.newBuilder()
                        .setUserId(request.getContext().getActor().getUserId())
                        .build())
                .addParticipants(StandingOrderParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewStandingOrder(outbound0);
        return ResolveCashPositionsResponse.newBuilder()
                .addCashPositions(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getStandingOrder().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Streams every matching cash position.
     */
    public ExportCashPositionsResponse exportCashPositions(ExportCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "ExportCashPositions");

        var outbound0 = BeanCopies.copy(LintFxHedgesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintFxHedgesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), LintFxHedgesRequest.class);
        var reply0 = treasuryFxHedgeValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintFxHedges(outbound0);

        var outbound1 = CheckFxHedgeEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFxHedge().getAudit().getEtag())
                                .build())
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getFxHedge().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = treasuryFxHedgeValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkFxHedgeEligibility(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportCashPositions", request.getFilter().getQuery());
        return ExportCashPositionsResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getFxHedge().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .build();
    }

    /**
     * Summarizes a cash position.
     */
    public SummarizeCashPositionsResponse summarizeCashPositions(SummarizeCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "SummarizeCashPositions");

        var outbound0 = ExplainStandingOrderDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainStandingOrderDecision(outbound0);

        var outbound1 = JsonCodec.roundTrip(CalculateCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getStandingOrder().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CalculateCashPositionRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getStandingOrder().getLocation().getCountryCode())
                                .setLocality(reply0.getStandingOrder().getLocation().getLocality())
                                .setRegion(reply0.getStandingOrder().getLocation().getRegion())
                                .build())
                        .build())
                .build(), CalculateCashPositionRequest.class);
        var reply1 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateCashPosition(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeCashPositions", request.getCashPositionId());
        return SummarizeCashPositionsResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists cash positions a page at a time.
     */
    public SuggestCashPositionsResponse suggestCashPositions(SuggestCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getContext().getTraceId(), "SuggestCashPositions");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestCashPositions", request.getPage().getPageToken());
        return SuggestCashPositionsResponse.getDefaultInstance();
    }

    /**
     * Returns a cash position.
     */
    public PeekCashPositionResponse peekCashPosition(PeekCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "PeekCashPosition");
        return PeekCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .build();
    }
}
