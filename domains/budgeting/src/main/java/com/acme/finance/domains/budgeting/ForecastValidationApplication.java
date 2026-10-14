package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateForecastRequest;
import com.acme.budgeting.v1.CalculateForecastResponse;
import com.acme.budgeting.v1.CheckForecastEligibilityRequest;
import com.acme.budgeting.v1.CheckForecastEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionRequest;
import com.acme.budgeting.v1.ExplainBudgetLineDecisionResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastCardSettlement;
import com.acme.budgeting.v1.GetForecastRatesRequest;
import com.acme.budgeting.v1.GetForecastRatesResponse;
import com.acme.budgeting.v1.NormalizeForecastRequest;
import com.acme.budgeting.v1.NormalizeForecastResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.PreviewForecastRequest;
import com.acme.budgeting.v1.PreviewForecastResponse;
import com.acme.budgeting.v1.QuoteForecastRequest;
import com.acme.budgeting.v1.QuoteForecastResponse;
import com.acme.budgeting.v1.ScoreForecastRequest;
import com.acme.budgeting.v1.ScoreForecastResponse;
import com.acme.budgeting.v1.ValidateForecastRequest;
import com.acme.budgeting.v1.ValidateForecastResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryLineTax;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.LintJournalEntriesRequest;
import com.acme.ledger.v1.LintJournalEntriesResponse;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.CheckLoanApplicationEligibilityRequest;
import com.acme.lending.v1.CheckLoanApplicationEligibilityResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
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
 * Checks forecasts against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class ForecastValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastValidationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking;

    private final LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final ForecastValidationRequestMapper mapper;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    public ForecastValidationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetLineValidationServiceGrpc.BudgetLineValidationServiceBlockingStub budgetingBudgetLineValidationServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            ChannelRegistry channels,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            JournalEntryValidationServiceGrpc.JournalEntryValidationServiceBlockingStub ledgerJournalEntryValidationServiceBlocking,
            LedgerAccountServiceGrpc.LedgerAccountServiceFutureStub ledgerLedgerAccountServiceFuture,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            ForecastValidationRequestMapper mapper,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetLineValidationServiceBlocking = budgetingBudgetLineValidationServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.channels = channels;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.ledgerJournalEntryValidationServiceBlocking = ledgerJournalEntryValidationServiceBlocking;
        this.ledgerLedgerAccountServiceFuture = ledgerLedgerAccountServiceFuture;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a forecast.
     */
    public ValidateForecastResponse validateForecast(ValidateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = QuoteForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteForecastRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<QuoteForecastRequest, QuoteForecastResponse>() {
            @Override
            public QuoteForecastResponse apply(QuoteForecastRequest input) {
                return budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteForecast(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input1 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                .setCreatedBy(request.getForecast().getAudit().getCreatedBy())
                                .setEtag(request.getForecast().getAudit().getEtag())
                                .setUpdatedAt(request.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateForecast", request.getForecastId());
        return ValidateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(reply0.getForecast().getForecastId())
                        .putAllMetadata(request.getForecast().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Previews a forecast.
     */
    public PreviewForecastResponse previewForecast(PreviewForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var futures0 = request.getForecast().getLinesList().stream()
                .map(element -> ExplainBudgetLineDecisionRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .addLines(BudgetLineLine.newBuilder()
                                .setAmount(element.getAmount())
                                .setTax(BudgetLineLineTax.newBuilder()
                                        .setAmount(element.getTax().getAmount())
                                        .addAllExemptions(element.getTax().getExemptionsList())
                                        .setRate(element.getTax().getRate())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingBudgetLineValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetLineDecision(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? ExplainBudgetLineDecisionResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewForecast", request.getForecast().getForecastId());
        return PreviewForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(request.getForecast().getBillingAddress())
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
    }

    /**
     * Checks a forecast.
     */
    public CheckForecastEligibilityResponse checkForecastEligibility(CheckForecastEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckForecastEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = StreamFiscalPeriodExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamFiscalPeriodExportRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getCreatedAt())
                        .putAllLabels(request.getForecast().getMetadataMap())
                        .setOwnerId(request.getForecast().getCounterparty().getBudgetId())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodExport(outbound0);
        var reply0 = StreamFiscalPeriodExportResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckForecastEligibility", request.getForecast().getForecastId());
        return CheckForecastEligibilityResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .setNotes(request.getForecast().getNotes())
                        .build())
                .build();
    }

    /**
     * Normalizes a forecast.
     */
    public NormalizeForecastResponse normalizeForecast(NormalizeForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = mapper.normalizeForecastStage0(request);
        var reply0 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateForecast(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeForecast", request.getForecast().getForecastId());
        return NormalizeForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(request.getForecastId())
                        .setNumber(reply0.getForecast().getNumber())
                        .build())
                .build();
    }

    /**
     * Explains a forecast.
     */
    public ExplainForecastDecisionResponse explainForecastDecision(ExplainForecastDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainForecastDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = ListLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getForecast().getAudit().getUpdatedAt())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = ledgerLedgerAccountServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listLedgerAccounts(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var futures1 = request.getForecast().getLinesList().stream()
                .map(element -> LintJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .setJournalEntry(JournalEntry.newBuilder()
                        .addLines(JournalEntryLine.newBuilder()
                                .setTax(JournalEntryLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .setJurisdiction(element.getTax().getJurisdiction())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> ledgerJournalEntryValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintJournalEntries(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? LintJournalEntriesResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = PreviewBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setHolderName(switch (reply1.getJournalEntry().getSettlementCase()) {
                                    case CARD -> reply1.getJournalEntry().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudget(outbound2);

        var outbound3 = GetForecastRatesRequest.newBuilder()
                .setContext(request.getContext())
                .setForecastId(request.getForecast().getForecastId())
                .build();
        var command3 = new DispatchCommand<>("acme.budgeting.v1.ForecastCalculationService/GetForecastRates", outbound3);
        var reply3 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getForecastRates(command3.payload());
        log.atDebug().addKeyValue("auditValue", request.getForecast().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainForecastDecision", request.getForecast().getNumber());
        return ExplainForecastDecisionResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(request.getForecast().getBillingAddress())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setBudgetId(reply3.getForecast().getCounterparty().getBudgetId())
                                .setDisplayName(reply2.getBudget().getHolder().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ExplainForecastDecisionResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Scores a forecast.
     */
    public ScoreForecastResponse scoreForecast(ScoreForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var outbound0 = RetryAuditTrailExportRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getForecast().getBillingAddress().getLocality())
                                .build())
                        .setCard(AuditTrailCardSettlement.newBuilder()
                                .setNetwork(switch (request.getForecast().getSettlementCase()) {
                                    case CARD -> request.getForecast().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).retryAuditTrailExport(outbound0);

        var outbound1 = CheckLoanApplicationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getForecast().getBillingAddress().getLinesList())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .setCard(LoanApplicationCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case CARD -> reply0.getAuditTrail().getCard().getHolderName();
                                    default -> "";
                                })
                                .setToken(switch (reply0.getAuditTrail().getSettlementCase()) {
                                    case CARD -> reply0.getAuditTrail().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkLoanApplicationEligibility(outbound1);

        var outbound2 = ListFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFixedAssetsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.assets.v1.FixedAssetService");
        var selected2 = FixedAssetServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.listFixedAssets(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreForecast", request.getForecast().getForecastId());
        return ScoreForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(request.getForecast().getBillingAddress())
                        .setForecastId(request.getForecastId())
                        .setNotes(reply1.getLoanApplication().getNotes())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
