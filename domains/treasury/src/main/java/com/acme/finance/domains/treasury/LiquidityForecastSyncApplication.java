package com.acme.finance.domains.treasury;

import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.CountAccessPoliciesRequest;
import com.acme.authorization.v1.CountAccessPoliciesResponse;
import com.acme.authorization.v1.SummarizeAccessPoliciesRequest;
import com.acme.authorization.v1.SummarizeAccessPoliciesResponse;
import com.acme.billing.v1.BatchCreateBillingAccountsRequest;
import com.acme.billing.v1.BatchCreateBillingAccountsResponse;
import com.acme.billing.v1.BillingAccountServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateCustomerSegmentsRequest;
import com.acme.customers.v1.RecalculateCustomerSegmentsResponse;
import com.acme.customers.v1.ScoreConsentRecordRequest;
import com.acme.customers.v1.ScoreConsentRecordResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyLifecycleServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryRequest;
import com.acme.expenses.v1.ScheduleExpensePolicyExpiryResponse;
import com.acme.expenses.v1.StreamExpensePolicyChangesRequest;
import com.acme.expenses.v1.StreamExpensePolicyChangesResponse;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.CommentLoanApprovalRequest;
import com.acme.lending.v1.CommentLoanApprovalResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferCardSettlement;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCardFunding;
import com.acme.treasury.v1.CashPositionLifecycleServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsRequest;
import com.acme.treasury.v1.CompareLiquidityForecastOptionsResponse;
import com.acme.treasury.v1.ExplainBankTransferCalculationRequest;
import com.acme.treasury.v1.ExplainBankTransferCalculationResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateRequest;
import com.acme.treasury.v1.GetLiquidityForecastSyncStateResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastImportServiceGrpc;
import com.acme.treasury.v1.LiquidityForecastLine;
import com.acme.treasury.v1.ListFxHedgeConflictsRequest;
import com.acme.treasury.v1.ListFxHedgeConflictsResponse;
import com.acme.treasury.v1.ListLiquidityForecastConflictsRequest;
import com.acme.treasury.v1.ListLiquidityForecastConflictsResponse;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsRequest;
import com.acme.treasury.v1.MapLiquidityForecastImportColumnsResponse;
import com.acme.treasury.v1.PullLiquidityForecastsRequest;
import com.acme.treasury.v1.PullLiquidityForecastsResponse;
import com.acme.treasury.v1.PushLiquidityForecastRequest;
import com.acme.treasury.v1.PushLiquidityForecastResponse;
import com.acme.treasury.v1.RecalculateFxHedgesRequest;
import com.acme.treasury.v1.RecalculateFxHedgesResponse;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictRequest;
import com.acme.treasury.v1.ResolveLiquidityForecastConflictResponse;
import com.acme.treasury.v1.StreamLiquidityForecastChangesRequest;
import com.acme.treasury.v1.StreamLiquidityForecastChangesResponse;
import com.acme.treasury.v1.SyncLiquidityForecastsRequest;
import com.acme.treasury.v1.SyncLiquidityForecastsResponse;
import com.acme.treasury.v1.TerminateCashPositionRequest;
import com.acme.treasury.v1.TerminateCashPositionResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Keeps liquidity forecasts in step with the systems of record in other regions. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class LiquidityForecastSyncApplication {
    private static final Logger log = LoggerFactory.getLogger(LiquidityForecastSyncApplication.class);

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking;

    private final BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync;

    private final ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking;

    private final CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking;

    private final PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking;

    private final ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceStub expensesExpensePolicySyncServiceAsync;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final LiquidityForecastSyncRequestMapper mapper;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceBlockingStub treasuryCashPositionLifecycleServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking;

    private final LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking;

    private final LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking;

    public LiquidityForecastSyncApplication(
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            AccessPolicyQueryServiceGrpc.AccessPolicyQueryServiceBlockingStub authorizationAccessPolicyQueryServiceBlocking,
            BillingAccountServiceGrpc.BillingAccountServiceStub billingBillingAccountServiceAsync,
            ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking,
            CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking,
            PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking,
            ApplicationEventPublisher events,
            ExpensePolicyLifecycleServiceGrpc.ExpensePolicyLifecycleServiceBlockingStub expensesExpensePolicyLifecycleServiceBlocking,
            ExpensePolicySyncServiceGrpc.ExpensePolicySyncServiceStub expensesExpensePolicySyncServiceAsync,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            LiquidityForecastSyncRequestMapper mapper,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            CashPositionLifecycleServiceGrpc.CashPositionLifecycleServiceBlockingStub treasuryCashPositionLifecycleServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking,
            LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking,
            LiquidityForecastImportServiceGrpc.LiquidityForecastImportServiceBlockingStub treasuryLiquidityForecastImportServiceBlocking) {
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.authorizationAccessPolicyQueryServiceBlocking = authorizationAccessPolicyQueryServiceBlocking;
        this.billingBillingAccountServiceAsync = billingBillingAccountServiceAsync;
        this.customersConsentRecordValidationServiceBlocking = customersConsentRecordValidationServiceBlocking;
        this.customersCustomerSegmentCalculationServiceBlocking = customersCustomerSegmentCalculationServiceBlocking;
        this.customersPreferenceCalculationServiceBlocking = customersPreferenceCalculationServiceBlocking;
        this.events = events;
        this.expensesExpensePolicyLifecycleServiceBlocking = expensesExpensePolicyLifecycleServiceBlocking;
        this.expensesExpensePolicySyncServiceAsync = expensesExpensePolicySyncServiceAsync;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryCashPositionLifecycleServiceBlocking = treasuryCashPositionLifecycleServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.treasuryFxHedgeSyncServiceBlocking = treasuryFxHedgeSyncServiceBlocking;
        this.treasuryFxHedgeValidationServiceBlocking = treasuryFxHedgeValidationServiceBlocking;
        this.treasuryLiquidityForecastCalculationServiceBlocking = treasuryLiquidityForecastCalculationServiceBlocking;
        this.treasuryLiquidityForecastImportServiceBlocking = treasuryLiquidityForecastImportServiceBlocking;
    }

    public record PullLiquidityForecastsStage2Requested(CountAccessPoliciesRequest request, CompletableFuture<CountAccessPoliciesResponse> completion, io.grpc.Context context) {}

    /**
     * Syncs a liquidity forecast.
     */
    public SyncLiquidityForecastsResponse syncLiquidityForecasts(SyncLiquidityForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SyncLiquidityForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        List<CommentLoanApprovalResponse> results0 = request.getLiquidityForecast().getLinesList().stream()
                .map(element -> CommentLoanApprovalRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(element.getTax().getAmount())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).commentLoanApproval(value)))
                .collect(Collectors.toList());
        var reply0 = results0.isEmpty() ? CommentLoanApprovalResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = mapper.syncLiquidityForecastsStage1(request, reply0);
        var reply1 = treasuryLiquidityForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareLiquidityForecastOptions(outbound1);

        var outbound2 = ScheduleExpensePolicyExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setExpensePolicy(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoan().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply1.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply2 = expensesExpensePolicyLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleExpensePolicyExpiry(outbound2);

        var outbound3 = TerminateCashPositionRequest.newBuilder()
                .setCashPositionId(request.getLiquidityForecast().getCounterparty().getCashPositionId())
                .setContext(request.getContext())
                .setDetails(TerminateCashPositionRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getLoan().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        Function<TerminateCashPositionRequest, TerminateCashPositionResponse> operation3 = treasuryCashPositionLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::terminateCashPosition;
        var reply3 = operation3.apply(outbound3);

        var outbound4 = BatchCreateBillingAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateBillingAccountsRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateBillingAccountsResponse.getDefaultInstance());
        var input4 = billingBillingAccountServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateBillingAccounts(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamExpensePolicyChangesRequest.newBuilder()
                .setChange(ExpensePolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setCreatedBy(request.getLiquidityForecast().getAudit().getCreatedBy())
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .setUpdatedAt(request.getLiquidityForecast().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamExpensePolicyChangesResponse.getDefaultInstance());
        var input5 = expensesExpensePolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamExpensePolicyChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SyncLiquidityForecasts", request.getLiquidityForecast().getLiquidityForecastId());
        return SyncLiquidityForecastsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getLiquidityForecast().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(request.getLiquidityForecast().getCounterparty())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .setOutcome(SyncLiquidityForecastsResponse.Outcome.newBuilder()
                        .setReferenceId(reply3.getOutcome().getReferenceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Pushs a liquidity forecast.
     */
    public PushLiquidityForecastResponse pushLiquidityForecast(PushLiquidityForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PushLiquidityForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = QuotePreferenceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuotePreferenceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getLiquidityForecast().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setPreference(Preference.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getLiquidityForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<QuotePreferenceRequest, QuotePreferenceResponse>() {
            @Override
            public QuotePreferenceResponse apply(QuotePreferenceRequest input) {
                return customersPreferenceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quotePreference(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ListFxHedgeConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFxHedgeConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPreference().getAudit().getUpdatedAt())
                        .putAllLabels(request.getLiquidityForecast().getMetadataMap())
                        .setOwnerId(request.getLiquidityForecast().getLiquidityForecastId())
                        .setRegion(request.getLiquidityForecast().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Stream.of(outbound1).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(treasuryFxHedgeSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFxHedgeConflicts(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound2 = mapper.pushLiquidityForecastStage2(request, reply0);
        var reply2 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCashPositions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PushLiquidityForecast", request.getLiquidityForecast().getNumber());
        return PushLiquidityForecastResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setOutcome(PushLiquidityForecastResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply2.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Pulls a liquidity forecast.
     */
    public PullLiquidityForecastsResponse pullLiquidityForecasts(PullLiquidityForecastsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PullLiquidityForecasts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = SummarizeAccessPoliciesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeAccessPoliciesRequest.Details.newBuilder()
                        .putAllFilters(request.getLiquidityForecast().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> authorizationAccessPolicyQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeAccessPolicies(outbound0), rpcExecutor);

        var outbound1 = RecalculateFxHedgesRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getLiquidityForecast().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFxHedges(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = CountAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountAccessPoliciesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getFxHedge().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(reply1.getResult().getCorrelationId())
                        .setRegion(reply1.getFxHedge().getBillingAddress().getRegion())
                        .build())
                .build();
        var completion2 = new CompletableFuture<CountAccessPoliciesResponse>();
        events.publishEvent(new PullLiquidityForecastsStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = LintCashPositionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .setCard(CashPositionCardFunding.newBuilder()
                                .setNetwork(switch (request.getLiquidityForecast().getSettlementCase()) {
                                    case CARD -> request.getLiquidityForecast().getCard().getNetwork();
                                    default -> "";
                                })
                                .setToken(switch (reply1.getFxHedge().getSettlementCase()) {
                                    case CARD -> reply1.getFxHedge().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintCashPositions(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PullLiquidityForecasts", request.getLiquidityForecast().getLiquidityForecastId());
        return PullLiquidityForecastsResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply3.getCashPosition().getAudit().getCreatedAt())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .putAllMetadata(reply1.getFxHedge().getMetadataMap())
                        .setNumber(request.getLiquidityForecast().getNumber())
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setOutcome(PullLiquidityForecastsResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Exchanges changes of liquidity forecasts with a peer.
     */
    public StreamLiquidityForecastChangesResponse streamLiquidityForecastChanges(StreamLiquidityForecastChangesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamLiquidityForecastChanges")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        if (request.hasChange()) {
            repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getChange());
        }

        var outbound0 = ScoreConsentRecordRequest.newBuilder()
                .setConsentRecord(ConsentRecord.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreConsentRecordRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(switch (request.getFrameCase()) {
                                    case CHANGE -> request.getChange().getBillingAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply0 = customersConsentRecordValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreConsentRecord(outbound0);

        var outbound1 = StreamAuditTrailExportRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(StreamAuditTrailExportRequest.Filter.newBuilder()
                        .setRegion(request.getChange().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamAuditTrailExport(outbound1);
        var reply1 = StreamAuditTrailExportResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getChange().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamLiquidityForecastChanges", request.getChange().getNumber());
        return StreamLiquidityForecastChangesResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getBreakdown().getCurrencyCode())
                                .build())
                        .setIssuedAt(request.getChange().getIssuedAt())
                        .setLiquidityForecastId(request.getChange().getLiquidityForecastId())
                        .build())
                .build();
    }

    /**
     * Resolves a liquidity forecast.
     */
    public ResolveLiquidityForecastConflictResponse resolveLiquidityForecastConflict(ResolveLiquidityForecastConflictRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveLiquidityForecastConflict")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveLiquidityForecast(request.getContext().getTenantId(), request.getLiquidityForecast());

        var outbound0 = ExplainBankTransferCalculationRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getLiquidityForecast().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getLiquidityForecast().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getLiquidityForecast().getBillingAddress().getLinesList())
                                .build())
                        .setCard(BankTransferCardSettlement.newBuilder()
                                .setNetwork(switch (request.getLiquidityForecast().getSettlementCase()) {
                                    case CARD -> request.getLiquidityForecast().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBankTransferCalculation(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ExplainFxHedgeDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getBankTransfer().getBillingAddress().getLinesList())
                                .setPostalCode(request.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .setRegion(reply0.getBankTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build()), ExplainFxHedgeDecisionRequest.class);
        var reply1 = treasuryFxHedgeValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFxHedgeDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getLiquidityForecast().getLiquidityForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResolveLiquidityForecastConflict", request.getLiquidityForecast().getLiquidityForecastId());
        return ResolveLiquidityForecastConflictResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(request.getLiquidityForecast().getCounterparty())
                        .setDueAt(reply0.getBankTransfer().getDueAt())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .setTotal(reply0.getBankTransfer().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a liquidity forecast.
     */
    public GetLiquidityForecastSyncStateResponse getLiquidityForecastSyncState(GetLiquidityForecastSyncStateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetLiquidityForecastSyncState")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getLiquidityForecastId(), "GetLiquidityForecastSyncState");

        var outbound0 = MapLiquidityForecastImportColumnsRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
        RpcPort<MapLiquidityForecastImportColumnsRequest, MapLiquidityForecastImportColumnsResponse> port0 = RpcPort.adapt(treasuryLiquidityForecastImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::mapLiquidityForecastImportColumns);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = mapper.getLiquidityForecastSyncStateStage1(request, reply0);
        var reply1 = customersCustomerSegmentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateCustomerSegments(outbound1);
        return GetLiquidityForecastSyncStateResponse.newBuilder()
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setCurrencyCode(reply0.getLiquidityForecast().getCounterparty().getCurrencyCode())
                                .build())
                        .addLines(LiquidityForecastLine.newBuilder()
                                .setDescription(reply1.getCustomerSegment().getDescription())
                                .build())
                        .setLiquidityForecastId(request.getLiquidityForecastId())
                        .build())
                .build();
    }

    /**
     * Lists liquidity forecasts a page at a time.
     */
    public ListLiquidityForecastConflictsResponse listLiquidityForecastConflicts(ListLiquidityForecastConflictsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListLiquidityForecastConflicts")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "LiquidityForecast", request.getContext().getTraceId(), "ListLiquidityForecastConflicts");

        var outbound0 = RpcSupport.unpack(Any.pack(CheckCashPositionEligibilityRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckCashPositionEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()), CheckCashPositionEligibilityRequest.class);
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkCashPositionEligibility(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListLiquidityForecastConflicts", request.getPage().getPageToken());
        return ListLiquidityForecastConflictsResponse.newBuilder()
                .addLiquidityForecasts(LiquidityForecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getCashPosition().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
