package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CheckBudgetEligibilityRequest;
import com.acme.budgeting.v1.CheckBudgetEligibilityResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetRatesRequest;
import com.acme.budgeting.v1.GetBudgetRatesResponse;
import com.acme.budgeting.v1.GetBudgetRulesRequest;
import com.acme.budgeting.v1.GetBudgetRulesResponse;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.NormalizeBudgetRequest;
import com.acme.budgeting.v1.NormalizeBudgetResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateBudgetRequest;
import com.acme.budgeting.v1.SimulateBudgetResponse;
import com.acme.budgeting.v1.ValidateBudgetRequest;
import com.acme.budgeting.v1.ValidateBudgetResponse;
import com.acme.careplans.v1.BatchGetCarePlansRequest;
import com.acme.careplans.v1.BatchGetCarePlansResponse;
import com.acme.careplans.v1.CarePlanServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ListArticlesRequest;
import com.acme.content.v1.ListArticlesResponse;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionRequest;
import com.acme.featureflags.v1.ExplainFeatureFlagDecisionResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeRequest;
import com.acme.ledger.v1.SendFiscalPeriodTestNoticeResponse;
import com.acme.lending.v1.ExplainLoanCalculationRequest;
import com.acme.lending.v1.ExplainLoanCalculationResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCalculationServiceGrpc;
import com.acme.lending.v1.LoanLimit;
import com.acme.risk.v1.SimulateStressTestRequest;
import com.acme.risk.v1.SimulateStressTestResponse;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestCalculationServiceGrpc;
import com.acme.risk.v1.StressTestCardSettlement;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestLineTax;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Checks budgets against the rules before anything acts on them. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class BudgetValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetValidationApplication.class);

    private final FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final CarePlanServiceGrpc.CarePlanServiceBlockingStub careplansCarePlanServiceBlocking;

    private final ArticleServiceGrpc.ArticleServiceFutureStub contentArticleServiceFuture;

    private final FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking;

    private final BudgetingRepository repository;

    private final StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking;

    private final Executor rpcExecutor;

    public BudgetValidationApplication(
            FixedAssetServiceGrpc.FixedAssetServiceStub assetsFixedAssetServiceAsync,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            CarePlanServiceGrpc.CarePlanServiceBlockingStub careplansCarePlanServiceBlocking,
            ArticleServiceGrpc.ArticleServiceFutureStub contentArticleServiceFuture,
            FeatureFlagValidationServiceGrpc.FeatureFlagValidationServiceBlockingStub featureflagsFeatureFlagValidationServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            LoanCalculationServiceGrpc.LoanCalculationServiceBlockingStub lendingLoanCalculationServiceBlocking,
            BudgetingRepository repository,
            StressTestCalculationServiceGrpc.StressTestCalculationServiceBlockingStub riskStressTestCalculationServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.assetsFixedAssetServiceAsync = assetsFixedAssetServiceAsync;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.careplansCarePlanServiceBlocking = careplansCarePlanServiceBlocking;
        this.contentArticleServiceFuture = contentArticleServiceFuture;
        this.featureflagsFeatureFlagValidationServiceBlocking = featureflagsFeatureFlagValidationServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.lendingLoanCalculationServiceBlocking = lendingLoanCalculationServiceBlocking;
        this.repository = repository;
        this.riskStressTestCalculationServiceBlocking = riskStressTestCalculationServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    /**
     * Validates a budget.
     */
    public ValidateBudgetResponse validateBudget(ValidateBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = SendFiscalPeriodTestNoticeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SendFiscalPeriodTestNoticeRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).sendFiscalPeriodTestNotice(value));

        var outbound1 = ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ExplainBudgetCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(value)).orElseThrow();

        var outbound2 = BatchCreateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateFixedAssetsRequest.StreamHeader.newBuilder()
                        .setSource(request.getBudget().getBudgetId())
                        .setSubmittedBy(request.getBudget().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateFixedAssetsResponse.getDefaultInstance());
        var input2 = assetsFixedAssetServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateFixedAssets(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudget().getAudit().getCreatedAt())
                                .setCreatedBy(request.getBudget().getAudit().getCreatedBy())
                                .setEtag(request.getBudget().getAudit().getEtag())
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getHolder().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateBudget", request.getBudget().getHolder().getForecastId());
        return ValidateBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .putAllLabels(reply1.getBudget().getLabelsMap())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setOutcome(ValidateBudgetResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Previews a budget.
     */
    public PreviewBudgetResponse previewBudget(PreviewBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = RpcSupport.unpack(Any.pack(BatchGetCarePlansRequest.newBuilder()
                .setContext(request.getContext())
                .build()), BatchGetCarePlansRequest.class);
        var reply0 = careplansCarePlanServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetCarePlans(outbound0);

        var results1 = new ArrayList<QuoteBudgetLineResponse>();
        for (var element : request.getBudget().getLimitsList()) {
            QuoteBudgetLineRequest itemRequest = QuoteBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudget().getAudit().getCreatedAt())
                                .setUpdatedBy(request.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setToken(request.getBudget().getCard().getToken())
                                .build())
                        .addLines(BudgetLineLine.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results1.add(budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudgetLine(itemRequest));
        }
        var reply1 = results1.isEmpty() ? QuoteBudgetLineResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewBudget", request.getBudget().getBudgetId());
        return PreviewBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setStatus(request.getBudget().getStatus())
                        .build())
                .setOutcome(PreviewBudgetResponse.Outcome.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a budget.
     */
    public CheckBudgetEligibilityResponse checkBudgetEligibility(CheckBudgetEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckBudgetEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = (ExplainFeatureFlagDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainFeatureFlagDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = featureflagsFeatureFlagValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFeatureFlagDecision(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(CalculateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getFeatureFlag().getOwner().getDisplayName())
                                .build())
                        .addLines(BudgetLineLine.newBuilder()
                                .setTax(BudgetLineLineTax.newBuilder()
                                        .setAmount(reply0.getFeatureFlag().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()), CalculateBudgetLineRequest.class);
        var reply1 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudgetLine(outbound1);

        var outbound2 = GetBudgetRatesRequest.newBuilder()
                .setBudgetId(request.getBudget().getBudgetId())
                .setContext(request.getContext())
                .build();
        Function<GetBudgetRatesRequest, GetBudgetRatesResponse> operation2 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::getBudgetRates;
        var reply2 = operation2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getHolder().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckBudgetEligibility", request.getBudget().getHolder().getForecastId());
        return CheckBudgetEligibilityResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply2.getBudget().getHolder().getNumber())
                                .build())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setOutcome(CheckBudgetEligibilityResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply0.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Normalizes a budget.
     */
    public NormalizeBudgetResponse normalizeBudget(NormalizeBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "NormalizeBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var futures0 = request.getBudget().getLimitsList().stream()
                .map(element -> SimulateStressTestRequest.newBuilder()
                .setContext(request.getContext())
                .setStressTest(StressTest.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .setUpdatedBy(request.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setCard(StressTestCardSettlement.newBuilder()
                                .setLast4(request.getBudget().getCard().getLast4())
                                .build())
                        .addLines(StressTestLine.newBuilder()
                                .setTax(StressTestLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> riskStressTestCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateStressTest(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? SimulateStressTestResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = SimulateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getBudget().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudgetLine(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "NormalizeBudget", request.getBudget().getBudgetId());
        return NormalizeBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getBudgetLine().getAudit().getCreatedBy())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getBudgetLine().getCounterparty().getEmail())
                                .build())
                        .setHolder(request.getBudget().getHolder())
                        .build())
                .build();
    }

    /**
     * Scores a budget.
     */
    public ScoreBudgetResponse scoreBudget(ScoreBudgetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreBudget")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var results0 = new ArrayList<ExplainLoanCalculationResponse>();
        for (var element : request.getBudget().getLimitsList()) {
            ExplainLoanCalculationRequest itemRequest = ExplainLoanCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoan(Loan.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .addLimits(LoanLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setName(element.getName())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .build();
            results0.add(lendingLoanCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainLoanCalculation(itemRequest));
        }
        var reply0 = results0.isEmpty() ? ExplainLoanCalculationResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = ListArticlesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListArticlesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getLoan().getAudit().getCreatedAt())
                        .putAllLabels(reply0.getLoan().getLabelsMap())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending1 = contentArticleServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listArticles(outbound1);
        var reply1 = RpcSupport.await(pending1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreBudget", request.getBudget().getBudgetId());
        return ScoreBudgetResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lints a budget.
     */
    public LintBudgetsResponse lintBudgets(LintBudgetsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintBudgets")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = SimulateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setNetwork(request.getBudget().getCard().getNetwork())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudget(value));
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintBudgets", request.getBudget().getBudgetId());
        return LintBudgetsResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .build())
                        .setBudgetId(request.getBudgetId())
                        .setCurrencyCode(request.getBudget().getCurrencyCode())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public GetBudgetRulesResponse getBudgetRules(GetBudgetRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "GetBudgetRules");

        var outbound0 = ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setBudgetId(request.getBudgetId())
                .setContext(request.getContext())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(value));
        return GetBudgetRulesResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAvailable(reply0.getBudget().getAvailable())
                        .setBudgetId(request.getBudgetId())
                        .build())
                .build();
    }
}
