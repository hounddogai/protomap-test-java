package com.acme.finance.domains.treasury;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.SyncRetentionPoliciesRequest;
import com.acme.audit.v1.SyncRetentionPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.lending.v1.CreateLoanApplicationRequest;
import com.acme.lending.v1.CreateLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationServiceGrpc;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ValidateRepaymentRequest;
import com.acme.lending.v1.ValidateRepaymentResponse;
import com.acme.performance.v1.BrowseReviewsRequest;
import com.acme.performance.v1.BrowseReviewsResponse;
import com.acme.performance.v1.ReviewQueryServiceGrpc;
import com.acme.recommendations.v1.RecommendationSet;
import com.acme.recommendations.v1.RecommendationSetCardSettlement;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.acme.recommendations.v1.UpdateRecommendationSetRequest;
import com.acme.recommendations.v1.UpdateRecommendationSetResponse;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionCalculationServiceGrpc;
import com.acme.treasury.v1.CashPositionLimit;
import com.acme.treasury.v1.CheckCashPositionEligibilityRequest;
import com.acme.treasury.v1.CheckCashPositionEligibilityResponse;
import com.acme.treasury.v1.CompareCashPositionOptionsRequest;
import com.acme.treasury.v1.CompareCashPositionOptionsResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.FxHedgeRef;
import com.acme.treasury.v1.GetCashPositionRatesRequest;
import com.acme.treasury.v1.GetCashPositionRatesResponse;
import com.acme.treasury.v1.GetCashPositionRulesRequest;
import com.acme.treasury.v1.GetCashPositionRulesResponse;
import com.acme.treasury.v1.LintCashPositionsRequest;
import com.acme.treasury.v1.LintCashPositionsResponse;
import com.acme.treasury.v1.PreviewCashPositionRequest;
import com.acme.treasury.v1.PreviewCashPositionResponse;
import com.acme.treasury.v1.QuoteCashPositionRequest;
import com.acme.treasury.v1.QuoteCashPositionResponse;
import com.acme.treasury.v1.ScoreCashPositionRequest;
import com.acme.treasury.v1.ScoreCashPositionResponse;
import com.acme.treasury.v1.ValidateCashPositionRequest;
import com.acme.treasury.v1.ValidateCashPositionResponse;
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
 * Checks cash positions against the rules before anything acts on them. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class CashPositionValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(CashPositionValidationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking;

    private final FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking;

    private final LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking;

    private final RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking;

    private final ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking;

    private final RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking;

    private final TreasuryRepository repository;

    private final Executor rpcExecutor;

    private final CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking;

    public CashPositionValidationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceBlockingStub auditRetentionPolicySyncServiceBlocking,
            FiscalPeriodExportServiceGrpc.FiscalPeriodExportServiceBlockingStub ledgerFiscalPeriodExportServiceBlocking,
            LoanApplicationServiceGrpc.LoanApplicationServiceBlockingStub lendingLoanApplicationServiceBlocking,
            RepaymentValidationServiceGrpc.RepaymentValidationServiceBlockingStub lendingRepaymentValidationServiceBlocking,
            ReviewQueryServiceGrpc.ReviewQueryServiceBlockingStub performanceReviewQueryServiceBlocking,
            RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking,
            TreasuryRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            CashPositionCalculationServiceGrpc.CashPositionCalculationServiceBlockingStub treasuryCashPositionCalculationServiceBlocking) {
        this.auditRetentionPolicySyncServiceBlocking = auditRetentionPolicySyncServiceBlocking;
        this.ledgerFiscalPeriodExportServiceBlocking = ledgerFiscalPeriodExportServiceBlocking;
        this.lendingLoanApplicationServiceBlocking = lendingLoanApplicationServiceBlocking;
        this.lendingRepaymentValidationServiceBlocking = lendingRepaymentValidationServiceBlocking;
        this.performanceReviewQueryServiceBlocking = performanceReviewQueryServiceBlocking;
        this.recommendationsRecommendationSetServiceBlocking = recommendationsRecommendationSetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.treasuryCashPositionCalculationServiceBlocking = treasuryCashPositionCalculationServiceBlocking;
    }

    /**
     * Validates a cash position.
     */
    public ValidateCashPositionResponse validateCashPosition(ValidateCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var results0 = new ArrayList<QuoteCashPositionResponse>();
        for (var element : request.getCashPosition().getLimitsList()) {
            QuoteCashPositionRequest itemRequest = QuoteCashPositionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .setPeriod(element.getPeriod())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
            results0.add(treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteCashPosition(itemRequest));
        }
        var reply0 = results0.isEmpty() ? QuoteCashPositionResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BeanCopies.copy(GetCashPositionRatesRequest.newBuilder()
                .setCashPositionId(request.getCashPosition().getCashPositionId())
                .setContext(request.getContext())
                .build(), GetCashPositionRatesRequest.class);
        var reply1 = treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCashPositionRates(outbound1);

        var outbound2 = ValidateRepaymentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateRepaymentRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getCashPosition().getContact().getName())
                                .build())
                        .build())
                .setRepayment(Repayment.newBuilder()
                        .addParticipants(RepaymentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply1.getCashPosition().getContact().getEmail())
                                        .setName(reply0.getCashPosition().getContact().getName())
                                        .setPreferredChannel(reply1.getCashPosition().getContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> lendingRepaymentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateRepayment(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getHolder().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateCashPosition", request.getCashPosition().getHolder().getFxHedgeId());
        return ValidateCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setBalance(request.getCashPosition().getBalance())
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Previews a cash position.
     */
    public PreviewCashPositionResponse previewCashPosition(PreviewCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = UpdateRecommendationSetRequest.newBuilder()
                .setContext(request.getContext())
                .setRecommendationSet(RecommendationSet.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getCashPosition().getAudit().getCreatedBy())
                                .setUpdatedBy(request.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setCard(RecommendationSetCardSettlement.newBuilder()
                                .setNetwork(switch (request.getCashPosition().getFundingCase()) {
                                    case CARD -> request.getCashPosition().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getCashPosition().getContact().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = recommendationsRecommendationSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateRecommendationSet(outbound0);

        var results1 = new ArrayList<SyncRetentionPoliciesResponse>();
        for (var element : request.getCashPosition().getLimitsList()) {
            SyncRetentionPoliciesRequest itemRequest = SyncRetentionPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SyncRetentionPoliciesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setName(element.getName())
                        .build())
                .build();
            results1.add(auditRetentionPolicySyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).syncRetentionPolicies(itemRequest));
        }
        var reply1 = results1.isEmpty() ? SyncRetentionPoliciesResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewCashPosition", request.getCashPosition().getCashPositionId());
        return PreviewCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRecommendationSet().getCounterparty().getEmail())
                                .build())
                        .setHolder(FxHedgeRef.newBuilder()
                                .setNumber(reply0.getRecommendationSet().getNumber())
                                .build())
                        .setStatus(request.getCashPosition().getStatus())
                        .build())
                .build();
    }

    /**
     * Checks a cash position.
     */
    public CheckCashPositionEligibilityResponse checkCashPositionEligibility(CheckCashPositionEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckCashPositionEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var futures0 = request.getCashPosition().getLimitsList().stream()
                .map(element -> CompareCashPositionOptionsRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .addLimits(CashPositionLimit.newBuilder()
                                .setAmount(element.getAmount())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCashPositionOptions(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CompareCashPositionOptionsResponse.getDefaultInstance()
                : results0.getLast();

        var outbound1 = BrowseReviewsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseReviewsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getCashPosition().getAudit().getUpdatedAt())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .setOwnerId(request.getCashPosition().getHolder().getFxHedgeId())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> performanceReviewQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseReviews(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckCashPositionEligibility", request.getCashPosition().getCashPositionId());
        return CheckCashPositionEligibilityResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .putAllLabels(request.getCashPosition().getLabelsMap())
                        .build())
                .setOutcome(CheckCashPositionEligibilityResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a cash position.
     */
    public ExplainCashPositionDecisionResponse explainCashPositionDecision(ExplainCashPositionDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainCashPositionDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = BeanCopies.copy(ScheduleFiscalPeriodExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleFiscalPeriodExportRequest.Details.newBuilder()
                        .putAllFilters(request.getCashPosition().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build(), ScheduleFiscalPeriodExportRequest.class);
        var reply0 = ledgerFiscalPeriodExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFiscalPeriodExport(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainCashPositionDecision", request.getCashPosition().getCashPositionId());
        return ExplainCashPositionDecisionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .setOpenedAt(request.getCashPosition().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Scores a cash position.
     */
    public ScoreCashPositionResponse scoreCashPosition(ScoreCashPositionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreCashPosition")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = CreateLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> lendingLoanApplicationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createLoanApplication(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getCashPositionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreCashPosition", request.getCashPosition().getCashPositionId());
        return ScoreCashPositionResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLoanApplication().getAudit().getUpdatedAt())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setHolder(request.getCashPosition().getHolder())
                        .build())
                .build();
    }

    /**
     * Lints a cash position.
     */
    public LintCashPositionsResponse lintCashPositions(LintCashPositionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LintCashPositions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveCashPosition(request.getContext().getTenantId(), request.getCashPosition());

        var outbound0 = GetCashPositionRatesRequest.newBuilder()
                .setCashPositionId(request.getCashPosition().getCashPositionId())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<GetCashPositionRatesRequest, GetCashPositionRatesResponse>() {
            @Override
            public GetCashPositionRatesResponse apply(GetCashPositionRatesRequest input) {
                return treasuryCashPositionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getCashPositionRates(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getCashPosition().getHolder().getFxHedgeId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "LintCashPositions", request.getCashPosition().getHolder().getFxHedgeId());
        return LintCashPositionsResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getCashPosition().getAudit().getUpdatedBy())
                                .build())
                        .setCashPositionId(request.getCashPositionId())
                        .setContact(request.getCashPosition().getContact())
                        .build())
                .build();
    }

    /**
     * Returns a cash position.
     */
    public GetCashPositionRulesResponse getCashPositionRules(GetCashPositionRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetCashPositionRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "CashPosition", request.getCashPositionId(), "GetCashPositionRules");
        return GetCashPositionRulesResponse.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setCashPositionId(request.getCashPositionId())
                        .build())
                .build();
    }
}
