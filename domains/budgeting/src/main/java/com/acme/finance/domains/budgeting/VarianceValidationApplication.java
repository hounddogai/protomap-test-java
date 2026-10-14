package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.RecordAuditEventVersionRequest;
import com.acme.audit.v1.RecordAuditEventVersionResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.CheckVarianceEligibilityRequest;
import com.acme.budgeting.v1.CheckVarianceEligibilityResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetVarianceRulesRequest;
import com.acme.budgeting.v1.GetVarianceRulesResponse;
import com.acme.budgeting.v1.PreviewBudgetLineRequest;
import com.acme.budgeting.v1.PreviewBudgetLineResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetsRequest;
import com.acme.budgeting.v1.RecalculateBudgetsResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.ValidateVarianceRequest;
import com.acme.budgeting.v1.ValidateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.datasets.v1.CreateDatasetRequest;
import com.acme.datasets.v1.CreateDatasetResponse;
import com.acme.datasets.v1.Dataset;
import com.acme.datasets.v1.DatasetComponent;
import com.acme.datasets.v1.DatasetServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ScoreCoverageRequest;
import com.acme.insurance.v1.ScoreCoverageResponse;
import com.acme.ledger.v1.JournalEntryHistoryServiceGrpc;
import com.acme.ledger.v1.ListJournalEntryVersionsRequest;
import com.acme.ledger.v1.ListJournalEntryVersionsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.NormalizeLoanApplicationRequest;
import com.acme.lending.v1.NormalizeLoanApplicationResponse;
import com.acme.recommendations.v1.BatchGetRecommendationSetsRequest;
import com.acme.recommendations.v1.BatchGetRecommendationSetsResponse;
import com.acme.recommendations.v1.RecommendationSetServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Checks variances against the rules before anything acts on them. Coordinates validation, durable audit writes,
 * and downstream operations.
 */
@Service
public class VarianceValidationApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceValidationApplication.class);

    private final AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final DatasetServiceGrpc.DatasetServiceBlockingStub datasetsDatasetServiceBlocking;

    private final ApplicationEventPublisher events;

    private final CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking;

    private final JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking;

    private final CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking;

    private final LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking;

    private final RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    public VarianceValidationApplication(
            AuditEventHistoryServiceGrpc.AuditEventHistoryServiceBlockingStub auditAuditEventHistoryServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            DatasetServiceGrpc.DatasetServiceBlockingStub datasetsDatasetServiceBlocking,
            ApplicationEventPublisher events,
            CoverageValidationServiceGrpc.CoverageValidationServiceBlockingStub insuranceCoverageValidationServiceBlocking,
            JournalEntryHistoryServiceGrpc.JournalEntryHistoryServiceBlockingStub ledgerJournalEntryHistoryServiceBlocking,
            CollateralValidationServiceGrpc.CollateralValidationServiceBlockingStub lendingCollateralValidationServiceBlocking,
            LoanApplicationValidationServiceGrpc.LoanApplicationValidationServiceBlockingStub lendingLoanApplicationValidationServiceBlocking,
            RecommendationSetServiceGrpc.RecommendationSetServiceBlockingStub recommendationsRecommendationSetServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.auditAuditEventHistoryServiceBlocking = auditAuditEventHistoryServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.datasetsDatasetServiceBlocking = datasetsDatasetServiceBlocking;
        this.events = events;
        this.insuranceCoverageValidationServiceBlocking = insuranceCoverageValidationServiceBlocking;
        this.ledgerJournalEntryHistoryServiceBlocking = ledgerJournalEntryHistoryServiceBlocking;
        this.lendingCollateralValidationServiceBlocking = lendingCollateralValidationServiceBlocking;
        this.lendingLoanApplicationValidationServiceBlocking = lendingLoanApplicationValidationServiceBlocking;
        this.recommendationsRecommendationSetServiceBlocking = recommendationsRecommendationSetServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
    }

    public record ExplainVarianceDecisionStage2Requested(PreviewBudgetLineRequest request, CompletableFuture<PreviewBudgetLineResponse> completion, io.grpc.Context context) {}

    /**
     * Validates a variance.
     */
    public ValidateVarianceResponse validateVariance(ValidateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ValidateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = QuoteBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(switch (request.getVariance().getOriginCase()) {
                                    case CUSTOMER -> request.getVariance().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudget(outbound0);

        var results1 = new ArrayList<ListJournalEntryVersionsResponse>();
        for (var element : request.getVariance().getHistoryList()) {
            ListJournalEntryVersionsRequest itemRequest = ListJournalEntryVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListJournalEntryVersionsRequest.Filter.newBuilder()
                        .setCreatedAfter(element.getChangedAt())
                        .putAllLabels(reply0.getBudget().getLabelsMap())
                        .build())
                .build();
            results1.add(ledgerJournalEntryHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listJournalEntryVersions(itemRequest));
        }
        var reply1 = results1.isEmpty() ? ListJournalEntryVersionsResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getContact())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input2 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();

        var outbound3 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getVariance().getAudit().getCreatedAt())
                                .setCreatedBy(request.getVariance().getAudit().getCreatedBy())
                                .setEtag(request.getVariance().getAudit().getEtag())
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input3 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ValidateVariance", request.getVariance().getVarianceId());
        return ValidateVarianceResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setSubject(request.getVariance().getSubject())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Previews a variance.
     */
    public PreviewVarianceResponse previewVariance(PreviewVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PreviewVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = BeanCopies.copy(BatchGetRecommendationSetsRequest.newBuilder()
                .setContext(request.getContext())
                .build(), BatchGetRecommendationSetsRequest.class);
        var reply0 = recommendationsRecommendationSetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetRecommendationSets(outbound0);

        var outbound1 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getVariance().getAudit().getUpdatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getVariance().getRequester().getPhone())
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(switch (request.getVariance().getOriginCase()) {
                                    case CUSTOMER -> request.getVariance().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetOptions(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PreviewVariance", request.getVariance().getVarianceId());
        return PreviewVarianceResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply1.getBudget().getContact().getPreferredChannel())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Checks a variance.
     */
    public CheckVarianceEligibilityResponse checkVarianceEligibility(CheckVarianceEligibilityRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CheckVarianceEligibility")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var results0 = new ArrayList<NormalizeLoanApplicationResponse>();
        for (var element : request.getVariance().getCommentsList()) {
            NormalizeLoanApplicationRequest itemRequest = NormalizeLoanApplicationRequest.newBuilder()
                .setContext(request.getContext())
                .setLoanApplication(LoanApplication.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .setUpdatedBy(request.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .build();
            results0.add(lendingLoanApplicationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLoanApplication(itemRequest));
        }
        var reply0 = results0.isEmpty() ? NormalizeLoanApplicationResponse.getDefaultInstance()
                : results0.getLast();

        List<QuoteBudgetResponse> results1 = request.getVariance().getCommentsList().stream()
                .map(element -> QuoteBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(element.getCreatedAt())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(element.getAuthorEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .flatMap(value -> Stream.of(budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudget(value)))
                .collect(Collectors.toList());
        var reply1 = results1.isEmpty() ? QuoteBudgetResponse.getDefaultInstance()
                : results1.getLast();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CheckVarianceEligibility", request.getVariance().getVarianceId());
        return CheckVarianceEligibilityResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Explains a variance.
     */
    public ExplainVarianceDecisionResponse explainVarianceDecision(ExplainVarianceDecisionRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainVarianceDecision")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = RecordAuditEventVersionRequest.newBuilder()
                .setAuditEvent(AuditEvent.newBuilder()
                        .setDetail(Any.pack(request.getContext()))
                        .setSubject(AuditTrailRef.newBuilder()
                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                .setNumber(request.getVariance().getCustomer().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecordAuditEventVersionRequest.Details.newBuilder()
                        .setAuthorEmail(request.getVariance().getRequester().getEmail())
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditEventHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordAuditEventVersion(outbound0), rpcExecutor);

        var outbound1 = CreateDatasetRequest.newBuilder()
                .setContext(request.getContext())
                .setDataset(Dataset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getVariance().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> datasetsDatasetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createDataset(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getVariance().getAudit().getUpdatedBy())
                                .build())
                        .addLines(BudgetLineLine.newBuilder()
                                .setQuantity(reply1.getDataset().getComponentsOrDefault("primary", DatasetComponent.getDefaultInstance()).getQuantity())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var completion2 = new CompletableFuture<PreviewBudgetLineResponse>();
        events.publishEvent(new ExplainVarianceDecisionStage2Requested(outbound2, completion2, io.grpc.Context.current()));
        var reply2 = completion2.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound3 = JsonCodec.roundTrip(RecalculateBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build(), RecalculateBudgetsRequest.class);
        var reply3 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateBudgets(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainVarianceDecision", request.getVariance().getVarianceId());
        return ExplainVarianceDecisionResponse.newBuilder()
                .setOutcome(ExplainVarianceDecisionResponse.Outcome.newBuilder()
                        .addAllNextSteps(reply2.getOutcome().getNextStepsList())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getAuditEvent().getCorrelationId())
                        .setStatus(reply3.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getAuditEvent().getSubject().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getDataset().getAudit().getUpdatedAt())
                                .build())
                        .setRequester(request.getVariance().getRequester())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Scores a variance.
     */
    public ScoreVarianceResponse scoreVariance(ScoreVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScoreVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = NormalizeCollateralRequest.newBuilder()
                .setCollateral(Collateral.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (request.getVariance().getOriginCase()) {
                                    case CUSTOMER -> request.getVariance().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeCollateralRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getVariance().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = lendingCollateralValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeCollateral(outbound0);

        var outbound1 = ScoreCoverageRequest.newBuilder()
                .setContext(request.getContext())
                .setCoverage(Coverage.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getCollateral().getOwner().getDisplayName())
                                .setEmail(request.getVariance().getRequester().getEmail())
                                .build())
                        .build())
                .setDetails(ScoreCoverageRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setLocality(switch (reply0.getCollateral().getLocatorCase()) {
                                    case ADDRESS -> reply0.getCollateral().getAddress().getLocality();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insuranceCoverageValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreCoverage(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getVariance().getVarianceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScoreVariance", request.getVariance().getVarianceId());
        return ScoreVarianceResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getCollateral().getOwner().getEmail())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply1.getCoverage().getAudit().getCreatedAt())
                                .build())
                        .setStatus(request.getVariance().getStatus())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceRulesResponse getVarianceRules(GetVarianceRulesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceRules")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceRules");
        return GetVarianceRulesResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }
}
