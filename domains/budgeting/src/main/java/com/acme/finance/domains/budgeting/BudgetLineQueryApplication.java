package com.acme.finance.domains.budgeting;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.budgeting.v1.BrowseBudgetLinesRequest;
import com.acme.budgeting.v1.BrowseBudgetLinesResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CountBudgetLinesRequest;
import com.acme.budgeting.v1.CountBudgetLinesResponse;
import com.acme.budgeting.v1.CountBudgetsRequest;
import com.acme.budgeting.v1.CountBudgetsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastCalculationRequest;
import com.acme.budgeting.v1.ExplainForecastCalculationResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.ExportBudgetsRequest;
import com.acme.budgeting.v1.ExportBudgetsResponse;
import com.acme.budgeting.v1.FindBudgetLinesRequest;
import com.acme.budgeting.v1.FindBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastCalculationServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.LintBudgetsRequest;
import com.acme.budgeting.v1.LintBudgetsResponse;
import com.acme.budgeting.v1.LookupBudgetLineRequest;
import com.acme.budgeting.v1.LookupBudgetLineResponse;
import com.acme.budgeting.v1.PeekBudgetLineRequest;
import com.acme.budgeting.v1.PeekBudgetLineResponse;
import com.acme.budgeting.v1.PreviewBudgetRequest;
import com.acme.budgeting.v1.PreviewBudgetResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.RecalculateForecastsRequest;
import com.acme.budgeting.v1.RecalculateForecastsResponse;
import com.acme.budgeting.v1.ResolveBudgetLinesRequest;
import com.acme.budgeting.v1.ResolveBudgetLinesResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.budgeting.v1.SimulateVarianceRequest;
import com.acme.budgeting.v1.SimulateVarianceResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.SummarizeBudgetLinesRequest;
import com.acme.budgeting.v1.SummarizeBudgetLinesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceCalculationServiceGrpc;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.PreviewDocumentRequest;
import com.acme.documents.v1.PreviewDocumentResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ScorePostingBatchRequest;
import com.acme.ledger.v1.ScorePostingBatchResponse;
import com.acme.suppliers.v1.SupplierRef;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about budget lines, for screens and reports. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetLineQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineQueryApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking;

    private final BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking;

    private final ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking;

    private final VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync;

    private final DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final BudgetingRepository repository;

    public BudgetLineQueryApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetQueryServiceGrpc.BudgetQueryServiceBlockingStub budgetingBudgetQueryServiceBlocking,
            BudgetValidationServiceGrpc.BudgetValidationServiceBlockingStub budgetingBudgetValidationServiceBlocking,
            ForecastCalculationServiceGrpc.ForecastCalculationServiceBlockingStub budgetingForecastCalculationServiceBlocking,
            VarianceCalculationServiceGrpc.VarianceCalculationServiceBlockingStub budgetingVarianceCalculationServiceBlocking,
            ChannelRegistry channels,
            CollectionCaseServiceGrpc.CollectionCaseServiceStub collectionsCollectionCaseServiceAsync,
            DocumentValidationServiceGrpc.DocumentValidationServiceBlockingStub documentsDocumentValidationServiceBlocking,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            BudgetingRepository repository) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetQueryServiceBlocking = budgetingBudgetQueryServiceBlocking;
        this.budgetingBudgetValidationServiceBlocking = budgetingBudgetValidationServiceBlocking;
        this.budgetingForecastCalculationServiceBlocking = budgetingForecastCalculationServiceBlocking;
        this.budgetingVarianceCalculationServiceBlocking = budgetingVarianceCalculationServiceBlocking;
        this.channels = channels;
        this.collectionsCollectionCaseServiceAsync = collectionsCollectionCaseServiceAsync;
        this.documentsDocumentValidationServiceBlocking = documentsDocumentValidationServiceBlocking;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.repository = repository;
    }

    /**
     * Returns a budget line.
     */
    public LookupBudgetLineResponse lookupBudgetLine(LookupBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "LookupBudgetLine");

        var outbound0 = RecalculateBudgetLinesRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<RecalculateBudgetLinesRequest, RecalculateBudgetLinesResponse> port0 = RpcPort.adapt(budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::recalculateBudgetLines);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = BatchCreateCollectionCasesRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateCollectionCasesRequest.StreamHeader.newBuilder()
                        .setSource(request.getBudgetLineId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateCollectionCasesResponse.getDefaultInstance());
        var input1 = collectionsCollectionCaseServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateCollectionCases(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        return LookupBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .putAllMetadata(reply0.getBudgetLine().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public BrowseBudgetLinesResponse browseBudgetLines(BrowseBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "BrowseBudgetLines");

        var outbound0 = (PreviewDocumentRequest.newBuilder()
                .setContext(request.getContext())
                .setDocument(Document.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = documentsDocumentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDocument(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseBudgetLines", request.getPage().getPageToken());
        return BrowseBudgetLinesResponse.newBuilder()
                .addBudgetLines(BudgetLine.newBuilder()
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getDocument().getCounterparty().getDisplayName())
                                .build())
                        .setTotal(reply0.getDocument().getTotal())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public FindBudgetLinesResponse findBudgetLines(FindBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "FindBudgetLines");

        var outbound0 = ExplainForecastCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.ForecastCalculationService");
        var selected0 = ForecastCalculationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainForecastCalculation(outbound0);

        var outbound1 = PreviewBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getForecast().getAudit().getEtag())
                                .build())
                        .setBudgetId(reply0.getForecast().getCounterparty().getBudgetId())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setToken(switch (reply0.getForecast().getSettlementCase()) {
                                    case CARD -> reply0.getForecast().getCard().getToken();
                                    default -> "";
                                })
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(reply0.getForecast().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBudget(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindBudgetLines", request.getPage().getPageToken());
        return FindBudgetLinesResponse.newBuilder()
                .addBudgetLines(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getForecast().getAudit().getCreatedAt())
                                .setUpdatedAt(reply1.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Counts budget lines by status.
     */
    public CountBudgetLinesResponse countBudgetLines(CountBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "CountBudgetLines");

        var outbound0 = ExportBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBudgetsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .putAllLabels(request.getFilter().getLabelsMap())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var stream0 = budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBudgets(outbound0);
        var reply0 = ExportBudgetsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBudgetLines", request.getPage().getPageToken());
        return CountBudgetLinesResponse.getDefaultInstance();
    }

    /**
     * Returns several budget lines at once.
     */
    public ResolveBudgetLinesResponse resolveBudgetLines(ResolveBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "ResolveBudgetLines");

        var outbound0 = LintBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setExternalAccountId(request.getContext().getTenantId())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintBudgetsRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var adapter0 = new Function<LintBudgetsRequest, LintBudgetsResponse>() {
            @Override
            public LintBudgetsResponse apply(LintBudgetsRequest input) {
                return budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBudgets(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ScorePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setCard(PostingBatchCardSettlement.newBuilder()
                                .setLast4(reply0.getBudget().getCard().getLast4())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scorePostingBatch(value));
        return ResolveBudgetLinesResponse.newBuilder()
                .addBudgetLines(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getPostingBatch().getAudit().getCreatedAt())
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams every matching budget line.
     */
    public ExportBudgetLinesResponse exportBudgetLines(ExportBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "ExportBudgetLines");

        var outbound0 = BeanCopies.copy(SimulateVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SimulateVarianceRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build(), SimulateVarianceRequest.class);
        var reply0 = budgetingVarianceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateVariance(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportBudgetLines", request.getFilter().getQuery());
        return ExportBudgetLinesResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setBudgetLineId(reply0.getVariance().getCustomer().getBudgetLineId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes a budget line.
     */
    public SummarizeBudgetLinesResponse summarizeBudgetLines(SummarizeBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "SummarizeBudgetLines");

        var outbound0 = SimulateBudgetLineRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<SimulateBudgetLineRequest, SimulateBudgetLineResponse>() {
            @Override
            public SimulateBudgetLineResponse apply(SimulateBudgetLineRequest input) {
                return budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBudgetLine(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = LintBudgetsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setCard(BudgetCardFunding.newBuilder()
                                .setNetwork(switch (reply0.getBudgetLine().getSettlementCase()) {
                                    case CARD -> reply0.getBudgetLine().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintBudgets(outbound1);

        var outbound2 = EstimateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply0.getBudgetLine().getBillingAddress().getLinesList())
                                .build())
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setNetwork(reply1.getBudget().getCard().getNetwork())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateBudgetLineRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getBudgetLine().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var reply2 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudgetLine(outbound2);

        var outbound3 = QuoteBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setNetwork(switch (reply0.getBudgetLine().getSettlementCase()) {
                                    case CARD -> reply0.getBudgetLine().getCard().getNetwork();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply3 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudgetLine(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeBudgetLines", request.getDetails().getFormat());
        return SummarizeBudgetLinesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists budget lines a page at a time.
     */
    public SuggestBudgetLinesResponse suggestBudgetLines(SuggestBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getContext().getTraceId(), "SuggestBudgetLines");

        var outbound0 = JsonCodec.roundTrip(RecalculateForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), RecalculateForecastsRequest.class);
        var reply0 = budgetingForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateForecasts(outbound0);

        var outbound1 = CalculateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setIban(switch (reply0.getForecast().getSettlementCase()) {
                                    case BANK -> reply0.getForecast().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudgetLine(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestBudgetLines", request.getPage().getPageToken());
        return SuggestBudgetLinesResponse.newBuilder()
                .addBudgetLines(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getForecast().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getBudgetLine().getBillingAddress().getRegion())
                                .build())
                        .setIssuedAt(reply0.getForecast().getIssuedAt())
                        .setTotal(reply1.getBudgetLine().getTotal())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public PeekBudgetLineResponse peekBudgetLine(PeekBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PeekBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "PeekBudgetLine");

        var outbound0 = CountBudgetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBudgetsRequest.Filter.newBuilder()
                        .setOwnerId(request.getBudgetLineId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.budgeting.v1.BudgetQueryService/CountBudgets", outbound0);
        var reply0 = budgetingBudgetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countBudgets(command0.payload());
        return PeekBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
