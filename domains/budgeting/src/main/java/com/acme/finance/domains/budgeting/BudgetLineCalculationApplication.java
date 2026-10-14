package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.ResolveDepreciationsRequest;
import com.acme.assets.v1.ResolveDepreciationsResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.banking.v1.BankAccountQueryServiceGrpc;
import com.acme.banking.v1.CountBankAccountsRequest;
import com.acme.banking.v1.CountBankAccountsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetBankFunding;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.CalculateBudgetLineRequest;
import com.acme.budgeting.v1.CalculateBudgetLineResponse;
import com.acme.budgeting.v1.CalculateBudgetRequest;
import com.acme.budgeting.v1.CalculateBudgetResponse;
import com.acme.budgeting.v1.CompareBudgetLineOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetLineOptionsResponse;
import com.acme.budgeting.v1.CompareBudgetOptionsRequest;
import com.acme.budgeting.v1.CompareBudgetOptionsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExplainBudgetCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetCalculationResponse;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationRequest;
import com.acme.budgeting.v1.ExplainBudgetLineCalculationResponse;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.GetBudgetLineRatesRequest;
import com.acme.budgeting.v1.GetBudgetLineRatesResponse;
import com.acme.budgeting.v1.QuoteBudgetLineRequest;
import com.acme.budgeting.v1.QuoteBudgetLineResponse;
import com.acme.budgeting.v1.QuoteBudgetRequest;
import com.acme.budgeting.v1.QuoteBudgetResponse;
import com.acme.budgeting.v1.RecalculateBudgetLinesRequest;
import com.acme.budgeting.v1.RecalculateBudgetLinesResponse;
import com.acme.budgeting.v1.SimulateBudgetLineRequest;
import com.acme.budgeting.v1.SimulateBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsRequest;
import com.acme.ledger.v1.ListPostingBatchSubscriptionsResponse;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.recommendations.v1.RecommendationSetQueryServiceGrpc;
import com.acme.recommendations.v1.SummarizeRecommendationSetsRequest;
import com.acme.recommendations.v1.SummarizeRecommendationSetsResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import com.acme.suppliers.v1.SupplierRef;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Computes the figures of a budget line, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class BudgetLineCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetLineCalculationApplication.class);

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceFutureStub assetsDepreciationQueryServiceFuture;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final RecommendationSetQueryServiceGrpc.RecommendationSetQueryServiceBlockingStub recommendationsRecommendationSetQueryServiceBlocking;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    public BudgetLineCalculationApplication(
            DepreciationQueryServiceGrpc.DepreciationQueryServiceFutureStub assetsDepreciationQueryServiceFuture,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            BudgetCalculationServiceGrpc.BudgetCalculationServiceBlockingStub budgetingBudgetCalculationServiceBlocking,
            ChannelRegistry channels,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            RecommendationSetQueryServiceGrpc.RecommendationSetQueryServiceBlockingStub recommendationsRecommendationSetQueryServiceBlocking,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking) {
        this.assetsDepreciationQueryServiceFuture = assetsDepreciationQueryServiceFuture;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.budgetingBudgetCalculationServiceBlocking = budgetingBudgetCalculationServiceBlocking;
        this.channels = channels;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.recommendationsRecommendationSetQueryServiceBlocking = recommendationsRecommendationSetQueryServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
    }

    /**
     * Calculates a budget line.
     */
    public CalculateBudgetLineResponse calculateBudgetLine(CalculateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = (EstimateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBank(BudgetBankFunding.newBuilder()
                                .setAccountHolder(request.getBudgetLine().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudget(outbound0);

        var outbound1 = ExplainBudgetCalculationRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBank(BudgetBankFunding.newBuilder()
                                .setBic(switch (reply0.getBudget().getFundingCase()) {
                                    case BANK -> reply0.getBudget().getBank().getBic();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateBudgetLine", request.getBudgetLine().getBudgetLineId());
        return CalculateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(request.getBudgetLine().getBillingAddress())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setNumber(reply0.getBudget().getHolder().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Estimates a budget line.
     */
    public EstimateBudgetLineResponse estimateBudgetLine(EstimateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = CountBankAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBankAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudgetLine().getAudit().getUpdatedAt())
                        .putAllLabels(request.getBudgetLine().getMetadataMap())
                        .setOwnerId(request.getBudgetLine().getCounterparty().getSupplierId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.banking.v1.BankAccountQueryService");
        var selected0 = BankAccountQueryServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.countBankAccounts(outbound0);

        var outbound1 = BeanCopies.copy(CalculateBudgetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CalculateBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build(), CalculateBudgetRequest.class);
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudget(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateBudgetLine", request.getBudgetLine().getBudgetLineId());
        return EstimateBudgetLineResponse.newBuilder()
                .setBreakdown(EstimateBudgetLineResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply1.getBreakdown().getCurrencyCode())
                        .build())
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setStatus(request.getBudgetLine().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Quotes a budget line.
     */
    public QuoteBudgetLineResponse quoteBudgetLine(QuoteBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = BatchGetDocumentsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var channel0 = channels.lookup("acme.documents.v1.DocumentService");
        var serviceName0 = "acme.documents.v1" + "." + "DocumentService";
        var methodName0 = "Bat" + "chGetDocuments";
        var descriptor0 = MethodDescriptor.<BatchGetDocumentsRequest, BatchGetDocumentsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(BatchGetDocumentsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(BatchGetDocumentsResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = WatchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getBudgetLine().getCounterparty().getEmail())
                        .build())
                .build();
        var stream1 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFixedAssets(outbound1);
        var reply1 = WatchFixedAssetsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setHolder(ForecastRef.newBuilder()
                                .setDisplayName(request.getBudgetLine().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.budgeting.v1.BudgetCalculationService");
        var selected2 = BudgetCalculationServiceGrpc.newBlockingStub(channel2)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply2 = selected2.compareBudgetOptions(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteBudgetLine", request.getBudgetLine().getBudgetLineId());
        return QuoteBudgetLineResponse.newBuilder()
                .setBreakdown(QuoteBudgetLineResponse.Breakdown.newBuilder()
                        .setTotal(request.getBudgetLine().getTotal())
                        .build())
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setTotal(reply2.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Compares a budget line.
     */
    public CompareBudgetLineOptionsResponse compareBudgetLineOptions(CompareBudgetLineOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareBudgetLineOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = (EstimateBudgetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudget(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareBudgetLineOptions", request.getBudgetLine().getBudgetLineId());
        return CompareBudgetLineOptionsResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(request.getBudgetLine().getCounterparty())
                        .setNumber(reply0.getBudget().getHolder().getNumber())
                        .build())
                .build();
    }

    /**
     * Recalculates a budget line.
     */
    public RecalculateBudgetLinesResponse recalculateBudgetLines(RecalculateBudgetLinesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateBudgetLines")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = ResolveDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var pending0 = assetsDepreciationQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).resolveDepreciations(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = (SummarizeRecommendationSetsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeRecommendationSetsRequest.Details.newBuilder()
                        .putAllFilters(request.getBudgetLine().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build()).toBuilder().build();
        var reply1 = recommendationsRecommendationSetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeRecommendationSets(outbound1);

        var outbound2 = ListPostingBatchSubscriptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPostingBatchSubscriptionsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPostingBatchSubscriptions(value));

        var outbound3 = QuoteBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getBudgetLine().getBillingAddress().getCountryCode())
                                .setLocality(request.getBudgetLine().getBillingAddress().getLocality())
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<QuoteBudgetRequest, QuoteBudgetResponse>() {
            @Override
            public QuoteBudgetResponse apply(QuoteBudgetRequest input) {
                return budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudget(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateBudgetLines", request.getBudgetLine().getBudgetLineId());
        return RecalculateBudgetLinesResponse.newBuilder()
                .setBreakdown(RecalculateBudgetLinesResponse.Breakdown.newBuilder()
                        .setTotal(reply3.getBreakdown().getTotal())
                        .build())
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setTotal(request.getBudgetLine().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a budget line.
     */
    public SimulateBudgetLineResponse simulateBudgetLine(SimulateBudgetLineRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateBudgetLine")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = SummarizeSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSearchIndexes(outbound0), rpcExecutor);

        var outbound1 = QuoteBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getBudgetLine().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteBudgetRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setLocality(request.getBudgetLine().getBillingAddress().getLocality())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteBudget(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateBudgetLine", request.getBudgetLine().getBudgetLineId());
        return SimulateBudgetLineResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getBudget().getAudit().getEtag())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setDueAt(request.getBudgetLine().getDueAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Explains a budget line.
     */
    public ExplainBudgetLineCalculationResponse explainBudgetLineCalculation(ExplainBudgetLineCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainBudgetLineCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudgetLine(request.getContext().getTenantId(), request.getBudgetLine());

        var outbound0 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudgetLine().getAudit().getEtag())
                                .build())
                        .setCard(BudgetCardFunding.newBuilder()
                                .setHolderName(switch (request.getBudgetLine().getSettlementCase()) {
                                    case CARD -> request.getBudgetLine().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetOptions(outbound0);

        var outbound1 = ExplainBudgetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainBudgetCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainBudgetCalculation(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getBudgetLine().getBudgetLineId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainBudgetLineCalculation", request.getBudgetLine().getBudgetLineId());
        return ExplainBudgetLineCalculationResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getBudget().getAudit().getEtag())
                                .setUpdatedBy(reply0.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply0.getBudget().getHolder().getDisplayName())
                                .build())
                        .setStatus(request.getBudgetLine().getStatus())
                        .build())
                .setOutcome(ExplainBudgetLineCalculationResponse.Outcome.newBuilder()
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Returns a budget line.
     */
    public GetBudgetLineRatesResponse getBudgetLineRates(GetBudgetLineRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetBudgetLineRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BudgetLine", request.getBudgetLineId(), "GetBudgetLineRates");

        var outbound0 = BeanCopies.copy(CalculateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getBudgetLineId())
                        .putAllLabels(request.getContext().getBaggageMap())
                        .build())
                .setContext(request.getContext())
                .build(), CalculateBudgetRequest.class);
        var reply0 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateBudget(outbound0);

        var outbound1 = CompareBudgetOptionsRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBudget().getAudit().getEtag())
                                .setUpdatedAt(reply0.getBudget().getAudit().getUpdatedAt())
                                .build())
                        .setBank(BudgetBankFunding.newBuilder()
                                .setIban(switch (reply0.getBudget().getFundingCase()) {
                                    case BANK -> reply0.getBudget().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(reply0.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply1 = budgetingBudgetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareBudgetOptions(outbound1);
        return GetBudgetLineRatesResponse.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBudgetLineId(request.getBudgetLineId())
                        .setCounterparty(SupplierRef.newBuilder()
                                .setDisplayName(reply1.getBudget().getHolder().getDisplayName())
                                .build())
                        .setNumber(reply1.getBudget().getHolder().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }
}
