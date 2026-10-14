package com.acme.finance.domains.payroll;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.PullAuditTrailsRequest;
import com.acme.audit.v1.PullAuditTrailsResponse;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.RetryRetentionPolicyExportRequest;
import com.acme.audit.v1.RetryRetentionPolicyExportResponse;
import com.acme.banking.v1.CalculateMandateRequest;
import com.acme.banking.v1.CalculateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.SummarizeDemandForecastsRequest;
import com.acme.forecasting.v1.SummarizeDemandForecastsResponse;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.identity.v1.WatchUsersRequest;
import com.acme.identity.v1.WatchUsersResponse;
import com.acme.insurance.v1.CheckPolicyEligibilityRequest;
import com.acme.insurance.v1.CheckPolicyEligibilityResponse;
import com.acme.insurance.v1.ComparePolicyOptionsRequest;
import com.acme.insurance.v1.ComparePolicyOptionsResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyBankFunding;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyValidationServiceGrpc;
import com.acme.insurance.v1.RecalculatePoliciesRequest;
import com.acme.insurance.v1.RecalculatePoliciesResponse;
import com.acme.ledger.v1.ListPostingBatchVersionsRequest;
import com.acme.ledger.v1.ListPostingBatchVersionsResponse;
import com.acme.ledger.v1.PostingBatchHistoryServiceGrpc;
import com.acme.payroll.v1.CalculatePayRunRequest;
import com.acme.payroll.v1.CalculatePayRunResponse;
import com.acme.payroll.v1.ComparePayRunOptionsRequest;
import com.acme.payroll.v1.ComparePayRunOptionsResponse;
import com.acme.payroll.v1.EstimatePayRunRequest;
import com.acme.payroll.v1.EstimatePayRunResponse;
import com.acme.payroll.v1.ExplainPayRunCalculationRequest;
import com.acme.payroll.v1.ExplainPayRunCalculationResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.SimulatePayRunRequest;
import com.acme.payroll.v1.SimulatePayRunResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.QuoteRfqRequest;
import com.acme.procurement.v1.QuoteRfqResponse;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.SearchSearchIndexesRequest;
import com.acme.search.v1.SearchSearchIndexesResponse;
import com.acme.search.v1.SummarizeSearchIndexesRequest;
import com.acme.search.v1.SummarizeSearchIndexesResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of a pay run, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class PayRunCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(PayRunCalculationApplication.class);

    private final AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking;

    private final DemandForecastQueryServiceGrpc.DemandForecastQueryServiceFutureStub forecastingDemandForecastQueryServiceFuture;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking;

    private final PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking;

    private final PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking;

    private final GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking;

    private final RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking;

    private final PayrollRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking;

    public PayRunCalculationApplication(
            AuditTrailSyncServiceGrpc.AuditTrailSyncServiceBlockingStub auditAuditTrailSyncServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            MandateCalculationServiceGrpc.MandateCalculationServiceBlockingStub bankingMandateCalculationServiceBlocking,
            DemandForecastQueryServiceGrpc.DemandForecastQueryServiceFutureStub forecastingDemandForecastQueryServiceFuture,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PolicyCalculationServiceGrpc.PolicyCalculationServiceBlockingStub insurancePolicyCalculationServiceBlocking,
            PolicyValidationServiceGrpc.PolicyValidationServiceBlockingStub insurancePolicyValidationServiceBlocking,
            PostingBatchHistoryServiceGrpc.PostingBatchHistoryServiceBlockingStub ledgerPostingBatchHistoryServiceBlocking,
            GoodsReceiptValidationServiceGrpc.GoodsReceiptValidationServiceBlockingStub procurementGoodsReceiptValidationServiceBlocking,
            RfqCalculationServiceGrpc.RfqCalculationServiceBlockingStub procurementRfqCalculationServiceBlocking,
            PayrollRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            SearchIndexServiceGrpc.SearchIndexServiceBlockingStub searchSearchIndexServiceBlocking) {
        this.auditAuditTrailSyncServiceBlocking = auditAuditTrailSyncServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.bankingMandateCalculationServiceBlocking = bankingMandateCalculationServiceBlocking;
        this.forecastingDemandForecastQueryServiceFuture = forecastingDemandForecastQueryServiceFuture;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePolicyCalculationServiceBlocking = insurancePolicyCalculationServiceBlocking;
        this.insurancePolicyValidationServiceBlocking = insurancePolicyValidationServiceBlocking;
        this.ledgerPostingBatchHistoryServiceBlocking = ledgerPostingBatchHistoryServiceBlocking;
        this.procurementGoodsReceiptValidationServiceBlocking = procurementGoodsReceiptValidationServiceBlocking;
        this.procurementRfqCalculationServiceBlocking = procurementRfqCalculationServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.searchSearchIndexServiceBlocking = searchSearchIndexServiceBlocking;
    }

    /**
     * Calculates a pay run.
     */
    public CalculatePayRunResponse calculatePayRun(CalculatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = ListPostingBatchVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListPostingBatchVersionsRequest.Filter.newBuilder()
                        .setRegion(request.getPayRun().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerPostingBatchHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listPostingBatchVersions(outbound0), rpcExecutor);

        var outbound1 = RecalculatePoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculatePoliciesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePolicies(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewGoodsReceiptRequest.newBuilder()
                .setContext(request.getContext())
                .setGoodsReceipt(GoodsReceipt.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getPayRun().getAudit().getCreatedBy())
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getPayRun().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        final class Stage2Adapter {
            PreviewGoodsReceiptResponse fetch(PreviewGoodsReceiptRequest input) {
                return procurementGoodsReceiptValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewGoodsReceipt(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculatePayRun", request.getPayRun().getPayRunId());
        return CalculatePayRunResponse.newBuilder()
                .setBreakdown(CalculatePayRunResponse.Breakdown.newBuilder()
                        .setTotal(reply1.getBreakdown().getTotal())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setIssuedAt(request.getPayRun().getIssuedAt())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates a pay run.
     */
    public EstimatePayRunResponse estimatePayRun(EstimatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = PullAuditTrailsRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getPayRun().getAudit().getEtag())
                                .build())
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setAccountHolder(request.getPayRun().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAuditTrailsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAuditTrails(outbound0), rpcExecutor);

        var outbound1 = SearchSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchSearchIndexesRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> searchSearchIndexServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchSearchIndexes(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimatePayRun", request.getPayRun().getPayRunId());
        return EstimatePayRunResponse.newBuilder()
                .setBreakdown(EstimatePayRunResponse.Breakdown.newBuilder()
                        .setTotal(request.getPayRun().getTotal())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getAuditTrail().getBillingAddress().getCountryCode())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a pay run.
     */
    public QuotePayRunResponse quotePayRun(QuotePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuotePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = SummarizeDemandForecastsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeDemandForecastsRequest.Details.newBuilder()
                        .putAllFilters(request.getPayRun().getMetadataMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = forecastingDemandForecastQueryServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeDemandForecasts(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = SummarizeSearchIndexesRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSearchIndexesRequest.Details.newBuilder()
                        .putAllFilters(reply0.getResult().getDetailsMap())
                        .setPeriod(reply0.getPeriod())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply1 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSearchIndexes(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuotePayRun", request.getPayRun().getPayRunId());
        return QuotePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setBillingAddress(request.getPayRun().getBillingAddress())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Compares a pay run.
     */
    public ComparePayRunOptionsResponse comparePayRunOptions(ComparePayRunOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ComparePayRunOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = CalculateMandateRequest.newBuilder()
                .setContext(request.getContext())
                .setMandate(Mandate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .setRecipient(request.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(bankingMandateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateMandate(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = CheckPolicyEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getMandate().getAudit().getCreatedAt())
                                .setCreatedBy(reply0.getMandate().getAudit().getCreatedBy())
                                .build())
                        .setCard(PolicyCardFunding.newBuilder()
                                .setLast4(switch (request.getPayRun().getSettlementCase()) {
                                    case CARD -> request.getPayRun().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(reply0.getMandate().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = insurancePolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkPolicyEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ComparePayRunOptions", request.getPayRun().getPayRunId());
        return ComparePayRunOptionsResponse.newBuilder()
                .setBreakdown(ComparePayRunOptionsResponse.Breakdown.newBuilder()
                        .setTotal(reply0.getMandate().getTotal())
                        .build())
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPolicy().getAudit().getEtag())
                                .build())
                        .setPayRunId(request.getPayRunId())
                        .setStatus(request.getPayRun().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a pay run.
     */
    public SimulatePayRunResponse simulatePayRun(SimulatePayRunRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulatePayRun")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = RetryRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setRetentionPolicy(RetentionPolicy.newBuilder()
                        .setVersion(request.getPayRun().getCounterparty().getVersion())
                        .build())
                .build();
        RpcPort<RetryRetentionPolicyExportRequest, RetryRetentionPolicyExportResponse> port0 = RpcPort.adapt(auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::retryRetentionPolicyExport);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = ResolveAuditTrailConflictRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getPayRun().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ResolveAuditTrailConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> auditAuditTrailSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveAuditTrailConflict(value));

        var outbound2 = (QuoteRfqRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteRfqRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getAuditTrail().getBillingAddress().getCountryCode())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = procurementRfqCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteRfq(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulatePayRun", request.getPayRun().getPayRunId());
        return SimulatePayRunResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getRetentionPolicy().getAudit().getEtag())
                                .setUpdatedBy(reply1.getAuditTrail().getAudit().getUpdatedBy())
                                .build())
                        .setDueAt(reply2.getRfq().getSla().getDueAt())
                        .setPayRunId(request.getPayRunId())
                        .setTotal(request.getPayRun().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Explains a pay run.
     */
    public ExplainPayRunCalculationResponse explainPayRunCalculation(ExplainPayRunCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainPayRunCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.savePayRun(request.getContext().getTenantId(), request.getPayRun());

        var outbound0 = JsonCodec.roundTrip(ComparePolicyOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPolicy(Policy.newBuilder()
                        .setBank(PolicyBankFunding.newBuilder()
                                .setBic(request.getPayRun().getBank().getBic())
                                .build())
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getContact().getName())
                                .build())
                        .build())
                .build(), ComparePolicyOptionsRequest.class);
        var reply0 = insurancePolicyCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).comparePolicyOptions(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPayRun().getPayRunId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainPayRunCalculation", request.getPayRun().getPayRunId());
        return ExplainPayRunCalculationResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setCounterparty(request.getPayRun().getCounterparty())
                        .setPayRunId(request.getPayRunId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a pay run.
     */
    public GetPayRunRatesResponse getPayRunRates(GetPayRunRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetPayRunRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "PayRun", request.getPayRunId(), "GetPayRunRates");

        var outbound0 = WatchUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .setOwnerId(request.getPayRunId())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchUsers(outbound0);
        var reply0 = WatchUsersResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        return GetPayRunRatesResponse.newBuilder()
                .setPayRun(PayRun.newBuilder()
                        .setPayRunId(request.getPayRunId())
                        .build())
                .build();
    }
}
