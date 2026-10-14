package com.acme.finance.domains.billing;

import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.billing.v1.BillingAccount;
import com.acme.billing.v1.BillingAccountBankFunding;
import com.acme.billing.v1.BillingAccountValidationServiceGrpc;
import com.acme.billing.v1.CalculateDunningRequest;
import com.acme.billing.v1.CalculateDunningResponse;
import com.acme.billing.v1.CalculatePaymentTermRequest;
import com.acme.billing.v1.CalculatePaymentTermResponse;
import com.acme.billing.v1.CompareDunningOptionsRequest;
import com.acme.billing.v1.CompareDunningOptionsResponse;
import com.acme.billing.v1.ComparePaymentTermOptionsRequest;
import com.acme.billing.v1.ComparePaymentTermOptionsResponse;
import com.acme.billing.v1.Dunning;
import com.acme.billing.v1.DunningComment;
import com.acme.billing.v1.DunningSla;
import com.acme.billing.v1.EstimateDunningRequest;
import com.acme.billing.v1.EstimateDunningResponse;
import com.acme.billing.v1.ExplainDunningCalculationRequest;
import com.acme.billing.v1.ExplainDunningCalculationResponse;
import com.acme.billing.v1.ExplainInvoiceDecisionRequest;
import com.acme.billing.v1.ExplainInvoiceDecisionResponse;
import com.acme.billing.v1.GetDunningRatesRequest;
import com.acme.billing.v1.GetDunningRatesResponse;
import com.acme.billing.v1.Invoice;
import com.acme.billing.v1.InvoiceCalculationServiceGrpc;
import com.acme.billing.v1.InvoiceRef;
import com.acme.billing.v1.InvoiceValidationServiceGrpc;
import com.acme.billing.v1.LintInvoicesRequest;
import com.acme.billing.v1.LintInvoicesResponse;
import com.acme.billing.v1.PaymentTerm;
import com.acme.billing.v1.PaymentTermCalculationServiceGrpc;
import com.acme.billing.v1.PreviewBillingAccountRequest;
import com.acme.billing.v1.PreviewBillingAccountResponse;
import com.acme.billing.v1.QuoteDunningRequest;
import com.acme.billing.v1.QuoteDunningResponse;
import com.acme.billing.v1.RecalculateDunningsRequest;
import com.acme.billing.v1.RecalculateDunningsResponse;
import com.acme.billing.v1.RecalculateInvoicesRequest;
import com.acme.billing.v1.RecalculateInvoicesResponse;
import com.acme.billing.v1.SimulateDunningRequest;
import com.acme.billing.v1.SimulateDunningResponse;
import com.acme.billing.v1.ValidateInvoiceRequest;
import com.acme.billing.v1.ValidateInvoiceResponse;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.ExplainVarianceDecisionRequest;
import com.acme.budgeting.v1.ExplainVarianceDecisionResponse;
import com.acme.budgeting.v1.PreviewVarianceRequest;
import com.acme.budgeting.v1.PreviewVarianceResponse;
import com.acme.budgeting.v1.SuggestBudgetLinesRequest;
import com.acme.budgeting.v1.SuggestBudgetLinesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSla;
import com.acme.budgeting.v1.VarianceStatusChange;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.collections.v1.ExplainWriteOffDecisionRequest;
import com.acme.collections.v1.ExplainWriteOffDecisionResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ScoreFeatureFlagRequest;
import com.acme.featureflags.v1.ScoreFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.metrics.v1.CountMetricSeriesesRequest;
import com.acme.metrics.v1.CountMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.payroll.v1.PayRunServiceGrpc;
import com.acme.payroll.v1.SearchPayRunsRequest;
import com.acme.payroll.v1.SearchPayRunsResponse;
import com.acme.search.v1.ExportSearchIndexesRequest;
import com.acme.search.v1.ExportSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.venues.v1.CloneVenueRequest;
import com.acme.venues.v1.CloneVenueResponse;
import com.acme.venues.v1.UpdateVenueRequest;
import com.acme.venues.v1.UpdateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.Optional;
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
 * Computes the figures of a dunning, such as totals, taxes, and estimates. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class DunningCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(DunningCalculationApplication.class);

    private final FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking;

    private final RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking;

    private final BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking;

    private final InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking;

    private final InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking;

    private final PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceFutureStub billingPaymentTermCalculationServiceFuture;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking;

    private final DunningCalculationRequestMapper mapper;

    private final MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking;

    private final PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking;

    private final BillingRepository repository;

    private final Executor rpcExecutor;

    private final SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking;

    private final VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking;

    public DunningCalculationApplication(
            FixedAssetQueryServiceGrpc.FixedAssetQueryServiceBlockingStub assetsFixedAssetQueryServiceBlocking,
            RetentionPolicyExportServiceGrpc.RetentionPolicyExportServiceBlockingStub auditRetentionPolicyExportServiceBlocking,
            BillingAccountValidationServiceGrpc.BillingAccountValidationServiceBlockingStub billingBillingAccountValidationServiceBlocking,
            InvoiceCalculationServiceGrpc.InvoiceCalculationServiceBlockingStub billingInvoiceCalculationServiceBlocking,
            InvoiceValidationServiceGrpc.InvoiceValidationServiceBlockingStub billingInvoiceValidationServiceBlocking,
            PaymentTermCalculationServiceGrpc.PaymentTermCalculationServiceFutureStub billingPaymentTermCalculationServiceFuture,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            WriteOffValidationServiceGrpc.WriteOffValidationServiceBlockingStub collectionsWriteOffValidationServiceBlocking,
            DunningCalculationRequestMapper mapper,
            MetricSeriesQueryServiceGrpc.MetricSeriesQueryServiceBlockingStub metricsMetricSeriesQueryServiceBlocking,
            PayRunServiceGrpc.PayRunServiceBlockingStub payrollPayRunServiceBlocking,
            BillingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexQueryServiceGrpc.SearchIndexQueryServiceBlockingStub searchSearchIndexQueryServiceBlocking,
            VenueServiceGrpc.VenueServiceBlockingStub venuesVenueServiceBlocking) {
        this.assetsFixedAssetQueryServiceBlocking = assetsFixedAssetQueryServiceBlocking;
        this.auditRetentionPolicyExportServiceBlocking = auditRetentionPolicyExportServiceBlocking;
        this.billingBillingAccountValidationServiceBlocking = billingBillingAccountValidationServiceBlocking;
        this.billingInvoiceCalculationServiceBlocking = billingInvoiceCalculationServiceBlocking;
        this.billingInvoiceValidationServiceBlocking = billingInvoiceValidationServiceBlocking;
        this.billingPaymentTermCalculationServiceFuture = billingPaymentTermCalculationServiceFuture;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.collectionsWriteOffValidationServiceBlocking = collectionsWriteOffValidationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesQueryServiceBlocking = metricsMetricSeriesQueryServiceBlocking;
        this.payrollPayRunServiceBlocking = payrollPayRunServiceBlocking;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexQueryServiceBlocking = searchSearchIndexQueryServiceBlocking;
        this.venuesVenueServiceBlocking = venuesVenueServiceBlocking;
    }

    /**
     * Calculates a dunning.
     */
    public CalculateDunningResponse calculateDunning(CalculateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = SummarizeFixedAssetsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsFixedAssetQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeFixedAssets(outbound0), rpcExecutor);

        var outbound1 = RecalculateInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecalculateInvoicesRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(request.getDetails().getDestination().getLinesList())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getDetails().getDestination().getRegion())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> billingInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateInvoices(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = JsonCodec.roundTrip(CountMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountMetricSeriesesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDunning().getAudit().getUpdatedAt())
                        .setRegion(reply1.getInvoice().getBillingAddress().getRegion())
                        .build())
                .build(), CountMetricSeriesesRequest.class);
        var reply2 = metricsMetricSeriesQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countMetricSerieses(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateDunning", request.getDunning().getDunningId());
        return CalculateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getInvoice().getCounterparty().getEmail())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setStatus(request.getDunning().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Estimates a dunning.
     */
    public EstimateDunningResponse estimateDunning(EstimateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = (SearchPayRunsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SearchPayRunsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDunning().getSla().getDueAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build()).toBuilder().build();
        var reply0 = payrollPayRunServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchPayRuns(outbound0);

        var outbound1 = ValidateInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDunning().getRequester().getPhone())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getDunning().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoice(value)).orElseThrow();

        var outbound2 = ExportSearchIndexesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportSearchIndexesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDunning().getAudit().getUpdatedAt())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var stream2 = searchSearchIndexQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportSearchIndexes(outbound2);
        var reply2 = ExportSearchIndexesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        List<PreviewVarianceResponse> results3 = request.getDunning().getHistoryList().stream()
                .map(element -> PreviewVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .addHistory(VarianceStatusChange.newBuilder()
                                .setChangedAt(element.getChangedAt())
                                .build())
                        .build())
                .build())
                .flatMap(value -> Stream.of(budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewVariance(value)))
                .collect(Collectors.toList());
        var reply3 = results3.isEmpty() ? PreviewVarianceResponse.getDefaultInstance()
                : results3.getLast();
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateDunning", request.getDunning().getDunningId());
        return EstimateDunningResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply1.getInvoice().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setSubject(request.getDunning().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes a dunning.
     */
    public QuoteDunningResponse quoteDunning(QuoteDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = ExplainVarianceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDunning().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(value)).orElseThrow();

        var outbound1 = ExplainInvoiceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainInvoiceDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getVariance().getRequester().getName())
                                .setPreferredChannel(request.getDunning().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDunning().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceDecision(outbound1);

        var outbound2 = PreviewBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDunning().getAudit().getCreatedAt())
                                .build())
                        .setBank(BillingAccountBankFunding.newBuilder()
                                .setBic(reply1.getInvoice().getBank().getBic())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setNumber(reply0.getVariance().getCustomer().getNumber())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel2 = channels.lookup("acme.billing.v1.BillingAccountValidationService");
        var serviceName2 = "acme.billing.v1" + "." + "BillingAccountValidationService";
        var methodName2 = "Pre" + "viewBillingAccount";
        var descriptor2 = MethodDescriptor.<PreviewBillingAccountRequest, PreviewBillingAccountResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewBillingAccountRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewBillingAccountResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);

        var outbound3 = ScoreFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScoreFeatureFlagRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(request.getDetails().getDestination().getCountryCode())
                                .addAllLines(reply1.getInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply2.getBillingAccount().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.featureflags.v1.FeatureFlagValidationService");
        var selected3 = FeatureFlagValidationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.scoreFeatureFlag(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteDunning", request.getDunning().getDunningId());
        return QuoteDunningResponse.newBuilder()
                .setBreakdown(QuoteDunningResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply3.getBreakdown().getCurrencyCode())
                        .addLines(QuoteDunningResponse.Breakdown.Line.newBuilder()
                                .setAmount(reply3.getFeatureFlag().getDiscount().getAmount())
                                .build())
                        .build())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getCustomer().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getInvoice().getAudit().getUpdatedAt())
                                .build())
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .setRequester(ContactInfo.newBuilder()
                                .setPreferredChannel(reply2.getBillingAccount().getContact().getPreferredChannel())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Compares a dunning.
     */
    public CompareDunningOptionsResponse compareDunningOptions(CompareDunningOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareDunningOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = mapper.compareDunningOptionsStage0(request);
        var reply0 = budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainVarianceDecision(outbound0);

        var outbound1 = CloneVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getVariance().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var reply1 = venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneVenue(outbound1);

        var outbound2 = PreviewBillingAccountRequest.newBuilder()
                .setBillingAccount(BillingAccount.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getVariance().getAudit().getCreatedBy())
                                .setUpdatedAt(reply1.getVenue().getAudit().getUpdatedAt())
                                .build())
                        .setHolder(InvoiceRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getAssignee().getDisplayName())
                                .setNumber(switch (reply0.getVariance().getOriginCase()) {
                                    case CUSTOMER -> reply0.getVariance().getCustomer().getNumber();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply2 = billingBillingAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewBillingAccount(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareDunningOptions", request.getDunning().getDunningId());
        return CompareDunningOptionsResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getVariance().getAssignee().getDisplayName())
                                .build())
                        .putAllCustomFields(request.getDunning().getCustomFieldsMap())
                        .setDunningId(request.getDunningId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Recalculates a dunning.
     */
    public RecalculateDunningsResponse recalculateDunnings(RecalculateDunningsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateDunnings")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = ExplainInvoiceDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDunning().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(request.getDetails().getDestination().getLocality())
                                .setPostalCode(request.getDetails().getDestination().getPostalCode())
                                .build())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceDecision(outbound0), rpcExecutor);

        var outbound1 = UpdateVenueRequest.newBuilder()
                .setContext(request.getContext())
                .setVenue(Venue.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getDunning().getAudit().getCreatedAt())
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> venuesVenueServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateVenue(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateDunnings", request.getDunning().getDunningId());
        return RecalculateDunningsResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getVenue().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setSubject(request.getDunning().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Simulates a dunning.
     */
    public SimulateDunningResponse simulateDunning(SimulateDunningRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateDunning")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = SuggestBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestBudgetLinesRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        final class Stage0Adapter {
            SuggestBudgetLinesResponse fetch(SuggestBudgetLinesRequest input) {
                return budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestBudgetLines(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = ShareRetentionPolicyExportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareRetentionPolicyExportRequest.Details.newBuilder()
                        .setChannel(switch (request.getDunning().getOriginCase()) {
                            case CHANNEL -> request.getDunning().getChannel().getChannel();
                            default -> "";
                        })
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDunning().getRequester().getEmail())
                                .setName(request.getDunning().getRequester().getName())
                                .build())
                        .build())
                .build();
        var reply1 = auditRetentionPolicyExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareRetentionPolicyExport(outbound1);

        var outbound2 = ComparePaymentTermOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getDunning().getAudit().getCreatedBy())
                                .build())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.billing.v1.PaymentTermCalculationService");
        var serviceName2 = "acme.billing.v1" + "." + "PaymentTermCalculationService";
        var methodName2 = "Com" + "parePaymentTermOptions";
        var descriptor2 = MethodDescriptor.<ComparePaymentTermOptionsRequest, ComparePaymentTermOptionsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ComparePaymentTermOptionsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ComparePaymentTermOptionsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateDunning", request.getDunning().getDunningId());
        return SimulateDunningResponse.newBuilder()
                .setBreakdown(SimulateDunningResponse.Breakdown.newBuilder()
                        .setCurrencyCode(reply2.getBreakdown().getCurrencyCode())
                        .build())
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getRetentionPolicy().getOwner().getEmail())
                                .build())
                        .addComments(DunningComment.newBuilder()
                                .setCreatedAt(reply1.getRetentionPolicy().getAudit().getCreatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setSubject(request.getDunning().getSubject())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Explains a dunning.
     */
    public ExplainDunningCalculationResponse explainDunningCalculation(ExplainDunningCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainDunningCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveDunning(request.getContext().getTenantId(), request.getDunning());

        var outbound0 = ValidateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateFeatureFlagRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDunning().getRequester().getPreferredChannel())
                                .build())
                        .build())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDunning().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.featureflags.v1.FeatureFlagValidationService");
        var serviceName0 = "acme.featureflags.v1" + "." + "FeatureFlagValidationService";
        var methodName0 = "Val" + "idateFeatureFlag";
        var descriptor0 = MethodDescriptor.<ValidateFeatureFlagRequest, ValidateFeatureFlagResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ValidateFeatureFlagRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ValidateFeatureFlagResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = ExplainWriteOffDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setWriteOff(WriteOff.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getDunning().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> collectionsWriteOffValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainWriteOffDecision(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDunning().getDunningId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainDunningCalculation", request.getDunning().getDunningId());
        return ExplainDunningCalculationResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply0.getFeatureFlag().getOwner().getDisplayName())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setPriority(reply1.getWriteOff().getCounterparty().getPriority())
                        .setRequester(request.getDunning().getRequester())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a dunning.
     */
    public GetDunningRatesResponse getDunningRates(GetDunningRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetDunningRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Dunning", request.getDunningId(), "GetDunningRates");

        var outbound0 = LintInvoicesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(LintInvoicesRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .setInvoice(Invoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> billingInvoiceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintInvoices(value)).orElseThrow();

        var outbound1 = CalculatePaymentTermRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentTerm(PaymentTerm.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoice().getAudit().getCreatedAt())
                                .setUpdatedBy(reply0.getInvoice().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var pending1 = billingPaymentTermCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).calculatePaymentTerm(outbound1);
        var reply1 = RpcSupport.await(pending1);
        return GetDunningRatesResponse.newBuilder()
                .setDunning(Dunning.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getPaymentTerm().getAudit().getUpdatedAt())
                                .build())
                        .setDunningId(request.getDunningId())
                        .setSla(DunningSla.newBuilder()
                                .setDueAt(reply0.getInvoice().getDueAt())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }
}
