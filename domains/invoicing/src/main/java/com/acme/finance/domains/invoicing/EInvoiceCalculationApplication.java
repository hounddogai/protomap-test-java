package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.ContractClauseQueryServiceGrpc;
import com.acme.contracts.v1.FindContractClausesRequest;
import com.acme.contracts.v1.FindContractClausesResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CreateFeatureFlagRequest;
import com.acme.featureflags.v1.CreateFeatureFlagResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.CalculateEInvoiceRequest;
import com.acme.invoicing.v1.CalculateEInvoiceResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareEInvoiceOptionsRequest;
import com.acme.invoicing.v1.CompareEInvoiceOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceBatchOptionsResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EstimateEInvoiceRequest;
import com.acme.invoicing.v1.EstimateEInvoiceResponse;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationRequest;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationResponse;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceBatchDecisionResponse;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateCalculationResponse;
import com.acme.invoicing.v1.GetEInvoiceRatesRequest;
import com.acme.invoicing.v1.GetEInvoiceRatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchBankSettlement;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCardSettlement;
import com.acme.invoicing.v1.InvoiceBatchLine;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCardSettlement;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewInvoiceBatchRequest;
import com.acme.invoicing.v1.PreviewInvoiceBatchResponse;
import com.acme.invoicing.v1.QuoteEInvoiceRequest;
import com.acme.invoicing.v1.QuoteEInvoiceResponse;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.RecalculateEInvoicesRequest;
import com.acme.invoicing.v1.RecalculateEInvoicesResponse;
import com.acme.invoicing.v1.SimulateEInvoiceRequest;
import com.acme.invoicing.v1.SimulateEInvoiceResponse;
import com.acme.invoicing.v1.ValidateInvoiceBatchRequest;
import com.acme.invoicing.v1.ValidateInvoiceBatchResponse;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import com.acme.venues.v1.FindVenuesRequest;
import com.acme.venues.v1.FindVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Computes the figures of an e invoice, such as totals, taxes, and estimates. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class EInvoiceCalculationApplication {
    private static final Logger log = LoggerFactory.getLogger(EInvoiceCalculationApplication.class);

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final ChannelRegistry channels;

    private final ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking;

    private final InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking;

    private final InvoicingRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final Executor rpcExecutor;

    private final SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking;

    private final VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking;

    public EInvoiceCalculationApplication(
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            ChannelRegistry channels,
            ContractClauseQueryServiceGrpc.ContractClauseQueryServiceBlockingStub contractsContractClauseQueryServiceBlocking,
            ApplicationEventPublisher events,
            FeatureFlagServiceGrpc.FeatureFlagServiceBlockingStub featureflagsFeatureFlagServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking,
            InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            MetricSeriesServiceGrpc.MetricSeriesServiceBlockingStub metricsMetricSeriesServiceBlocking,
            InvoicingRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SearchIndexValidationServiceGrpc.SearchIndexValidationServiceBlockingStub searchSearchIndexValidationServiceBlocking,
            VenueQueryServiceGrpc.VenueQueryServiceBlockingStub venuesVenueQueryServiceBlocking) {
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.channels = channels;
        this.contractsContractClauseQueryServiceBlocking = contractsContractClauseQueryServiceBlocking;
        this.events = events;
        this.featureflagsFeatureFlagServiceBlocking = featureflagsFeatureFlagServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceBlocking = invoicingInvoiceBatchValidationServiceBlocking;
        this.invoicingInvoiceTemplateCalculationServiceBlocking = invoicingInvoiceTemplateCalculationServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.metricsMetricSeriesServiceBlocking = metricsMetricSeriesServiceBlocking;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.rpcExecutor = rpcExecutor;
        this.searchSearchIndexValidationServiceBlocking = searchSearchIndexValidationServiceBlocking;
        this.venuesVenueQueryServiceBlocking = venuesVenueQueryServiceBlocking;
    }

    public record EstimateEInvoiceStage0Requested(CheckInvoiceBatchEligibilityRequest request, CompletableFuture<CheckInvoiceBatchEligibilityResponse> completion, io.grpc.Context context) {}

    /**
     * Calculates an e invoice.
     */
    public CalculateEInvoiceResponse calculateEInvoice(CalculateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CalculateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = JsonCodec.roundTrip(PreviewInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .build())
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setAccountHolder(request.getEInvoice().getBank().getAccountHolder())
                                .setBic(request.getEInvoice().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), PreviewInvoiceBatchRequest.class);
        var reply0 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoiceBatch(outbound0);

        var outbound1 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input1 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();

        var outbound2 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getEInvoice().getAudit().getCreatedAt())
                                .setCreatedBy(request.getEInvoice().getAudit().getCreatedBy())
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .setUpdatedAt(request.getEInvoice().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
        var collector2 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input2 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector2);
        input2.onNext(outbound2);
        input2.onCompleted();
        var reply2 = collector2.await();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CalculateEInvoice", request.getEInvoice().getEInvoiceId());
        return CalculateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .setStatus(request.getEInvoice().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Estimates an e invoice.
     */
    public EstimateEInvoiceResponse estimateEInvoice(EstimateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "EstimateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = CheckInvoiceBatchEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getEInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CheckInvoiceBatchEligibilityResponse>();
        events.publishEvent(new EstimateEInvoiceStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = WatchMetricSeriesesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchMetricSeriesesRequest.Filter.newBuilder()
                        .setRegion(reply0.getInvoiceBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var stream1 = metricsMetricSeriesServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchMetricSerieses(outbound1);
        var reply1 = WatchMetricSeriesesResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }

        var outbound2 = FindVenuesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindVenuesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getInvoiceBatch().getCounterparty().getCustomerId())
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var reply2 = RpcSupport.call(outbound2, value -> venuesVenueQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findVenues(value));
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "EstimateEInvoice", request.getEInvoice().getEInvoiceId());
        return EstimateEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setTotal(request.getEInvoice().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Quotes an e invoice.
     */
    public QuoteEInvoiceResponse quoteEInvoice(QuoteEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "QuoteEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = PreviewInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setAccountHolder(request.getEInvoice().getBank().getAccountHolder())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(request.getEInvoice().getBillingAddress().getLinesList())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.invoicing.v1.InvoiceBatchValidationService");
        var selected0 = InvoiceBatchValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.previewInvoiceBatch(outbound0);

        var outbound1 = (ExplainInvoiceTemplateCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getDetails().getDestination().getRecipient())
                                .build())
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setNetwork(reply0.getInvoiceBatch().getCard().getNetwork())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply1 = invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceTemplateCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "QuoteEInvoice", request.getEInvoice().getEInvoiceId());
        return QuoteEInvoiceResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getInvoiceBatch().getBillingAddress().getCountryCode())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setIssuedAt(request.getEInvoice().getIssuedAt())
                        .putAllMetadata(reply1.getInvoiceTemplate().getMetadataMap())
                        .build())
                .build();
    }

    /**
     * Compares an e invoice.
     */
    public CompareEInvoiceOptionsResponse compareEInvoiceOptions(CompareEInvoiceOptionsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CompareEInvoiceOptions")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = ListFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFeatureFlagsRequest.Filter.newBuilder()
                        .setRegion(request.getDetails().getDestination().getRegion())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listFeatureFlags(outbound0), rpcExecutor);

        var outbound1 = CompareInvoiceTemplateOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceTemplateOptions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = NormalizeInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(request.getEInvoice().getBillingAddress().getPostalCode())
                                .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setLast4(switch (reply1.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> reply1.getInvoiceTemplate().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeInvoiceBatch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CompareEInvoiceOptions", request.getEInvoice().getEInvoiceId());
        return CompareEInvoiceOptionsResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply2.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply2.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(request.getEInvoice().getDueAt())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Recalculates an e invoice.
     */
    public RecalculateEInvoicesResponse recalculateEInvoices(RecalculateEInvoicesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RecalculateEInvoices")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = JsonCodec.roundTrip(CompareFiscalPeriodVersionsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CompareFiscalPeriodVersionsRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setCountryCode(request.getEInvoice().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build(), CompareFiscalPeriodVersionsRequest.class);
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareFiscalPeriodVersions(outbound0);

        var outbound1 = JsonCodec.roundTrip(CreateFeatureFlagRequest.newBuilder()
                .setContext(request.getContext())
                .setFeatureFlag(FeatureFlag.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEInvoice().getAudit().getEtag())
                                .setUpdatedAt(request.getEInvoice().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getFiscalPeriod().getAudit().getUpdatedBy())
                                .build())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getFiscalPeriod().getOrganizer().getDisplayName())
                                .build())
                        .build())
                .build(), CreateFeatureFlagRequest.class);
        var reply1 = featureflagsFeatureFlagServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createFeatureFlag(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RecalculateEInvoices", request.getEInvoice().getEInvoiceId());
        return RecalculateEInvoicesResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply1.getFeatureFlag().getAudit().getUpdatedAt())
                                .build())
                        .setBillingAddress(request.getEInvoice().getBillingAddress())
                        .setEInvoiceId(request.getEInvoiceId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Simulates an e invoice.
     */
    public SimulateEInvoiceResponse simulateEInvoice(SimulateEInvoiceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SimulateEInvoice")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = NormalizeInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getEInvoice().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(request.getEInvoice().getBillingAddress().getRecipient())
                                .build())
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setNetwork(request.getEInvoice().getCard().getNetwork())
                                .setToken(request.getEInvoice().getCard().getToken())
                                .build())
                        .build())
                .build();
        RpcPort<NormalizeInvoiceBatchRequest, NormalizeInvoiceBatchResponse> port0 = RpcPort.adapt(invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::normalizeInvoiceBatch);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = PreviewInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getInvoiceBatch().getAudit().getCreatedAt())
                                .build())
                        .setBank(InvoiceBatchBankSettlement.newBuilder()
                                .setIban(switch (reply0.getInvoiceBatch().getSettlementCase()) {
                                    case BANK -> reply0.getInvoiceBatch().getBank().getIban();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewInvoiceBatch(outbound1);

        var futures2 = request.getEInvoice().getLinesList().stream()
                .map(element -> FindContractClausesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindContractClausesRequest.Filter.newBuilder()
                        .setCreatedAfter(reply1.getInvoiceBatch().getAudit().getUpdatedAt())
                        .putAllLabels(element.getAttributesMap())
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> contractsContractClauseQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findContractClauses(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures2.toArray(CompletableFuture[]::new)).join();
        var results2 = futures2.stream().map(CompletableFuture::join).toList();
        var reply2 = results2.isEmpty() ? FindContractClausesResponse.getDefaultInstance()
                : results2.getLast();
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SimulateEInvoice", request.getEInvoice().getEInvoiceId());
        return SimulateEInvoiceResponse.newBuilder()
                .setBreakdown(SimulateEInvoiceResponse.Breakdown.newBuilder()
                        .setCurrencyCode(request.getDetails().getCurrencyCode())
                        .build())
                .setEInvoice(EInvoice.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getInvoiceBatch().getBillingAddress().getLinesList())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply0.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setIssuedAt(reply0.getInvoiceBatch().getIssuedAt())
                        .build())
                .build();
    }

    /**
     * Explains an e invoice.
     */
    public ExplainEInvoiceCalculationResponse explainEInvoiceCalculation(ExplainEInvoiceCalculationRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExplainEInvoiceCalculation")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveEInvoice(request.getContext().getTenantId(), request.getEInvoice());

        var outbound0 = JsonCodec.roundTrip(QuoteInvoiceTemplateRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(QuoteInvoiceTemplateRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(request.getEInvoice().getBillingAddress().getLocality())
                                .setPostalCode(request.getEInvoice().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setCard(InvoiceTemplateCardSettlement.newBuilder()
                                .setLast4(request.getEInvoice().getCard().getLast4())
                                .build())
                        .build())
                .build(), QuoteInvoiceTemplateRequest.class);
        var reply0 = invoicingInvoiceTemplateCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceTemplate(outbound0);

        var builder1 = ValidateSearchIndexRequest.newBuilder();
        builder1.setContext(request.getContext());
        builder1.setSearchIndex(SearchIndex.newBuilder()
                .setAddress(PostalAddress.newBuilder()
                        .setLocality(reply0.getInvoiceTemplate().getBillingAddress().getLocality())
                        .build())
                .setAudit(AuditInfo.newBuilder()
                        .setEtag(reply0.getInvoiceTemplate().getAudit().getEtag())
                        .build())
                .setOwner(UserRef.newBuilder()
                        .setDisplayName(request.getEInvoice().getCounterparty().getDisplayName())
                        .build())
                .build());
        var outbound1 = builder1.build();
        var reply1 = searchSearchIndexValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateSearchIndex(outbound1);

        var outbound2 = ExplainInvoiceBatchDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setCard(InvoiceBatchCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getInvoiceTemplate().getSettlementCase()) {
                                    case CARD -> reply0.getInvoiceTemplate().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply2 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainInvoiceBatchDecision(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getEInvoice().getEInvoiceId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExplainEInvoiceCalculation", request.getEInvoice().getEInvoiceId());
        return ExplainEInvoiceCalculationResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getSearchIndex().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(InvoiceBatchRef.newBuilder()
                                .setDisplayName(reply2.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .setDueAt(request.getEInvoice().getDueAt())
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(reply0.getInvoiceTemplate().getNotes())
                        .setNumber(reply2.getInvoiceBatch().getNumber())
                        .build())
                .build();
    }

    /**
     * Returns an e invoice.
     */
    public GetEInvoiceRatesResponse getEInvoiceRates(GetEInvoiceRatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetEInvoiceRates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "EInvoice", request.getEInvoiceId(), "GetEInvoiceRates");

        var outbound0 = CompareInvoiceBatchOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(InvoiceBatchLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<CompareInvoiceBatchOptionsRequest, CompareInvoiceBatchOptionsResponse>() {
            @Override
            public CompareInvoiceBatchOptionsResponse apply(CompareInvoiceBatchOptionsRequest input) {
                return invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareInvoiceBatchOptions(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ValidateInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(reply0.getInvoiceBatch().getBillingAddress().getLocality())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply0.getInvoiceBatch().getCounterparty().getCustomerId())
                                .build())
                        .build())
                .build();
        var reply1 = invoicingInvoiceBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateInvoiceBatch(outbound1);
        return GetEInvoiceRatesResponse.newBuilder()
                .setEInvoice(EInvoice.newBuilder()
                        .setEInvoiceId(request.getEInvoiceId())
                        .setNotes(reply0.getInvoiceBatch().getNotes())
                        .setTotal(reply0.getInvoiceBatch().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }
}
