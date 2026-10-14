package com.acme.finance.domains.invoicing;

import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesRequest;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CountInvoiceTemplatesRequest;
import com.acme.invoicing.v1.CountInvoiceTemplatesResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationRequest;
import com.acme.invoicing.v1.ExplainEInvoiceCalculationResponse;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.FindInvoiceTemplatesRequest;
import com.acme.invoicing.v1.FindInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateBankSettlement;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceTemplateRequest;
import com.acme.invoicing.v1.LookupInvoiceTemplateResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PreviewEInvoiceRequest;
import com.acme.invoicing.v1.PreviewEInvoiceResponse;
import com.acme.invoicing.v1.QuoteInvoiceBatchRequest;
import com.acme.invoicing.v1.QuoteInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SuggestInvoiceTemplatesResponse;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesRequest;
import com.acme.invoicing.v1.SummarizeInvoiceTemplatesResponse;
import com.acme.lineage.v1.BatchCreateLineageNodesRequest;
import com.acme.lineage.v1.BatchCreateLineageNodesResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.lineage.v1.LineageNodeServiceGrpc;
import com.acme.modelregistry.v1.EstimateRegisteredModelRequest;
import com.acme.modelregistry.v1.EstimateRegisteredModelResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.ratelimiting.v1.FindRateLimitsRequest;
import com.acme.ratelimiting.v1.FindRateLimitsResponse;
import com.acme.ratelimiting.v1.RateLimitQueryServiceGrpc;
import com.acme.risk.v1.LintRiskScenariosRequest;
import com.acme.risk.v1.LintRiskScenariosResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferCalculationServiceGrpc;
import com.acme.treasury.v1.BankTransferLine;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.EstimateFxHedgeRequest;
import com.acme.treasury.v1.EstimateFxHedgeResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeBankSettlement;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.SimulateBankTransferRequest;
import com.acme.treasury.v1.SimulateBankTransferResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Answers read-only questions about invoice templates, for screens and reports. Coordinates validation, durable
 * audit writes, and downstream operations.
 */
@Service
public class InvoiceTemplateQueryApplication {
    private static final Logger log = LoggerFactory.getLogger(InvoiceTemplateQueryApplication.class);

    private final AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking;

    private final ChannelRegistry channels;

    private final EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking;

    private final InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking;

    private final InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceFutureStub invoicingInvoiceBatchValidationServiceFuture;

    private final InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking;

    private final LineageNodeServiceGrpc.LineageNodeServiceStub lineageLineageNodeServiceAsync;

    private final RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking;

    private final RateLimitQueryServiceGrpc.RateLimitQueryServiceBlockingStub ratelimitingRateLimitQueryServiceBlocking;

    private final InvoicingRepository repository;

    private final RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking;

    private final BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    public InvoiceTemplateQueryApplication(
            AccessPolicyServiceGrpc.AccessPolicyServiceBlockingStub authorizationAccessPolicyServiceBlocking,
            ChannelRegistry channels,
            EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking,
            InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking,
            InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceFutureStub invoicingInvoiceBatchValidationServiceFuture,
            InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking,
            LineageNodeServiceGrpc.LineageNodeServiceStub lineageLineageNodeServiceAsync,
            RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking,
            RateLimitQueryServiceGrpc.RateLimitQueryServiceBlockingStub ratelimitingRateLimitQueryServiceBlocking,
            InvoicingRepository repository,
            RiskScenarioValidationServiceGrpc.RiskScenarioValidationServiceBlockingStub riskRiskScenarioValidationServiceBlocking,
            BankTransferCalculationServiceGrpc.BankTransferCalculationServiceBlockingStub treasuryBankTransferCalculationServiceBlocking,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking) {
        this.authorizationAccessPolicyServiceBlocking = authorizationAccessPolicyServiceBlocking;
        this.channels = channels;
        this.invoicingEInvoiceCalculationServiceBlocking = invoicingEInvoiceCalculationServiceBlocking;
        this.invoicingInvoiceBatchCalculationServiceBlocking = invoicingInvoiceBatchCalculationServiceBlocking;
        this.invoicingInvoiceBatchValidationServiceFuture = invoicingInvoiceBatchValidationServiceFuture;
        this.invoicingInvoiceTemplateValidationServiceBlocking = invoicingInvoiceTemplateValidationServiceBlocking;
        this.lineageLineageNodeServiceAsync = lineageLineageNodeServiceAsync;
        this.modelregistryRegisteredModelCalculationServiceBlocking = modelregistryRegisteredModelCalculationServiceBlocking;
        this.ratelimitingRateLimitQueryServiceBlocking = ratelimitingRateLimitQueryServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioValidationServiceBlocking = riskRiskScenarioValidationServiceBlocking;
        this.treasuryBankTransferCalculationServiceBlocking = treasuryBankTransferCalculationServiceBlocking;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
    }

    /**
     * Returns an invoice template.
     */
    public LookupInvoiceTemplateResponse lookupInvoiceTemplate(LookupInvoiceTemplateRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "LookupInvoiceTemplate")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "LookupInvoiceTemplate");
        return LookupInvoiceTemplateResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setInvoiceTemplateId(request.getInvoiceTemplateId())
                        .build())
                .build();
    }

    /**
     * Lists invoice templates a page at a time.
     */
    public BrowseInvoiceTemplatesResponse browseInvoiceTemplates(BrowseInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "BrowseInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "BrowseInvoiceTemplates");

        var outbound0 = EstimateRegisteredModelRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateRegisteredModelRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setRegisteredModel(RegisteredModel.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build();
        final class Stage0Adapter {
            EstimateRegisteredModelResponse fetch(EstimateRegisteredModelRequest input) {
                return modelregistryRegisteredModelCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateRegisteredModel(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "BrowseInvoiceTemplates", request.getPage().getPageToken());
        return BrowseInvoiceTemplatesResponse.newBuilder()
                .addInvoiceTemplates(InvoiceTemplate.newBuilder()
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .build();
    }

    /**
     * Lists invoice templates a page at a time.
     */
    public FindInvoiceTemplatesResponse findInvoiceTemplates(FindInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FindInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "FindInvoiceTemplates");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "FindInvoiceTemplates", request.getPage().getPageToken());
        return FindInvoiceTemplatesResponse.getDefaultInstance();
    }

    /**
     * Counts invoice templates by status.
     */
    public CountInvoiceTemplatesResponse countInvoiceTemplates(CountInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "CountInvoiceTemplates");

        var outbound0 = FindRateLimitsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindRateLimitsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ratelimitingRateLimitQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findRateLimits(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountInvoiceTemplates", request.getPage().getPageToken());
        return CountInvoiceTemplatesResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Returns several invoice templates at once.
     */
    public ResolveInvoiceTemplatesResponse resolveInvoiceTemplates(ResolveInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResolveInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "ResolveInvoiceTemplates");

        var outbound0 = WatchAccessPoliciesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream0 = authorizationAccessPolicyServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAccessPolicies(outbound0);
        var reply0 = WatchAccessPoliciesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = PreviewEInvoiceRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewEInvoiceRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.invoicing.v1.EInvoiceValidationService");
        var serviceName1 = "acme.invoicing.v1" + "." + "EInvoiceValidationService";
        var methodName1 = "Pre" + "viewEInvoice";
        var descriptor1 = MethodDescriptor.<PreviewEInvoiceRequest, PreviewEInvoiceResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(PreviewEInvoiceRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(PreviewEInvoiceResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        return ResolveInvoiceTemplatesResponse.newBuilder()
                .addInvoiceTemplates(InvoiceTemplate.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply1.getEInvoice().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
    }

    /**
     * Streams every matching invoice template.
     */
    public ExportInvoiceTemplatesResponse exportInvoiceTemplates(ExportInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExportInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "ExportInvoiceTemplates");

        var outbound0 = JsonCodec.roundTrip(SimulateBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .addLines(BankTransferLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(SimulateBankTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build(), SimulateBankTransferRequest.class);
        var reply0 = treasuryBankTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateBankTransfer(outbound0);

        var outbound1 = BeanCopies.copy(EstimateFxHedgeRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateFxHedgeRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getBankTransfer().getBillingAddress().getPostalCode())
                                .setRecipient(reply0.getBankTransfer().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setBank(FxHedgeBankSettlement.newBuilder()
                                .setBic(reply0.getBankTransfer().getBank().getBic())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(reply0.getBankTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build(), EstimateFxHedgeRequest.class);
        var reply1 = treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateFxHedge(outbound1);

        var outbound2 = ExplainEInvoiceCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setEInvoice(EInvoice.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getBankTransfer().getAudit().getEtag())
                                .setUpdatedBy(reply1.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getBankTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build();
        var adapter2 = new Function<ExplainEInvoiceCalculationRequest, ExplainEInvoiceCalculationResponse>() {
            @Override
            public ExplainEInvoiceCalculationResponse apply(ExplainEInvoiceCalculationRequest input) {
                return invoicingEInvoiceCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainEInvoiceCalculation(input);
            }
        };
        var reply2 = adapter2.apply(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExportInvoiceTemplates", request.getFilter().getQuery());
        return ExportInvoiceTemplatesResponse.newBuilder()
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getFxHedge().getAudit().getCreatedAt())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getFxHedge().getBillingAddress().getCountryCode())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setTotal(reply0.getBreakdown().getTotal())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply2.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Summarizes an invoice template.
     */
    public SummarizeInvoiceTemplatesResponse summarizeInvoiceTemplates(SummarizeInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SummarizeInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getInvoiceTemplateId(), "SummarizeInvoiceTemplates");

        var outbound0 = LintRiskScenariosRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> riskRiskScenarioValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintRiskScenarios(value));

        var outbound1 = NormalizeInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(NormalizeInvoiceBatchRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(reply0.getRiskScenario().getRequester().getName())
                                .build())
                        .build())
                .build();
        var pending1 = invoicingInvoiceBatchValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeInvoiceBatch(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = CheckInvoiceTemplateEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceTemplate(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getRiskScenario().getAudit().getCreatedBy())
                                .build())
                        .setBank(InvoiceTemplateBankSettlement.newBuilder()
                                .setAccountHolder(reply1.getInvoiceBatch().getBank().getAccountHolder())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getRiskScenario().getAssignee().getDisplayName())
                                .setEmail(reply1.getInvoiceBatch().getCounterparty().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(invoicingInvoiceTemplateValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkInvoiceTemplateEligibility(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = BeanCopies.copy(ScoreBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getInvoiceTemplate().getBillingAddress().getPostalCode())
                                .setRegion(reply1.getInvoiceBatch().getBillingAddress().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ScoreBankTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply2.getInvoiceTemplate().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .build(), ScoreBankTransferRequest.class);
        var reply3 = treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBankTransfer(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getInvoiceTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SummarizeInvoiceTemplates", request.getInvoiceTemplateId());
        return SummarizeInvoiceTemplatesResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply3.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists invoice templates a page at a time.
     */
    public SuggestInvoiceTemplatesResponse suggestInvoiceTemplates(SuggestInvoiceTemplatesRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuggestInvoiceTemplates")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "InvoiceTemplate", request.getContext().getTraceId(), "SuggestInvoiceTemplates");

        var outbound0 = QuoteInvoiceBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setInvoiceBatch(InvoiceBatch.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllMetadata(request.getFilter().getLabelsMap())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.invoicing.v1.InvoiceBatchCalculationService/QuoteInvoiceBatch", outbound0);
        var reply0 = invoicingInvoiceBatchCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).quoteInvoiceBatch(command0.payload());

        var outbound1 = BatchCreateLineageNodesRequest.newBuilder()
                .setContext(request.getContext())
                .setRecord(LineageNode.newBuilder()
                        .addChildren(LineageNode.newBuilder()
                                .addChildren(LineageNode.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setDisplayName(reply0.getInvoiceBatch().getCounterparty().getDisplayName())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
        var collector1 = new StreamCollector<>(BatchCreateLineageNodesResponse.getDefaultInstance());
        var input1 = lineageLineageNodeServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateLineageNodes(collector1);
        input1.onNext(outbound1);
        input1.onCompleted();
        var reply1 = collector1.await();
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuggestInvoiceTemplates", request.getPage().getPageToken());
        return SuggestInvoiceTemplatesResponse.newBuilder()
                .addInvoiceTemplates(InvoiceTemplate.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getInvoiceBatch().getAudit().getEtag())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(reply0.getInvoiceBatch().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build();
    }
}
