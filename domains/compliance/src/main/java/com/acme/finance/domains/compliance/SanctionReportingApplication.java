package com.acme.finance.domains.compliance;

import com.acme.cart.v1.Cart;
import com.acme.cart.v1.CartCalculationServiceGrpc;
import com.acme.cart.v1.CompareCartOptionsRequest;
import com.acme.cart.v1.CompareCartOptionsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CalculateSanctionRequest;
import com.acme.compliance.v1.CalculateSanctionResponse;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.DeleteSanctionReportRequest;
import com.acme.compliance.v1.DeleteSanctionReportResponse;
import com.acme.compliance.v1.FetchSanctionReportRequest;
import com.acme.compliance.v1.FetchSanctionReportResponse;
import com.acme.compliance.v1.GenerateSanctionReportRequest;
import com.acme.compliance.v1.GenerateSanctionReportResponse;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.compliance.v1.ScheduleSanctionReportRequest;
import com.acme.compliance.v1.ScheduleSanctionReportResponse;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.SuggestLedgerAccountsRequest;
import com.acme.ledger.v1.SuggestLedgerAccountsResponse;
import com.acme.risk.v1.CancelExposureWorkflowRequest;
import com.acme.risk.v1.CancelExposureWorkflowResponse;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RejectRiskScenarioRequest;
import com.acme.risk.v1.RejectRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SignalExposureWorkflowRequest;
import com.acme.risk.v1.SignalExposureWorkflowResponse;
import com.acme.search.v1.CheckSearchIndexEligibilityRequest;
import com.acme.search.v1.CheckSearchIndexEligibilityResponse;
import com.acme.subscriptions.v1.CreateSubscriptionRequest;
import com.acme.subscriptions.v1.CreateSubscriptionResponse;
import com.acme.subscriptions.v1.Subscription;
import com.acme.subscriptions.v1.SubscriptionServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import java.util.Optional;
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
 * Builds and schedules reports about sanctions. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class SanctionReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(SanctionReportingApplication.class);

    private final CartCalculationServiceGrpc.CartCalculationServiceBlockingStub cartCartCalculationServiceBlocking;

    private final ChannelRegistry channels;

    private final SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking;

    private final ApplicationEventPublisher events;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final SanctionReportingRequestMapper mapper;

    private final ComplianceRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking;

    public SanctionReportingApplication(
            CartCalculationServiceGrpc.CartCalculationServiceBlockingStub cartCartCalculationServiceBlocking,
            ChannelRegistry channels,
            SanctionCalculationServiceGrpc.SanctionCalculationServiceBlockingStub complianceSanctionCalculationServiceBlocking,
            ApplicationEventPublisher events,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            SanctionReportingRequestMapper mapper,
            ComplianceRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SubscriptionServiceGrpc.SubscriptionServiceBlockingStub subscriptionsSubscriptionServiceBlocking) {
        this.cartCartCalculationServiceBlocking = cartCartCalculationServiceBlocking;
        this.channels = channels;
        this.complianceSanctionCalculationServiceBlocking = complianceSanctionCalculationServiceBlocking;
        this.events = events;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.subscriptionsSubscriptionServiceBlocking = subscriptionsSubscriptionServiceBlocking;
    }

    public record FetchSanctionReportStage0Requested(CheckSearchIndexEligibilityRequest request, CompletableFuture<CheckSearchIndexEligibilityResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a sanction.
     */
    public GenerateSanctionReportResponse generateSanctionReport(GenerateSanctionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateSanctionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "GenerateSanctionReport");

        var outbound0 = SignalExposureWorkflowRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SignalExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .putAllParameters(request.getDetails().getFiltersMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).signalExposureWorkflow(value)).orElseThrow();

        var outbound1 = PauseExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getExposure().getAudit().getUpdatedBy())
                                .build())
                        .setHolder(SupplierRef.newBuilder()
                                .setEmail(reply0.getExposure().getHolder().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseExposureWorkflow(value));
        log.atDebug().addKeyValue("auditValue", request.getSanctionId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateSanctionReport", request.getSanctionId());
        return GenerateSanctionReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a sanction.
     */
    public FetchSanctionReportResponse fetchSanctionReport(FetchSanctionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchSanctionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "FetchSanctionReport");

        var outbound0 = CheckSearchIndexEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckSearchIndexEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CheckSearchIndexEligibilityResponse>();
        events.publishEvent(new FetchSanctionReportStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return FetchSanctionReportResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(reply0.getSearchIndex().getAudit().getEtag())
                                                .build())
                                        .build())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(reply0.getSearchIndex().getOwner().getEmail())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
    }

    /**
     * Schedules a sanction.
     */
    public ScheduleSanctionReportResponse scheduleSanctionReport(ScheduleSanctionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleSanctionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "ScheduleSanctionReport");

        var outbound0 = SuggestLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getDetails().getFiltersMap())
                        .setOwnerId(request.getSanctionId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestLedgerAccounts(outbound0), rpcExecutor);

        var outbound1 = ResolvePremiumConflictRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ResolvePremiumConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleSanctionReport", request.getDetails().getFormat());
        return ScheduleSanctionReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of sanctions as they happen.
     */
    public StreamSanctionMetricsResponse streamSanctionMetrics(StreamSanctionMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamSanctionMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getContext().getTraceId(), "StreamSanctionMetrics");

        var outbound0 = CreateSubscriptionRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscription(Subscription.newBuilder()
                        .setHolder(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(subscriptionsSubscriptionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).createSubscription(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = RpcSupport.unpack(Any.pack(RejectRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getSubscription().getAudit().getCreatedAt())
                                .build())
                        .build())
                .build()), RejectRiskScenarioRequest.class);
        var reply1 = riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectRiskScenario(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamSanctionMetrics", request.getFilter().getQuery());
        return StreamSanctionMetricsResponse.newBuilder()
                .setSanction(Sanction.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getRiskScenario().getAudit().getCreatedAt())
                                .setEtag(reply0.getSubscription().getAudit().getEtag())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setManager(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
    }

    /**
     * Lists sanctions a page at a time.
     */
    public ListSanctionReportsResponse listSanctionReports(ListSanctionReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListSanctionReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getContext().getTraceId(), "ListSanctionReports");

        var outbound0 = CancelExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setExposure(Exposure.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.risk.v1.ExposureWorkflowService");
        var selected0 = ExposureWorkflowServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.cancelExposureWorkflow(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListSanctionReports", request.getPage().getPageToken());
        return ListSanctionReportsResponse.newBuilder()
                .addSanctions(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .setAudit(AuditInfo.newBuilder()
                                        .setCreatedBy(reply0.getExposure().getAudit().getCreatedBy())
                                        .build())
                                .addChildren(Sanction.newBuilder()
                                        .setManager(UserRef.newBuilder()
                                                .setEmail(reply0.getExposure().getContact().getEmail())
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts sanctions by status.
     */
    public CountSanctionTotalsResponse countSanctionTotals(CountSanctionTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountSanctionTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getContext().getTraceId(), "CountSanctionTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountSanctionTotals", request.getPage().getPageToken());
        return CountSanctionTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes a sanction.
     */
    public DeleteSanctionReportResponse deleteSanctionReport(DeleteSanctionReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteSanctionReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Sanction", request.getSanctionId(), "DeleteSanctionReport");

        var outbound0 = CalculateSanctionRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setSanction(Sanction.newBuilder()
                        .addChildren(Sanction.newBuilder()
                                .addChildren(Sanction.newBuilder()
                                        .setAudit(AuditInfo.newBuilder()
                                                .setEtag(request.getEtag())
                                                .build())
                                        .setManager(UserRef.newBuilder()
                                                .setUserId(request.getContext().getActor().getUserId())
                                                .build())
                                        .build())
                                .build())
                        .setSanctionId(request.getSanctionId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> complianceSanctionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateSanction(value)).orElseThrow();

        var outbound1 = mapper.deleteSanctionReportStage1(request, reply0);
        var reply1 = cartCartCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).compareCartOptions(outbound1);
        return DeleteSanctionReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }
}
