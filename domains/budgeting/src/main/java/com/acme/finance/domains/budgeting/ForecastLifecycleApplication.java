package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.BudgetRef;
import com.acme.budgeting.v1.ExpireForecastRequest;
import com.acme.budgeting.v1.ExpireForecastResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetForecastLifecycleRequest;
import com.acme.budgeting.v1.GetForecastLifecycleResponse;
import com.acme.budgeting.v1.ReconcileForecastsRequest;
import com.acme.budgeting.v1.ReconcileForecastsResponse;
import com.acme.budgeting.v1.ReinstateForecastRequest;
import com.acme.budgeting.v1.ReinstateForecastResponse;
import com.acme.budgeting.v1.RenewForecastRequest;
import com.acme.budgeting.v1.RenewForecastResponse;
import com.acme.budgeting.v1.ScheduleForecastExpiryRequest;
import com.acme.budgeting.v1.ScheduleForecastExpiryResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SuspendForecastRequest;
import com.acme.budgeting.v1.SuspendForecastResponse;
import com.acme.budgeting.v1.TerminateForecastRequest;
import com.acme.budgeting.v1.TerminateForecastResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.compliance.v1.StreamSanctionMetricsRequest;
import com.acme.compliance.v1.StreamSanctionMetricsResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fulfillment.v1.BrowseFulfillmentsRequest;
import com.acme.fulfillment.v1.BrowseFulfillmentsResponse;
import com.acme.fulfillment.v1.FulfillmentQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.risk.v1.DelegateStressTestApprovalRequest;
import com.acme.risk.v1.DelegateStressTestApprovalResponse;
import com.acme.risk.v1.StressTestApprovalServiceGrpc;
import com.acme.suppliers.v1.BatchGetSuppliersRequest;
import com.acme.suppliers.v1.BatchGetSuppliersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CountSuppliersRequest;
import com.acme.suppliers.v1.CountSuppliersResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.ScoreSupplierRatingRequest;
import com.acme.suppliers.v1.ScoreSupplierRatingResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.UpdateSupplierRequest;
import com.acme.suppliers.v1.UpdateSupplierResponse;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Moves a forecast between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class ForecastLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(ForecastLifecycleApplication.class);

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final ApplicationEventPublisher events;

    private final FulfillmentQueryServiceGrpc.FulfillmentQueryServiceBlockingStub fulfillmentFulfillmentQueryServiceBlocking;

    private final ForecastLifecycleRequestMapper mapper;

    private final BudgetingRepository repository;

    private final StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture;

    private final SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking;

    public ForecastLifecycleApplication(
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            ChannelRegistry channels,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            ApplicationEventPublisher events,
            FulfillmentQueryServiceGrpc.FulfillmentQueryServiceBlockingStub fulfillmentFulfillmentQueryServiceBlocking,
            ForecastLifecycleRequestMapper mapper,
            BudgetingRepository repository,
            StressTestApprovalServiceGrpc.StressTestApprovalServiceBlockingStub riskStressTestApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture,
            SupplierServiceGrpc.SupplierServiceBlockingStub suppliersSupplierServiceBlocking) {
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.channels = channels;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.events = events;
        this.fulfillmentFulfillmentQueryServiceBlocking = fulfillmentFulfillmentQueryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskStressTestApprovalServiceBlocking = riskStressTestApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRatingValidationServiceFuture = suppliersSupplierRatingValidationServiceFuture;
        this.suppliersSupplierServiceBlocking = suppliersSupplierServiceBlocking;
    }

    public record RenewForecastStage0Requested(ValidateSupplierRatingRequest request, CompletableFuture<ValidateSupplierRatingResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a forecast.
     */
    public ActivateForecastResponse activateForecast(ActivateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "ActivateForecast");

        var outbound0 = StreamSanctionMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNewOwner().getEmail())
                        .build())
                .build();
        var stream0 = complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamSanctionMetrics(outbound0);
        var reply0 = StreamSanctionMetricsResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }

        var outbound1 = ShareVarianceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareVarianceReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        Function<ShareVarianceReportRequest, ShareVarianceReportResponse> operation1 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::shareVarianceReport;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateForecast", request.getDetails().getReasonCode());
        return ActivateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ActivateForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply1.getOutcome().getEffectiveAt())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .build())
                .build();
    }

    /**
     * Suspends a forecast.
     */
    public SuspendForecastResponse suspendForecast(SuspendForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "SuspendForecast");

        var outbound0 = ShareVarianceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareVarianceReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.VarianceReportingService");
        var serviceName0 = "acme.budgeting.v1" + "." + "VarianceReportingService";
        var methodName0 = "Sha" + "reVarianceReport";
        var descriptor0 = MethodDescriptor.<ShareVarianceReportRequest, ShareVarianceReportResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName0, methodName0))
                .setRequestMarshaller(ProtoUtils.marshaller(ShareVarianceReportRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ShareVarianceReportResponse.getDefaultInstance()))
                .build();
        var reply0 = ClientCalls.blockingUnaryCall(channel0, descriptor0,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound0);

        var outbound1 = BrowseFulfillmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFulfillmentsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getVariance().getSla().getDueAt())
                        .putAllLabels(reply0.getVariance().getCustomFieldsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var adapter1 = new Function<BrowseFulfillmentsRequest, BrowseFulfillmentsResponse>() {
            @Override
            public BrowseFulfillmentsResponse apply(BrowseFulfillmentsRequest input) {
                return fulfillmentFulfillmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseFulfillments(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendForecast", request.getDetails().getReasonCode());
        return SuspendForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(reply0.getVariance().getSla().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(SuspendForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a forecast.
     */
    public ReinstateForecastResponse reinstateForecast(ReinstateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "ReinstateForecast");

        var outbound0 = BeanCopies.copy(ReconcileForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileForecastsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .build(), ReconcileForecastsRequest.class);
        var reply0 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileForecasts(outbound0);

        var outbound1 = RpcSupport.unpack(Any.pack(ExplainSupplierRatingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSupplierRatingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build()), ExplainSupplierRatingDecisionRequest.class);
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSupplierRatingDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateForecast", request.getDetails().getReasonCode());
        return ReinstateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getForecast().getAudit().getCreatedAt())
                                .build())
                        .setCounterparty(BudgetRef.newBuilder()
                                .setDisplayName(reply1.getSupplierRating().getCounterparty().getDisplayName())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ReinstateForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Terminates a forecast.
     */
    public TerminateForecastResponse terminateForecast(TerminateForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "TerminateForecast");

        var outbound0 = mapper.terminateForecastStage0(request);
        var reply0 = riskStressTestApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).delegateStressTestApproval(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateForecast", request.getDetails().getReasonCode());
        return TerminateForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(reply0.getStressTest().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(TerminateForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Renews a forecast.
     */
    public RenewForecastResponse renewForecast(RenewForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "RenewForecast");

        var outbound0 = ValidateSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ValidateSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .build())
                .build();
        var completion0 = new CompletableFuture<ValidateSupplierRatingResponse>();
        events.publishEvent(new RenewForecastStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = mapper.renewForecastStage1(request);
        var reply1 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetSuppliers(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewForecast", request.getDetails().getReasonCode());
        return RenewForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(reply0.getSupplierRating().getBillingAddress())
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(RenewForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a forecast.
     */
    public ExpireForecastResponse expireForecast(ExpireForecastRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireForecast")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "ExpireForecast");

        var outbound0 = ScoreSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        var pending0 = suppliersSupplierRatingValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).scoreSupplierRating(outbound0);
        var reply0 = RpcSupport.await(pending0);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireForecast", request.getDetails().getReasonCode());
        return ExpireForecastResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setForecastId(request.getForecastId())
                        .build())
                .setOutcome(ExpireForecastResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Returns a forecast.
     */
    public GetForecastLifecycleResponse getForecastLifecycle(GetForecastLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetForecastLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Forecast", request.getForecastId(), "GetForecastLifecycle");

        var outbound0 = (UpdateSupplierRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplier(Supplier.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = suppliersSupplierServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateSupplier(outbound0);

        var outbound1 = BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getSupplier().getPassport().getExpiresAt())
                        .setRegion(reply0.getSupplier().getPrimaryAddress().getRegion())
                        .build())
                .build();
        var adapter1 = new Function<BrowseSupplierContractsRequest, BrowseSupplierContractsResponse>() {
            @Override
            public BrowseSupplierContractsResponse apply(BrowseSupplierContractsRequest input) {
                return suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(input);
            }
        };
        var reply1 = adapter1.apply(outbound1);
        return GetForecastLifecycleResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getSupplier().getAudit().getEtag())
                                .build())
                        .setForecastId(request.getForecastId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Schedules a forecast.
     */
    public ScheduleForecastExpiryResponse scheduleForecastExpiry(ScheduleForecastExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleForecastExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveForecast(request.getContext().getTenantId(), request.getForecast());

        var futures0 = request.getForecast().getLinesList().stream()
                .map(element -> CountSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSuppliersRequest.Filter.newBuilder()
                        .setOwnerId(element.getLineId())
                        .setRegion(request.getForecast().getBillingAddress().getRegion())
                        .build())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSuppliers(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures0.toArray(CompletableFuture[]::new)).join();
        var results0 = futures0.stream().map(CompletableFuture::join).toList();
        var reply0 = results0.isEmpty() ? CountSuppliersResponse.getDefaultInstance()
                : results0.getLast();
        log.atDebug().addKeyValue("auditValue", request.getForecast().getForecastId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleForecastExpiry", request.getForecast().getForecastId());
        return ScheduleForecastExpiryResponse.newBuilder()
                .setForecast(Forecast.newBuilder()
                        .setDueAt(request.getForecast().getDueAt())
                        .setForecastId(request.getForecastId())
                        .build())
                .build();
    }
}
