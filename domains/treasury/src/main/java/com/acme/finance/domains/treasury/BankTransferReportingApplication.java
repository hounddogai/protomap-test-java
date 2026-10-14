package com.acme.finance.domains.treasury;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ApproveAttestationRequest;
import com.acme.compliance.v1.ApproveAttestationResponse;
import com.acme.compliance.v1.Attestation;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionRequest;
import com.acme.customers.v1.ExplainCustomerSegmentDecisionResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.featureflags.v1.FeatureFlagServiceGrpc;
import com.acme.featureflags.v1.ListFeatureFlagsRequest;
import com.acme.featureflags.v1.ListFeatureFlagsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchCardSettlement;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.PreviewPostingBatchRequest;
import com.acme.ledger.v1.PreviewPostingBatchResponse;
import com.acme.lending.v1.ApproveLoanApplicationRequest;
import com.acme.lending.v1.ApproveLoanApplicationResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.maintenance.v1.EstimateWorkOrderRequest;
import com.acme.maintenance.v1.EstimateWorkOrderResponse;
import com.acme.maintenance.v1.WorkOrder;
import com.acme.maintenance.v1.WorkOrderCalculationServiceGrpc;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionRequest;
import com.acme.metrics.v1.ExplainMetricSeriesDecisionResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesComponent;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.patients.v1.PatientRef;
import com.acme.prescriptions.v1.Prescription;
import com.acme.prescriptions.v1.PrescriptionCalculationServiceGrpc;
import com.acme.prescriptions.v1.RecalculatePrescriptionsRequest;
import com.acme.prescriptions.v1.RecalculatePrescriptionsResponse;
import com.acme.risk.v1.RecallRiskScenarioRequest;
import com.acme.risk.v1.RecallRiskScenarioResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.treasury.v1.BankTransfer;
import com.acme.treasury.v1.BankTransferValidationServiceGrpc;
import com.acme.treasury.v1.CashPosition;
import com.acme.treasury.v1.CashPositionQueryServiceGrpc;
import com.acme.treasury.v1.CashPositionRef;
import com.acme.treasury.v1.CashPositionValidationServiceGrpc;
import com.acme.treasury.v1.CountBankTransferTotalsRequest;
import com.acme.treasury.v1.CountBankTransferTotalsResponse;
import com.acme.treasury.v1.CountCashPositionsRequest;
import com.acme.treasury.v1.CountCashPositionsResponse;
import com.acme.treasury.v1.DeleteBankTransferReportRequest;
import com.acme.treasury.v1.DeleteBankTransferReportResponse;
import com.acme.treasury.v1.ExplainCashPositionDecisionRequest;
import com.acme.treasury.v1.ExplainCashPositionDecisionResponse;
import com.acme.treasury.v1.ExplainFxHedgeCalculationRequest;
import com.acme.treasury.v1.ExplainFxHedgeCalculationResponse;
import com.acme.treasury.v1.ExplainFxHedgeDecisionRequest;
import com.acme.treasury.v1.ExplainFxHedgeDecisionResponse;
import com.acme.treasury.v1.FetchBankTransferReportRequest;
import com.acme.treasury.v1.FetchBankTransferReportResponse;
import com.acme.treasury.v1.FxHedge;
import com.acme.treasury.v1.FxHedgeCalculationServiceGrpc;
import com.acme.treasury.v1.FxHedgeLine;
import com.acme.treasury.v1.FxHedgeSyncServiceGrpc;
import com.acme.treasury.v1.FxHedgeValidationServiceGrpc;
import com.acme.treasury.v1.GenerateBankTransferReportRequest;
import com.acme.treasury.v1.GenerateBankTransferReportResponse;
import com.acme.treasury.v1.LiquidityForecast;
import com.acme.treasury.v1.LiquidityForecastCalculationServiceGrpc;
import com.acme.treasury.v1.ListBankTransferReportsRequest;
import com.acme.treasury.v1.ListBankTransferReportsResponse;
import com.acme.treasury.v1.RecalculateLiquidityForecastsRequest;
import com.acme.treasury.v1.RecalculateLiquidityForecastsResponse;
import com.acme.treasury.v1.ReconcileFxHedgesRequest;
import com.acme.treasury.v1.ReconcileFxHedgesResponse;
import com.acme.treasury.v1.ScheduleBankTransferReportRequest;
import com.acme.treasury.v1.ScheduleBankTransferReportResponse;
import com.acme.treasury.v1.ScoreBankTransferRequest;
import com.acme.treasury.v1.ScoreBankTransferResponse;
import com.acme.treasury.v1.ShareBankTransferReportRequest;
import com.acme.treasury.v1.ShareBankTransferReportResponse;
import com.acme.treasury.v1.StreamBankTransferMetricsRequest;
import com.acme.treasury.v1.StreamBankTransferMetricsResponse;
import com.acme.treasury.v1.StreamFxHedgeChangesRequest;
import com.acme.treasury.v1.StreamFxHedgeChangesResponse;
import com.google.protobuf.Any;
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
 * Builds and schedules reports about bank transfers. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class BankTransferReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(BankTransferReportingApplication.class);

    private final ChannelRegistry channels;

    private final CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking;

    private final HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking;

    private final FeatureFlagServiceGrpc.FeatureFlagServiceFutureStub featureflagsFeatureFlagServiceFuture;

    private final PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking;

    private final LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking;

    private final WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking;

    private final BankTransferReportingRequestMapper mapper;

    private final MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking;

    private final PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking;

    private final TreasuryRepository repository;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    private final BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking;

    private final CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking;

    private final CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking;

    private final FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceStub treasuryFxHedgeSyncServiceAsync;

    private final FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking;

    private final FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking;

    private final LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking;

    public BankTransferReportingApplication(
            ChannelRegistry channels,
            CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking,
            HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking,
            FeatureFlagServiceGrpc.FeatureFlagServiceFutureStub featureflagsFeatureFlagServiceFuture,
            PostingBatchValidationServiceGrpc.PostingBatchValidationServiceBlockingStub ledgerPostingBatchValidationServiceBlocking,
            LoanApplicationApprovalServiceGrpc.LoanApplicationApprovalServiceBlockingStub lendingLoanApplicationApprovalServiceBlocking,
            WorkOrderCalculationServiceGrpc.WorkOrderCalculationServiceBlockingStub maintenanceWorkOrderCalculationServiceBlocking,
            BankTransferReportingRequestMapper mapper,
            MetricSeriesValidationServiceGrpc.MetricSeriesValidationServiceBlockingStub metricsMetricSeriesValidationServiceBlocking,
            PrescriptionCalculationServiceGrpc.PrescriptionCalculationServiceBlockingStub prescriptionsPrescriptionCalculationServiceBlocking,
            TreasuryRepository repository,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            BankTransferValidationServiceGrpc.BankTransferValidationServiceBlockingStub treasuryBankTransferValidationServiceBlocking,
            CashPositionQueryServiceGrpc.CashPositionQueryServiceBlockingStub treasuryCashPositionQueryServiceBlocking,
            CashPositionValidationServiceGrpc.CashPositionValidationServiceBlockingStub treasuryCashPositionValidationServiceBlocking,
            FxHedgeCalculationServiceGrpc.FxHedgeCalculationServiceBlockingStub treasuryFxHedgeCalculationServiceBlocking,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceStub treasuryFxHedgeSyncServiceAsync,
            FxHedgeSyncServiceGrpc.FxHedgeSyncServiceBlockingStub treasuryFxHedgeSyncServiceBlocking,
            FxHedgeValidationServiceGrpc.FxHedgeValidationServiceBlockingStub treasuryFxHedgeValidationServiceBlocking,
            LiquidityForecastCalculationServiceGrpc.LiquidityForecastCalculationServiceBlockingStub treasuryLiquidityForecastCalculationServiceBlocking) {
        this.channels = channels;
        this.customersCustomerSegmentValidationServiceBlocking = customersCustomerSegmentValidationServiceBlocking;
        this.customersHouseholdValidationServiceBlocking = customersHouseholdValidationServiceBlocking;
        this.featureflagsFeatureFlagServiceFuture = featureflagsFeatureFlagServiceFuture;
        this.ledgerPostingBatchValidationServiceBlocking = ledgerPostingBatchValidationServiceBlocking;
        this.lendingLoanApplicationApprovalServiceBlocking = lendingLoanApplicationApprovalServiceBlocking;
        this.maintenanceWorkOrderCalculationServiceBlocking = maintenanceWorkOrderCalculationServiceBlocking;
        this.mapper = mapper;
        this.metricsMetricSeriesValidationServiceBlocking = metricsMetricSeriesValidationServiceBlocking;
        this.prescriptionsPrescriptionCalculationServiceBlocking = prescriptionsPrescriptionCalculationServiceBlocking;
        this.repository = repository;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.treasuryBankTransferValidationServiceBlocking = treasuryBankTransferValidationServiceBlocking;
        this.treasuryCashPositionQueryServiceBlocking = treasuryCashPositionQueryServiceBlocking;
        this.treasuryCashPositionValidationServiceBlocking = treasuryCashPositionValidationServiceBlocking;
        this.treasuryFxHedgeCalculationServiceBlocking = treasuryFxHedgeCalculationServiceBlocking;
        this.treasuryFxHedgeSyncServiceAsync = treasuryFxHedgeSyncServiceAsync;
        this.treasuryFxHedgeSyncServiceBlocking = treasuryFxHedgeSyncServiceBlocking;
        this.treasuryFxHedgeValidationServiceBlocking = treasuryFxHedgeValidationServiceBlocking;
        this.treasuryLiquidityForecastCalculationServiceBlocking = treasuryLiquidityForecastCalculationServiceBlocking;
    }

    /**
     * Generates a bank transfer.
     */
    public GenerateBankTransferReportResponse generateBankTransferReport(GenerateBankTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateBankTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "GenerateBankTransferReport");

        var outbound0 = (EstimateWorkOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setWorkOrder(WorkOrder.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply0 = maintenanceWorkOrderCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateWorkOrder(outbound0);

        var outbound1 = ExplainMetricSeriesDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainMetricSeriesDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getWorkOrder().getRequester().getEmail())
                                .build())
                        .build())
                .setMetricSeries(MetricSeries.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(switch (reply0.getWorkOrder().getOriginCase()) {
                                    case CUSTOMER -> reply0.getWorkOrder().getCustomer().getDisplayName();
                                    default -> "";
                                })
                                .setUserId(reply0.getWorkOrder().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var reply1 = metricsMetricSeriesValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainMetricSeriesDecision(outbound1);

        var outbound2 = ExplainFxHedgeDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getWorkOrder().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(switch (reply1.getMetricSeries().getLocatorCase()) {
                                    case ADDRESS -> reply1.getMetricSeries().getAddress().getPostalCode();
                                    default -> "";
                                })
                                .build())
                        .addLines(FxHedgeLine.newBuilder()
                                .setDescription(reply1.getMetricSeries().getComponentsOrDefault("primary", MetricSeriesComponent.getDefaultInstance()).getDescription())
                                .build())
                        .build())
                .build();
        var reply2 = treasuryFxHedgeValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFxHedgeDecision(outbound2);

        var outbound3 = ExplainCustomerSegmentDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply0.getWorkOrder().getCustomer().getDisplayName())
                                .setUserId(reply0.getWorkOrder().getAssignee().getUserId())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.customers.v1.CustomerSegmentValidationService");
        var selected3 = CustomerSegmentValidationServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.explainCustomerSegmentDecision(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateBankTransferReport", request.getBankTransferId());
        return GenerateBankTransferReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply2.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a bank transfer.
     */
    public FetchBankTransferReportResponse fetchBankTransferReport(FetchBankTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchBankTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "FetchBankTransferReport");

        var outbound0 = ExplainCashPositionDecisionRequest.newBuilder()
                .setCashPosition(CashPosition.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var reply0 = treasuryCashPositionValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainCashPositionDecision(outbound0);
        return FetchBankTransferReportResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setBankTransferId(request.getBankTransferId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a bank transfer.
     */
    public ScheduleBankTransferReportResponse scheduleBankTransferReport(ScheduleBankTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleBankTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "ScheduleBankTransferReport");

        var outbound0 = RecalculatePrescriptionsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setPrescription(Prescription.newBuilder()
                        .setCounterparty(PatientRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var command0 = new DispatchCommand<>("acme.prescriptions.v1.PrescriptionCalculationService/RecalculatePrescriptions", outbound0);
        var reply0 = prescriptionsPrescriptionCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculatePrescriptions(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleBankTransferReport", request.getBankTransferId());
        return ScheduleBankTransferReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of bank transfers as they happen.
     */
    public StreamBankTransferMetricsResponse streamBankTransferMetrics(StreamBankTransferMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamBankTransferMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "StreamBankTransferMetrics");

        var outbound0 = ScoreBankTransferRequest.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> treasuryBankTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreBankTransfer(outbound0), rpcExecutor);

        var outbound1 = CountCashPositionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCashPositionsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getFilter().getCreatedAfter())
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setQuery(request.getFilter().getQuery())
                        .setRegion(request.getFilter().getRegion())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> treasuryCashPositionQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countCashPositions(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = RpcSupport.unpack(Any.pack(RecalculateLiquidityForecastsRequest.newBuilder()
                .setContext(request.getContext())
                .setLiquidityForecast(LiquidityForecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getBankTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build()), RecalculateLiquidityForecastsRequest.class);
        var reply2 = treasuryLiquidityForecastCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateLiquidityForecasts(outbound2);

        var outbound3 = ApproveAttestationRequest.newBuilder()
                .setAttestation(Attestation.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply2.getLiquidityForecast().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var channel3 = channels.lookup("acme.compliance.v1.AttestationApprovalService");
        var serviceName3 = "acme.compliance.v1" + "." + "AttestationApprovalService";
        var methodName3 = "App" + "roveAttestation";
        var descriptor3 = MethodDescriptor.<ApproveAttestationRequest, ApproveAttestationResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName3, methodName3))
                .setRequestMarshaller(ProtoUtils.marshaller(ApproveAttestationRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ApproveAttestationResponse.getDefaultInstance()))
                .build();
        var reply3 = ClientCalls.blockingUnaryCall(channel3, descriptor3,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound3);
        log.atDebug().addKeyValue("auditValue", request.getSubscriber().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamBankTransferMetrics", request.getSubscriber().getPhone());
        return StreamBankTransferMetricsResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getBankTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setBankTransferId(reply0.getBankTransfer().getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply3.getAttestation().getBillingAddress().getPostalCode())
                                .setRegion(reply2.getLiquidityForecast().getBillingAddress().getRegion())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .setDueAt(reply3.getAttestation().getDueAt())
                        .setNumber(reply2.getLiquidityForecast().getNumber())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Lists bank transfers a page at a time.
     */
    public ListBankTransferReportsResponse listBankTransferReports(ListBankTransferReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBankTransferReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "ListBankTransferReports");

        var outbound0 = ReconcileFxHedgesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileFxHedgesRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .addLines(FxHedgeLine.newBuilder()
                                .putAllAttributes(request.getFilter().getLabelsMap())
                                .build())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.treasury.v1.FxHedgeSyncService/ReconcileFxHedges", outbound0);
        var reply0 = treasuryFxHedgeSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileFxHedges(command0.payload());
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBankTransferReports", request.getPage().getPageToken());
        return ListBankTransferReportsResponse.newBuilder()
                .addBankTransfers(BankTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getFxHedge().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
    }

    /**
     * Shares a bank transfer.
     */
    public ShareBankTransferReportResponse shareBankTransferReport(ShareBankTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareBankTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "ShareBankTransferReport");

        var outbound0 = RpcSupport.unpack(Any.pack(ExplainHouseholdDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainHouseholdDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .setHousehold(Household.newBuilder()
                        .addChildren(Household.newBuilder()
                                .setManager(UserRef.newBuilder()
                                        .setEmail(request.getDetails().getRecipient().getEmail())
                                        .build())
                                .build())
                        .build())
                .build()), ExplainHouseholdDecisionRequest.class);
        var reply0 = customersHouseholdValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainHouseholdDecision(outbound0);

        var outbound1 = mapper.shareBankTransferReportStage1(request, reply0);
        var reply1 = lendingLoanApplicationApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).approveLoanApplication(outbound1);

        var outbound2 = ListFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getHousehold().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getLoanApplication().getMetadataMap())
                        .setRegion(reply1.getLoanApplication().getBillingAddress().getRegion())
                        .build())
                .build();
        var pending2 = featureflagsFeatureFlagServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).listFeatureFlags(outbound2);
        var reply2 = RpcSupport.await(pending2);

        var outbound3 = StreamFxHedgeChangesRequest.newBuilder()
                .setChange(FxHedge.newBuilder()
                        .setCounterparty(CashPositionRef.newBuilder()
                                .setDisplayName(reply0.getHousehold().getManager().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var collector3 = new StreamCollector<>(StreamFxHedgeChangesResponse.getDefaultInstance());
        var input3 = treasuryFxHedgeSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamFxHedgeChanges(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareBankTransferReport", request.getDetails().getRecipient().getPhone());
        return ShareBankTransferReportResponse.newBuilder()
                .setBankTransfer(BankTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply0.getHousehold().getAudit().getUpdatedBy())
                                .build())
                        .setBankTransferId(request.getBankTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getHousehold().getLocation().getCountryCode())
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setCustomerId(reply1.getLoanApplication().getCounterparty().getCustomerId())
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .setOutcome(ShareBankTransferReportResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Counts bank transfers by status.
     */
    public CountBankTransferTotalsResponse countBankTransferTotals(CountBankTransferTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountBankTransferTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getContext().getTraceId(), "CountBankTransferTotals");
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountBankTransferTotals", request.getPage().getPageToken());
        return CountBankTransferTotalsResponse.getDefaultInstance();
    }

    /**
     * Deletes a bank transfer.
     */
    public DeleteBankTransferReportResponse deleteBankTransferReport(DeleteBankTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteBankTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "BankTransfer", request.getBankTransferId(), "DeleteBankTransferReport");

        var outbound0 = ExplainFxHedgeCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setFxHedge(FxHedge.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var adapter0 = new Function<ExplainFxHedgeCalculationRequest, ExplainFxHedgeCalculationResponse>() {
            @Override
            public ExplainFxHedgeCalculationResponse apply(ExplainFxHedgeCalculationRequest input) {
                return treasuryFxHedgeCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFxHedgeCalculation(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = PreviewPostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getFxHedge().getBillingAddress().getCountryCode())
                                .build())
                        .setCard(PostingBatchCardSettlement.newBuilder()
                                .setHolderName(switch (reply0.getFxHedge().getSettlementCase()) {
                                    case CARD -> reply0.getFxHedge().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(LedgerAccountRef.newBuilder()
                                .setCurrencyCode(reply0.getFxHedge().getCounterparty().getCurrencyCode())
                                .build())
                        .build())
                .build();
        var reply1 = ledgerPostingBatchValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewPostingBatch(outbound1);

        var outbound2 = ValidateCustomerSegmentRequest.newBuilder()
                .setContext(request.getContext())
                .setCustomerSegment(CustomerSegment.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getPostingBatch().getAudit().getCreatedBy())
                                .setEtag(reply0.getFxHedge().getAudit().getEtag())
                                .setUpdatedAt(reply0.getFxHedge().getAudit().getUpdatedAt())
                                .setUpdatedBy(reply0.getFxHedge().getAudit().getUpdatedBy())
                                .build())
                        .build())
                .build();
        var reply2 = Optional.of(outbound2).map(value -> value.toBuilder().build())
                .map(value -> customersCustomerSegmentValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateCustomerSegment(value)).orElseThrow();

        var outbound3 = RecallRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply2.getCustomerSegment().getOwner().getDisplayName())
                                .setUserId(reply2.getCustomerSegment().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getPostingBatch().getAudit().getEtag())
                                .build())
                        .build())
                .build();
        var adapter3 = new Function<RecallRiskScenarioRequest, RecallRiskScenarioResponse>() {
            @Override
            public RecallRiskScenarioResponse apply(RecallRiskScenarioRequest input) {
                return riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallRiskScenario(input);
            }
        };
        var reply3 = adapter3.apply(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBankTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteBankTransferReport", request.getBankTransferId());
        return DeleteBankTransferReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }
}
