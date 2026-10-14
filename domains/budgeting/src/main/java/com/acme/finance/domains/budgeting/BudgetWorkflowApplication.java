package com.acme.finance.domains.budgeting;

import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.budgeting.v1.ActivateForecastRequest;
import com.acme.budgeting.v1.ActivateForecastResponse;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowRequest;
import com.acme.budgeting.v1.AdvanceBudgetWorkflowResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineApprovalServiceGrpc;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineCalculationServiceGrpc;
import com.acme.budgeting.v1.BudgetLineCardSettlement;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineQueryServiceGrpc;
import com.acme.budgeting.v1.BudgetLineReportingServiceGrpc;
import com.acme.budgeting.v1.CancelBudgetWorkflowRequest;
import com.acme.budgeting.v1.CancelBudgetWorkflowResponse;
import com.acme.budgeting.v1.CountBudgetLineTotalsRequest;
import com.acme.budgeting.v1.CountBudgetLineTotalsResponse;
import com.acme.budgeting.v1.EstimateBudgetLineRequest;
import com.acme.budgeting.v1.EstimateBudgetLineResponse;
import com.acme.budgeting.v1.ExplainForecastDecisionRequest;
import com.acme.budgeting.v1.ExplainForecastDecisionResponse;
import com.acme.budgeting.v1.ExportBudgetLinesRequest;
import com.acme.budgeting.v1.ExportBudgetLinesResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastLifecycleServiceGrpc;
import com.acme.budgeting.v1.ForecastRef;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.ForecastValidationServiceGrpc;
import com.acme.budgeting.v1.GenerateVarianceReportRequest;
import com.acme.budgeting.v1.GenerateVarianceReportResponse;
import com.acme.budgeting.v1.GetForecastSyncStateRequest;
import com.acme.budgeting.v1.GetForecastSyncStateResponse;
import com.acme.budgeting.v1.InspectBudgetWorkflowRequest;
import com.acme.budgeting.v1.InspectBudgetWorkflowResponse;
import com.acme.budgeting.v1.ListBudgetLineReportsRequest;
import com.acme.budgeting.v1.ListBudgetLineReportsResponse;
import com.acme.budgeting.v1.ListBudgetWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetWorkflowsResponse;
import com.acme.budgeting.v1.PauseBudgetWorkflowRequest;
import com.acme.budgeting.v1.PauseBudgetWorkflowResponse;
import com.acme.budgeting.v1.RecallBudgetLineRequest;
import com.acme.budgeting.v1.RecallBudgetLineResponse;
import com.acme.budgeting.v1.ResumeBudgetWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetWorkflowResponse;
import com.acme.budgeting.v1.RetryBudgetWorkflowRequest;
import com.acme.budgeting.v1.RetryBudgetWorkflowResponse;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.budgeting.v1.ScheduleVarianceReportRequest;
import com.acme.budgeting.v1.ScheduleVarianceReportResponse;
import com.acme.budgeting.v1.ShareBudgetLineReportRequest;
import com.acme.budgeting.v1.ShareBudgetLineReportResponse;
import com.acme.budgeting.v1.ShareVarianceReportRequest;
import com.acme.budgeting.v1.ShareVarianceReportResponse;
import com.acme.budgeting.v1.SignalBudgetWorkflowRequest;
import com.acme.budgeting.v1.SignalBudgetWorkflowResponse;
import com.acme.budgeting.v1.StartBudgetWorkflowRequest;
import com.acme.budgeting.v1.StartBudgetWorkflowResponse;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.BrowseFeatureFlagsRequest;
import com.acme.featureflags.v1.BrowseFeatureFlagsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CountCoverageTotalsRequest;
import com.acme.insurance.v1.CountCoverageTotalsResponse;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ListClaimWorkflowsRequest;
import com.acme.insurance.v1.ListClaimWorkflowsResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRiskDecisionResponse;
import com.acme.suppliers.v1.SummarizeSuppliersRequest;
import com.acme.suppliers.v1.SummarizeSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.travel.v1.Trip;
import com.acme.travel.v1.TripCardSettlement;
import com.acme.travel.v1.TripServiceGrpc;
import com.acme.travel.v1.UpdateTripRequest;
import com.acme.travel.v1.UpdateTripResponse;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Runs the long-lived workflow that takes a budget from start to finish. Coordinates validation, durable audit
 * writes, and downstream operations.
 */
@Service
public class BudgetWorkflowApplication {
    private static final Logger log = LoggerFactory.getLogger(BudgetWorkflowApplication.class);

    private final FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking;

    private final BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking;

    private final BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking;

    private final BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceBlockingStub budgetingBudgetLineLifecycleServiceBlocking;

    private final BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking;

    private final BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking;

    private final ForecastLifecycleServiceGrpc.ForecastLifecycleServiceBlockingStub budgetingForecastLifecycleServiceBlocking;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking;

    private final BudgetWorkflowRequestMapper mapper;

    private final BudgetingRepository repository;

    private final Executor rpcExecutor;

    private final SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking;

    private final SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking;

    private final TripServiceGrpc.TripServiceBlockingStub travelTripServiceBlocking;

    public BudgetWorkflowApplication(
            FixedAssetApprovalServiceGrpc.FixedAssetApprovalServiceBlockingStub assetsFixedAssetApprovalServiceBlocking,
            BudgetLineApprovalServiceGrpc.BudgetLineApprovalServiceBlockingStub budgetingBudgetLineApprovalServiceBlocking,
            BudgetLineCalculationServiceGrpc.BudgetLineCalculationServiceBlockingStub budgetingBudgetLineCalculationServiceBlocking,
            BudgetLineLifecycleServiceGrpc.BudgetLineLifecycleServiceBlockingStub budgetingBudgetLineLifecycleServiceBlocking,
            BudgetLineQueryServiceGrpc.BudgetLineQueryServiceBlockingStub budgetingBudgetLineQueryServiceBlocking,
            BudgetLineReportingServiceGrpc.BudgetLineReportingServiceBlockingStub budgetingBudgetLineReportingServiceBlocking,
            ForecastLifecycleServiceGrpc.ForecastLifecycleServiceBlockingStub budgetingForecastLifecycleServiceBlocking,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            LedgerAccountValidationServiceGrpc.LedgerAccountValidationServiceBlockingStub ledgerLedgerAccountValidationServiceBlocking,
            BudgetWorkflowRequestMapper mapper,
            BudgetingRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierQueryServiceGrpc.SupplierQueryServiceBlockingStub suppliersSupplierQueryServiceBlocking,
            SupplierRiskValidationServiceGrpc.SupplierRiskValidationServiceBlockingStub suppliersSupplierRiskValidationServiceBlocking,
            TripServiceGrpc.TripServiceBlockingStub travelTripServiceBlocking) {
        this.assetsFixedAssetApprovalServiceBlocking = assetsFixedAssetApprovalServiceBlocking;
        this.budgetingBudgetLineApprovalServiceBlocking = budgetingBudgetLineApprovalServiceBlocking;
        this.budgetingBudgetLineCalculationServiceBlocking = budgetingBudgetLineCalculationServiceBlocking;
        this.budgetingBudgetLineLifecycleServiceBlocking = budgetingBudgetLineLifecycleServiceBlocking;
        this.budgetingBudgetLineQueryServiceBlocking = budgetingBudgetLineQueryServiceBlocking;
        this.budgetingBudgetLineReportingServiceBlocking = budgetingBudgetLineReportingServiceBlocking;
        this.budgetingForecastLifecycleServiceBlocking = budgetingForecastLifecycleServiceBlocking;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.ledgerLedgerAccountValidationServiceBlocking = ledgerLedgerAccountValidationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierQueryServiceBlocking = suppliersSupplierQueryServiceBlocking;
        this.suppliersSupplierRiskValidationServiceBlocking = suppliersSupplierRiskValidationServiceBlocking;
        this.travelTripServiceBlocking = travelTripServiceBlocking;
    }

    public record AdvanceBudgetWorkflowStage0Requested(CountBudgetLineTotalsRequest request, CompletableFuture<CountBudgetLineTotalsResponse> completion, io.grpc.Context context) {}

    public record SignalBudgetWorkflowStage0Requested(BrowseFeatureFlagsRequest request, CompletableFuture<BrowseFeatureFlagsResponse> completion, io.grpc.Context context) {}

    /**
     * Starts a budget.
     */
    public StartBudgetWorkflowResponse startBudgetWorkflow(StartBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StartBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = SummarizeSuppliersRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(SummarizeSuppliersRequest.Details.newBuilder()
                        .putAllFilters(request.getBudget().getLabelsMap())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage0Adapter {
            SummarizeSuppliersResponse fetch(SummarizeSuppliersRequest input) {
                return suppliersSupplierQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSuppliers(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = GenerateVarianceReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateVarianceReportRequest.Details.newBuilder()
                        .setPeriod(reply0.getPeriod())
                        .build())
                .build();
        Function<GenerateVarianceReportRequest, GenerateVarianceReportResponse> operation1 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::generateVarianceReport;
        var reply1 = operation1.apply(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StartBudgetWorkflow", request.getBudget().getBudgetId());
        return StartBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Advances a budget.
     */
    public AdvanceBudgetWorkflowResponse advanceBudgetWorkflow(AdvanceBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "AdvanceBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = CountBudgetLineTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountBudgetLineTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        var completion0 = new CompletableFuture<CountBudgetLineTotalsResponse>();
        events.publishEvent(new AdvanceBudgetWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound1 = RpcSupport.unpack(Any.pack(ShareBudgetLineReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareBudgetLineReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setName(request.getBudget().getContact().getName())
                                .setPhone(request.getBudget().getContact().getPhone())
                                .setPreferredChannel(request.getBudget().getContact().getPreferredChannel())
                                .build())
                        .build())
                .build()), ShareBudgetLineReportRequest.class);
        var reply1 = budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareBudgetLineReport(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "AdvanceBudgetWorkflow", request.getBudget().getBudgetId());
        return AdvanceBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setOutcome(AdvanceBudgetWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getOutcome().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Cancels a budget.
     */
    public CancelBudgetWorkflowResponse cancelBudgetWorkflow(CancelBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CancelBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = BeanCopies.copy(ShareVarianceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareVarianceReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .setPhone(request.getDetails().getNotify().getPhone())
                                .build())
                        .build())
                .build(), ShareVarianceReportRequest.class);
        var reply0 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareVarianceReport(outbound0);

        var outbound1 = ScheduleBudgetLineExpiryRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getVariance().getAudit().getCreatedBy())
                                .setEtag(reply0.getVariance().getAudit().getEtag())
                                .setUpdatedBy(request.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setBank(BudgetLineBankSettlement.newBuilder()
                                .setIban(request.getBudget().getBank().getIban())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        RpcPort<ScheduleBudgetLineExpiryRequest, ScheduleBudgetLineExpiryResponse> port1 = RpcPort.adapt(budgetingBudgetLineLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::scheduleBudgetLineExpiry);
        var reply1 = port1.exchange(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CancelBudgetWorkflow", request.getBudget().getBudgetId());
        return CancelBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBalance(request.getBudget().getBalance())
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getVariance().getAssignee().getEmail())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply1.getBudgetLine().getNumber())
                                .build())
                        .build())
                .setOutcome(CancelBudgetWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Retrys a budget.
     */
    public RetryBudgetWorkflowResponse retryBudgetWorkflow(RetryBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RetryBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ListBudgetLineReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListBudgetLineReportsRequest.Filter.newBuilder()
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        RpcPort<ListBudgetLineReportsRequest, ListBudgetLineReportsResponse> port0 = RpcPort.adapt(budgetingBudgetLineReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::listBudgetLineReports);
        var reply0 = port0.exchange(outbound0);

        var outbound1 = RecallBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getBudget().getAudit().getEtag())
                                .build())
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setHolderName(switch (request.getBudget().getFundingCase()) {
                                    case CARD -> request.getBudget().getCard().getHolderName();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RecallBudgetLineRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = budgetingBudgetLineApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recallBudgetLine(outbound1);

        var outbound2 = BeanCopies.copy(ActivateForecastRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ActivateForecastRequest.Details.newBuilder()
                        .setNotifyContact(ContactInfo.newBuilder()
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .setForecastId(request.getBudget().getHolder().getForecastId())
                .build(), ActivateForecastRequest.class);
        var reply2 = budgetingForecastLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).activateForecast(outbound2);

        var outbound3 = UpdateTripRequest.newBuilder()
                .setContext(request.getContext())
                .setTrip(Trip.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(request.getBudget().getAudit().getUpdatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply2.getForecast().getBillingAddress().getLinesList())
                                .build())
                        .setCard(TripCardSettlement.newBuilder()
                                .setLast4(switch (reply1.getBudgetLine().getSettlementCase()) {
                                    case CARD -> reply1.getBudgetLine().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply3 = travelTripServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).updateTrip(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RetryBudgetWorkflow", request.getBudget().getBudgetId());
        return RetryBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setAvailable(request.getBudget().getAvailable())
                        .setBudgetId(request.getBudgetId())
                        .build())
                .setOutcome(RetryBudgetWorkflowResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Pauses a budget.
     */
    public PauseBudgetWorkflowResponse pauseBudgetWorkflow(PauseBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "PauseBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ScheduleVarianceReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var adapter0 = new Function<ScheduleVarianceReportRequest, ScheduleVarianceReportResponse>() {
            @Override
            public ScheduleVarianceReportResponse apply(ScheduleVarianceReportRequest input) {
                return budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleVarianceReport(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = ListClaimWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListClaimWorkflowsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getUpdatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.insurance.v1.ClaimWorkflowService");
        var serviceName1 = "acme.insurance.v1" + "." + "ClaimWorkflowService";
        var methodName1 = "Lis" + "tClaimWorkflows";
        var descriptor1 = MethodDescriptor.<ListClaimWorkflowsRequest, ListClaimWorkflowsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(ListClaimWorkflowsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListClaimWorkflowsResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);

        var outbound2 = (ExplainSupplierRiskDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainSupplierRiskDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotify().getEmail())
                                .setName(request.getDetails().getNotify().getName())
                                .setPhone(request.getBudget().getContact().getPhone())
                                .build())
                        .build())
                .setSupplierRisk(SupplierRisk.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getBudget().getContact().getEmail())
                                .build())
                        .build())
                .build()).toBuilder().build();
        var reply2 = suppliersSupplierRiskValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSupplierRiskDecision(outbound2);

        var builder3 = GetForecastSyncStateRequest.newBuilder();
        builder3.setContext(request.getContext());
        builder3.setForecastId(request.getBudget().getHolder().getForecastId());
        var outbound3 = builder3.build();
        var reply3 = budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getForecastSyncState(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "PauseBudgetWorkflow", request.getBudget().getBudgetId());
        return PauseBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setCurrencyCode(reply3.getForecast().getCounterparty().getCurrencyCode())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply3.getForecast().getNumber())
                                .build())
                        .setOpenedAt(request.getBudget().getOpenedAt())
                        .build())
                .setOutcome(PauseBudgetWorkflowResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Resumes a budget.
     */
    public ResumeBudgetWorkflowResponse resumeBudgetWorkflow(ResumeBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ResumeBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = ExportBudgetLinesRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportBudgetLinesRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getAudit().getCreatedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .build();
        var stream0 = budgetingBudgetLineQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportBudgetLines(outbound0);
        var reply0 = ExportBudgetLinesResponse.getDefaultInstance();
        while (stream0.hasNext()) {
            reply0 = stream0.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ResumeBudgetWorkflow", request.getBudget().getBudgetId());
        return ResumeBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setHolder(request.getBudget().getHolder())
                        .build())
                .build();
    }

    /**
     * Returns a budget.
     */
    public InspectBudgetWorkflowResponse inspectBudgetWorkflow(InspectBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "InspectBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getBudgetId(), "InspectBudgetWorkflow");

        var outbound0 = SubmitFixedAssetRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitFixedAssetRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(assetsFixedAssetApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitFixedAsset(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = mapper.inspectBudgetWorkflowStage1(request, reply0);
        var reply1 = ledgerLedgerAccountValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).normalizeLedgerAccount(outbound1);
        return InspectBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getFixedAsset().getOwner().getEmail())
                                .setName(reply1.getLedgerAccount().getContact().getName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists budgets a page at a time.
     */
    public ListBudgetWorkflowsResponse listBudgetWorkflows(ListBudgetWorkflowsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListBudgetWorkflows")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Budget", request.getContext().getTraceId(), "ListBudgetWorkflows");

        var outbound0 = ExplainForecastDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setForecast(Forecast.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .build();
        var channel0 = channels.lookup("acme.budgeting.v1.ForecastValidationService");
        var selected0 = ForecastValidationServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.explainForecastDecision(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListBudgetWorkflows", request.getPage().getPageToken());
        return ListBudgetWorkflowsResponse.newBuilder()
                .addBudgets(Budget.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getForecast().getAudit().getUpdatedAt())
                                .build())
                        .setHolder(ForecastRef.newBuilder()
                                .setNumber(reply0.getForecast().getNumber())
                                .build())
                        .build())
                .build();
    }

    /**
     * Signals a budget.
     */
    public SignalBudgetWorkflowResponse signalBudgetWorkflow(SignalBudgetWorkflowRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SignalBudgetWorkflow")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveBudget(request.getContext().getTenantId(), request.getBudget());

        var outbound0 = BrowseFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getBudget().getOpenedAt())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setOwnerId(request.getBudget().getHolder().getForecastId())
                        .build())
                .build();
        var completion0 = new CompletableFuture<BrowseFeatureFlagsResponse>();
        events.publishEvent(new SignalBudgetWorkflowStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();

        var futures1 = request.getBudget().getLimitsList().stream()
                .map(element -> EstimateBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(request.getBudget().getAudit().getCreatedBy())
                                .build())
                        .setCard(BudgetLineCardSettlement.newBuilder()
                                .setHolderName(request.getBudget().getCard().getHolderName())
                                .setNetwork(request.getBudget().getCard().getNetwork())
                                .build())
                        .addLines(BudgetLineLine.newBuilder()
                                .setTax(BudgetLineLineTax.newBuilder()
                                        .setAmount(element.getAmount())
                                        .build())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build())
                .map(value -> CompletableFuture.supplyAsync(() -> budgetingBudgetLineCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudgetLine(value), rpcExecutor))
                .toList();
        CompletableFuture.allOf(futures1.toArray(CompletableFuture[]::new)).join();
        var results1 = futures1.stream().map(CompletableFuture::join).toList();
        var reply1 = results1.isEmpty() ? EstimateBudgetLineResponse.getDefaultInstance()
                : results1.getLast();

        var outbound2 = CountCoverageTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountCoverageTotalsRequest.Filter.newBuilder()
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .setRegion(reply1.getBudgetLine().getBillingAddress().getRegion())
                        .build())
                .build();
        RpcPort<CountCoverageTotalsRequest, CountCoverageTotalsResponse> port2 = RpcPort.adapt(insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::countCoverageTotals);
        var reply2 = port2.exchange(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getBudget().getBudgetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SignalBudgetWorkflow", request.getBudget().getBudgetId());
        return SignalBudgetWorkflowResponse.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setBudgetId(request.getBudgetId())
                        .putAllLabels(request.getBudget().getLabelsMap())
                        .build())
                .setOutcome(SignalBudgetWorkflowResponse.Outcome.newBuilder()
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .build();
    }
}
