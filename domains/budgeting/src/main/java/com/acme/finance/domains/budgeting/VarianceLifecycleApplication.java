package com.acme.finance.domains.budgeting;

import com.acme.budgeting.v1.ActivateVarianceRequest;
import com.acme.budgeting.v1.ActivateVarianceResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCalculationServiceGrpc;
import com.acme.budgeting.v1.EstimateBudgetRequest;
import com.acme.budgeting.v1.EstimateBudgetResponse;
import com.acme.budgeting.v1.ExpireVarianceRequest;
import com.acme.budgeting.v1.ExpireVarianceResponse;
import com.acme.budgeting.v1.FetchVarianceReportRequest;
import com.acme.budgeting.v1.FetchVarianceReportResponse;
import com.acme.budgeting.v1.ForecastSyncServiceGrpc;
import com.acme.budgeting.v1.GetVarianceLifecycleRequest;
import com.acme.budgeting.v1.GetVarianceLifecycleResponse;
import com.acme.budgeting.v1.ListForecastConflictsRequest;
import com.acme.budgeting.v1.ListForecastConflictsResponse;
import com.acme.budgeting.v1.ListVarianceReportsRequest;
import com.acme.budgeting.v1.ListVarianceReportsResponse;
import com.acme.budgeting.v1.RenewVarianceRequest;
import com.acme.budgeting.v1.RenewVarianceResponse;
import com.acme.budgeting.v1.ScheduleVarianceExpiryRequest;
import com.acme.budgeting.v1.ScheduleVarianceExpiryResponse;
import com.acme.budgeting.v1.ScoreVarianceRequest;
import com.acme.budgeting.v1.ScoreVarianceResponse;
import com.acme.budgeting.v1.SuspendVarianceRequest;
import com.acme.budgeting.v1.SuspendVarianceResponse;
import com.acme.budgeting.v1.TerminateVarianceRequest;
import com.acme.budgeting.v1.TerminateVarianceResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceComment;
import com.acme.budgeting.v1.VarianceReportingServiceGrpc;
import com.acme.budgeting.v1.VarianceValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.ExportFeatureFlagsRequest;
import com.acme.featureflags.v1.ExportFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeJournalEntryUpdatesResponse;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowRequest;
import com.acme.risk.v1.AdvanceRiskRegisterWorkflowResponse;
import com.acme.risk.v1.ExposureWorkflowServiceGrpc;
import com.acme.risk.v1.PauseExposureWorkflowRequest;
import com.acme.risk.v1.PauseExposureWorkflowResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterWorkflowServiceGrpc;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.LintSupplierRatingsRequest;
import com.acme.suppliers.v1.LintSupplierRatingsResponse;
import com.acme.suppliers.v1.PreviewSupplierRatingRequest;
import com.acme.suppliers.v1.PreviewSupplierRatingResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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
 * Moves a variance between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class VarianceLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(VarianceLifecycleApplication.class);

    private final BudgetCalculationServiceGrpc.BudgetCalculationServiceFutureStub budgetingBudgetCalculationServiceFuture;

    private final ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking;

    private final VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking;

    private final VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking;

    private final ChannelRegistry channels;

    private final FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking;

    private final JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking;

    private final VarianceLifecycleRequestMapper mapper;

    private final BudgetingRepository repository;

    private final ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking;

    private final RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking;

    private final Executor rpcExecutor;

    private final SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking;

    private final SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture;

    public VarianceLifecycleApplication(
            BudgetCalculationServiceGrpc.BudgetCalculationServiceFutureStub budgetingBudgetCalculationServiceFuture,
            ForecastSyncServiceGrpc.ForecastSyncServiceBlockingStub budgetingForecastSyncServiceBlocking,
            VarianceReportingServiceGrpc.VarianceReportingServiceBlockingStub budgetingVarianceReportingServiceBlocking,
            VarianceValidationServiceGrpc.VarianceValidationServiceBlockingStub budgetingVarianceValidationServiceBlocking,
            ChannelRegistry channels,
            FeatureFlagQueryServiceGrpc.FeatureFlagQueryServiceBlockingStub featureflagsFeatureFlagQueryServiceBlocking,
            JournalEntryNotificationServiceGrpc.JournalEntryNotificationServiceBlockingStub ledgerJournalEntryNotificationServiceBlocking,
            VarianceLifecycleRequestMapper mapper,
            BudgetingRepository repository,
            ExposureWorkflowServiceGrpc.ExposureWorkflowServiceBlockingStub riskExposureWorkflowServiceBlocking,
            RiskRegisterWorkflowServiceGrpc.RiskRegisterWorkflowServiceBlockingStub riskRiskRegisterWorkflowServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            SupplierContractQueryServiceGrpc.SupplierContractQueryServiceBlockingStub suppliersSupplierContractQueryServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceBlockingStub suppliersSupplierRatingValidationServiceBlocking,
            SupplierRatingValidationServiceGrpc.SupplierRatingValidationServiceFutureStub suppliersSupplierRatingValidationServiceFuture) {
        this.budgetingBudgetCalculationServiceFuture = budgetingBudgetCalculationServiceFuture;
        this.budgetingForecastSyncServiceBlocking = budgetingForecastSyncServiceBlocking;
        this.budgetingVarianceReportingServiceBlocking = budgetingVarianceReportingServiceBlocking;
        this.budgetingVarianceValidationServiceBlocking = budgetingVarianceValidationServiceBlocking;
        this.channels = channels;
        this.featureflagsFeatureFlagQueryServiceBlocking = featureflagsFeatureFlagQueryServiceBlocking;
        this.ledgerJournalEntryNotificationServiceBlocking = ledgerJournalEntryNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskExposureWorkflowServiceBlocking = riskExposureWorkflowServiceBlocking;
        this.riskRiskRegisterWorkflowServiceBlocking = riskRiskRegisterWorkflowServiceBlocking;
        this.rpcExecutor = rpcExecutor;
        this.suppliersSupplierContractQueryServiceBlocking = suppliersSupplierContractQueryServiceBlocking;
        this.suppliersSupplierRatingValidationServiceBlocking = suppliersSupplierRatingValidationServiceBlocking;
        this.suppliersSupplierRatingValidationServiceFuture = suppliersSupplierRatingValidationServiceFuture;
    }

    /**
     * Activates a variance.
     */
    public ActivateVarianceResponse activateVariance(ActivateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "ActivateVariance");

        var outbound0 = mapper.activateVarianceStage0(request);
        var reply0 = riskRiskRegisterWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advanceRiskRegisterWorkflow(outbound0);

        var outbound1 = UnsubscribeJournalEntryUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(UnsubscribeJournalEntryUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> ledgerJournalEntryNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).unsubscribeJournalEntryUpdates(value)).orElseThrow();

        var outbound2 = JsonCodec.roundTrip(SummarizeSupplierContractsRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setReason(request.getReason())
                .build(), SummarizeSupplierContractsRequest.class);
        var reply2 = suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).summarizeSupplierContracts(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateVariance", request.getDetails().getReasonCode());
        return ActivateVarianceResponse.newBuilder()
                .setOutcome(ActivateVarianceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .addAllReferenceIds(reply2.getResult().getReferenceIdsList())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Suspends a variance.
     */
    public SuspendVarianceResponse suspendVariance(SuspendVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "SuspendVariance");

        var outbound0 = RpcSupport.unpack(Any.pack(FetchVarianceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setVarianceId(request.getVarianceId())
                .build()), FetchVarianceReportRequest.class);
        var reply0 = budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchVarianceReport(outbound0);

        var outbound1 = EstimateBudgetRequest.newBuilder()
                .setBudget(Budget.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        var pending1 = budgetingBudgetCalculationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).estimateBudget(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = ListVarianceReportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListVarianceReportsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getVariance().getAudit().getUpdatedAt())
                        .putAllLabels(reply1.getBudget().getLabelsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var channel2 = channels.lookup("acme.budgeting.v1.VarianceReportingService");
        var serviceName2 = "acme.budgeting.v1" + "." + "VarianceReportingService";
        var methodName2 = "Lis" + "tVarianceReports";
        var descriptor2 = MethodDescriptor.<ListVarianceReportsRequest, ListVarianceReportsResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName2, methodName2))
                .setRequestMarshaller(ProtoUtils.marshaller(ListVarianceReportsRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(ListVarianceReportsResponse.getDefaultInstance()))
                .build();
        var reply2 = ClientCalls.blockingUnaryCall(channel2, descriptor2,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendVariance", request.getDetails().getReasonCode());
        return SuspendVarianceResponse.newBuilder()
                .setOutcome(SuspendVarianceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply2.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getBudget().getContact().getEmail())
                                .build())
                        .addComments(VarianceComment.newBuilder()
                                .setCreatedAt(reply0.getVariance().getAudit().getCreatedAt())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setPhone(reply1.getBudget().getContact().getPhone())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Terminates a variance.
     */
    public TerminateVarianceResponse terminateVariance(TerminateVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "TerminateVariance");

        var outbound0 = LintSupplierRatingsRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var pending0 = suppliersSupplierRatingValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).lintSupplierRatings(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = ExplainSupplierRatingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getSupplierRating().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getSupplierRating().getBillingAddress().getRecipient())
                                .build())
                        .setCard(SupplierRatingCardSettlement.newBuilder()
                                .setLast4(switch (reply0.getSupplierRating().getSettlementCase()) {
                                    case CARD -> reply0.getSupplierRating().getCard().getLast4();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply1 = suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainSupplierRatingDecision(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateVariance", request.getDetails().getReasonCode());
        return TerminateVarianceResponse.newBuilder()
                .setOutcome(TerminateVarianceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getOutcome().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getSupplierRating().getCounterparty().getEmail())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Renews a variance.
     */
    public RenewVarianceResponse renewVariance(RenewVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "RenewVariance");

        var outbound0 = RpcSupport.unpack(Any.pack(PauseExposureWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PauseExposureWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build()), PauseExposureWorkflowRequest.class);
        var reply0 = riskExposureWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pauseExposureWorkflow(outbound0);

        var outbound1 = ExportFeatureFlagsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportFeatureFlagsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getExposure().getOpenedAt())
                        .putAllLabels(reply0.getExposure().getLabelsMap())
                        .build())
                .build();
        var stream1 = featureflagsFeatureFlagQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportFeatureFlags(outbound1);
        var reply1 = ExportFeatureFlagsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewVariance", request.getDetails().getReasonCode());
        return RenewVarianceResponse.newBuilder()
                .setOutcome(RenewVarianceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply0.getExposure().getContact().getEmail())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Expires a variance.
     */
    public ExpireVarianceResponse expireVariance(ExpireVarianceRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireVariance")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "ExpireVariance");

        var outbound0 = BrowseSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseSupplierContracts(outbound0), rpcExecutor);

        var outbound1 = FetchVarianceReportRequest.newBuilder()
                .setContext(request.getContext())
                .setVarianceId(request.getVarianceId())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingVarianceReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).fetchVarianceReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireVariance", request.getDetails().getReasonCode());
        return ExpireVarianceResponse.newBuilder()
                .setOutcome(ExpireVarianceResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(reply1.getVariance().getAssignee().getEmail())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply1.getVariance().getAudit().getEtag())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Returns a variance.
     */
    public GetVarianceLifecycleResponse getVarianceLifecycle(GetVarianceLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetVarianceLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Variance", request.getVarianceId(), "GetVarianceLifecycle");
        return GetVarianceLifecycleResponse.newBuilder()
                .setVariance(Variance.newBuilder()
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }

    /**
     * Schedules a variance.
     */
    public ScheduleVarianceExpiryResponse scheduleVarianceExpiry(ScheduleVarianceExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleVarianceExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveVariance(request.getContext().getTenantId(), request.getVariance());

        var outbound0 = SuggestSupplierContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestSupplierContractsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getVariance().getSla().getDueAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getVariance().getVarianceId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> suppliersSupplierContractQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestSupplierContracts(outbound0), rpcExecutor);

        var outbound1 = ScoreVarianceRequest.newBuilder()
                .setContext(request.getContext())
                .setVariance(Variance.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setEmail(request.getVariance().getAssignee().getEmail())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> budgetingVarianceValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scoreVariance(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = PreviewSupplierRatingRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PreviewSupplierRatingRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getVariance().getRequester().getEmail())
                                .build())
                        .build())
                .setSupplierRating(SupplierRating.newBuilder()
                        .setCounterparty(CustomerRef.newBuilder()
                                .setDisplayName(request.getVariance().getAssignee().getDisplayName())
                                .build())
                        .build())
                .build();
        RpcPort<PreviewSupplierRatingRequest, PreviewSupplierRatingResponse> port2 = RpcPort.adapt(suppliersSupplierRatingValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::previewSupplierRating);
        var reply2 = port2.exchange(outbound2);

        var outbound3 = ListForecastConflictsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListForecastConflictsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getVariance().getAudit().getCreatedAt())
                        .setRegion(reply2.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(budgetingForecastSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listForecastConflicts(value)))
                .collect(Collectors.toList()).getFirst();
        log.atDebug().addKeyValue("auditValue", request.getVariance().getSubject()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleVarianceExpiry", request.getVariance().getSubject());
        return ScheduleVarianceExpiryResponse.newBuilder()
                .setOutcome(ScheduleVarianceExpiryResponse.Outcome.newBuilder()
                        .setMessage(reply2.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setVariance(Variance.newBuilder()
                        .setAssignee(reply1.getVariance().getAssignee())
                        .putAllCustomFields(request.getVariance().getCustomFieldsMap())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(reply1.getVariance().getRequester().getName())
                                .build())
                        .setVarianceId(request.getVarianceId())
                        .build())
                .build();
    }
}
