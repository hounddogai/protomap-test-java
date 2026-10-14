package com.acme.finance.domains.assets;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.FindAppointmentsRequest;
import com.acme.appointments.v1.FindAppointmentsResponse;
import com.acme.assets.v1.ActivateFixedAssetRequest;
import com.acme.assets.v1.ActivateFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.ExpireFixedAssetRequest;
import com.acme.assets.v1.ExpireFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GenerateDepreciationReportRequest;
import com.acme.assets.v1.GenerateDepreciationReportResponse;
import com.acme.assets.v1.GetFixedAssetLifecycleRequest;
import com.acme.assets.v1.GetFixedAssetLifecycleResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.PreviewDepreciationRequest;
import com.acme.assets.v1.PreviewDepreciationResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.ReinstateFixedAssetRequest;
import com.acme.assets.v1.ReinstateFixedAssetResponse;
import com.acme.assets.v1.RenewFixedAssetRequest;
import com.acme.assets.v1.RenewFixedAssetResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.SimulateAssetTransferRequest;
import com.acme.assets.v1.SimulateAssetTransferResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.SuspendFixedAssetRequest;
import com.acme.assets.v1.SuspendFixedAssetResponse;
import com.acme.assets.v1.TerminateFixedAssetRequest;
import com.acme.assets.v1.TerminateFixedAssetResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyBlockAction;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.LintAccessPoliciesRequest;
import com.acme.authorization.v1.LintAccessPoliciesResponse;
import com.acme.banking.v1.StandingOrder;
import com.acme.banking.v1.StandingOrderApprovalServiceGrpc;
import com.acme.banking.v1.StandingOrderParticipant;
import com.acme.banking.v1.SubmitStandingOrderRequest;
import com.acme.banking.v1.SubmitStandingOrderResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.PeekUserRequest;
import com.acme.identity.v1.PeekUserResponse;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindLedgerAccountsRequest;
import com.acme.ledger.v1.FindLedgerAccountsResponse;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApplicationRef;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.lending.v1.RejectLoanRequest;
import com.acme.lending.v1.RejectLoanResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioApprovalServiceGrpc;
import com.acme.risk.v1.SubmitRiskScenarioRequest;
import com.acme.risk.v1.SubmitRiskScenarioResponse;
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
 * Moves a fixed asset between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class FixedAssetLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(FixedAssetLifecycleApplication.class);

    private final AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking;

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking;

    private final RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking;

    private final ApplicationEventPublisher events;

    private final UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking;

    private final FixedAssetLifecycleRequestMapper mapper;

    private final AssetsRepository repository;

    private final RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync;

    private final RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking;

    private final Executor rpcExecutor;

    public FixedAssetLifecycleApplication(
            AppointmentQueryServiceGrpc.AppointmentQueryServiceBlockingStub appointmentsAppointmentQueryServiceBlocking,
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            DepreciationReportingServiceGrpc.DepreciationReportingServiceBlockingStub assetsDepreciationReportingServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            FixedAssetValidationServiceGrpc.FixedAssetValidationServiceBlockingStub assetsFixedAssetValidationServiceBlocking,
            RetentionPolicySyncServiceGrpc.RetentionPolicySyncServiceStub auditRetentionPolicySyncServiceAsync,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            StandingOrderApprovalServiceGrpc.StandingOrderApprovalServiceBlockingStub bankingStandingOrderApprovalServiceBlocking,
            ApplicationEventPublisher events,
            UserQueryServiceGrpc.UserQueryServiceBlockingStub identityUserQueryServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            LoanApprovalServiceGrpc.LoanApprovalServiceBlockingStub lendingLoanApprovalServiceBlocking,
            FixedAssetLifecycleRequestMapper mapper,
            AssetsRepository repository,
            RiskRegisterServiceGrpc.RiskRegisterServiceStub riskRiskRegisterServiceAsync,
            RiskScenarioApprovalServiceGrpc.RiskScenarioApprovalServiceBlockingStub riskRiskScenarioApprovalServiceBlocking,
            @Qualifier("rpcExecutor") Executor rpcExecutor) {
        this.appointmentsAppointmentQueryServiceBlocking = appointmentsAppointmentQueryServiceBlocking;
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.assetsDepreciationReportingServiceBlocking = assetsDepreciationReportingServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.assetsFixedAssetValidationServiceBlocking = assetsFixedAssetValidationServiceBlocking;
        this.auditRetentionPolicySyncServiceAsync = auditRetentionPolicySyncServiceAsync;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.bankingStandingOrderApprovalServiceBlocking = bankingStandingOrderApprovalServiceBlocking;
        this.events = events;
        this.identityUserQueryServiceBlocking = identityUserQueryServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.lendingLoanApprovalServiceBlocking = lendingLoanApprovalServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.riskRiskRegisterServiceAsync = riskRiskRegisterServiceAsync;
        this.riskRiskScenarioApprovalServiceBlocking = riskRiskScenarioApprovalServiceBlocking;
        this.rpcExecutor = rpcExecutor;
    }

    public record GetFixedAssetLifecycleStage0Requested(PeekUserRequest request, CompletableFuture<PeekUserResponse> completion, io.grpc.Context context) {}

    /**
     * Activates a fixed asset.
     */
    public ActivateFixedAssetResponse activateFixedAsset(ActivateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "ActivateFixedAsset");

        var outbound0 = mapper.activateFixedAssetStage0(request);
        var reply0 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).searchFixedAssets(outbound0);

        var outbound1 = mapper.activateFixedAssetStage1(request);
        var reply1 = authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAccessPolicies(outbound1);

        var outbound2 = StreamDepreciationMetricsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setName(request.getDetails().getNotifyContact().getName())
                        .setPhone(request.getDetails().getNotifyContact().getPhone())
                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                        .build())
                .build();
        var stream2 = assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamDepreciationMetrics(outbound2);
        var reply2 = StreamDepreciationMetricsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = BatchCreateRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateRiskRegistersRequest.StreamHeader.newBuilder()
                        .setSource(request.getContext().getActor().getUserId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector3 = new StreamCollector<>(BatchCreateRiskRegistersResponse.getDefaultInstance());
        var input3 = riskRiskRegisterServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateRiskRegisters(collector3);
        input3.onNext(outbound3);
        input3.onCompleted();
        var reply3 = collector3.await();

        var outbound4 = StreamRetentionPolicyChangesRequest.newBuilder()
                .setChange(RetentionPolicy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .setBlock(RetentionPolicyBlockAction.newBuilder()
                                .setNotifyEmail(request.getContext().getActor().getEmail())
                                .setReason(request.getReason())
                                .build())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(StreamRetentionPolicyChangesResponse.getDefaultInstance());
        var input4 = auditRetentionPolicySyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamRetentionPolicyChanges(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateFixedAsset", request.getDetails().getReasonCode());
        return ActivateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setName(reply1.getAccessPolicy().getName())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(reply1.getAccessPolicy().getOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ActivateFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Suspends a fixed asset.
     */
    public SuspendFixedAssetResponse suspendFixedAsset(SuspendFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "SuspendFixedAsset");

        var outbound0 = GenerateDepreciationReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(GenerateDepreciationReportRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .setReason(request.getReason())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsDepreciationReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).generateDepreciationReport(outbound0), rpcExecutor);

        var outbound1 = FindLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindLedgerAccountsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findLedgerAccounts(outbound1), rpcExecutor);

        var outbound2 = RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending2 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(outbound2), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1, pending2).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        var reply2 = pending2.join();

        var outbound3 = ReconcileAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setLocality(switch (reply2.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply2.getFixedAsset().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRecipient(switch (reply2.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply2.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setFixedAssetId(reply2.getFixedAsset().getFixedAssetId())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply3 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAssetTransfers(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendFixedAsset", request.getFixedAssetId());
        return SuspendFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply2.getFixedAsset().getAudit().getEtag())
                                .build())
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setWeight(reply2.getFixedAsset().getDimensions().getWeight())
                                .build())
                        .setFixedAssetId(reply3.getAssetTransfer().getCounterparty().getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(SuspendFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates a fixed asset.
     */
    public ReinstateFixedAssetResponse reinstateFixedAsset(ReinstateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "ReinstateFixedAsset");

        var outbound0 = FindAppointmentsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindAppointmentsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> appointmentsAppointmentQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).findAppointments(value)).orElseThrow();

        var outbound1 = SubmitRiskScenarioRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubmitRiskScenarioRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setRiskScenario(RiskScenario.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .build();
        RpcPort<SubmitRiskScenarioRequest, SubmitRiskScenarioResponse> port1 = RpcPort.adapt(riskRiskScenarioApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS)::submitRiskScenario);
        var reply1 = port1.exchange(outbound1);

        var outbound2 = SimulateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRiskScenario().getAudit().getUpdatedBy())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(reply1.getRiskScenario().getAssignee().getDisplayName())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
        final class Stage2Adapter {
            SimulateAssetTransferResponse fetch(SimulateAssetTransferRequest input) {
                return assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateAssetTransfer(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateFixedAsset", request.getDetails().getReasonCode());
        return ReinstateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getRiskScenario().getAudit().getUpdatedBy())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(ReinstateFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Terminates a fixed asset.
     */
    public TerminateFixedAssetResponse terminateFixedAsset(TerminateFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "TerminateFixedAsset");

        var builder0 = PreviewDepreciationRequest.newBuilder();
        builder0.setContext(request.getContext());
        builder0.setDepreciation(Depreciation.newBuilder()
                .setOrganizer(UserRef.newBuilder()
                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                        .setUserId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .addParticipants(DepreciationParticipant.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build());
        var outbound0 = builder0.build();
        var reply0 = assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).previewDepreciation(outbound0);

        var outbound1 = ExportUsersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportUsersRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getDepreciation().getAudit().getUpdatedAt())
                        .setRegion(reply0.getDepreciation().getLocation().getRegion())
                        .build())
                .build();
        var stream1 = identityUserQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportUsers(outbound1);
        var reply1 = ExportUsersResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateFixedAsset", request.getDetails().getReasonCode());
        return TerminateFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .setOwner(UserRef.newBuilder()
                                .setEmail(reply0.getDepreciation().getOrganizer().getEmail())
                                .build())
                        .build())
                .setOutcome(TerminateFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .build();
    }

    /**
     * Renews a fixed asset.
     */
    public RenewFixedAssetResponse renewFixedAsset(RenewFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "RenewFixedAsset");

        var outbound0 = RejectLoanRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RejectLoanRequest.Details.newBuilder()
                        .setApprover(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .setLoan(Loan.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .build())
                        .setHolder(LoanApplicationRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(lendingLoanApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).rejectLoan(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = LintAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getLoan().getAudit().getCreatedBy())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(LintAssetTransfersRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getLoan().getContact().getEmail())
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply1 = RpcSupport.call(outbound1, value -> assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).lintAssetTransfers(value));
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewFixedAsset", request.getDetails().getReasonCode());
        return RenewFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(RenewFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .setReferenceId(reply0.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Expires a fixed asset.
     */
    public ExpireFixedAssetResponse expireFixedAsset(ExpireFixedAssetRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireFixedAsset")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "ExpireFixedAsset");

        var outbound0 = (BrowseDepreciationsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseDepreciationsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getFixedAssetId())
                        .build())
                .build()).toBuilder().build();
        var reply0 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseDepreciations(outbound0);

        var outbound1 = BeanCopies.copy(CheckFixedAssetEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(CheckFixedAssetEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build(), CheckFixedAssetEligibilityRequest.class);
        var reply1 = assetsFixedAssetValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkFixedAssetEligibility(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireFixedAsset", request.getDetails().getReasonCode());
        return ExpireFixedAssetResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setDimensions(FixedAssetDimensions.newBuilder()
                                .setLength(reply1.getFixedAsset().getDimensions().getLength())
                                .build())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(ExpireFixedAssetResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Returns a fixed asset.
     */
    public GetFixedAssetLifecycleResponse getFixedAssetLifecycle(GetFixedAssetLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetFixedAssetLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "FixedAsset", request.getFixedAssetId(), "GetFixedAssetLifecycle");

        var outbound0 = PeekUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUserId(request.getContext().getActor().getUserId())
                .build();
        var completion0 = new CompletableFuture<PeekUserResponse>();
        events.publishEvent(new GetFixedAssetLifecycleStage0Requested(outbound0, completion0, io.grpc.Context.current()));
        var reply0 = completion0.orTimeout(10, TimeUnit.SECONDS).join();
        return GetFixedAssetLifecycleResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Schedules a fixed asset.
     */
    public ScheduleFixedAssetExpiryResponse scheduleFixedAssetExpiry(ScheduleFixedAssetExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleFixedAssetExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveFixedAsset(request.getContext().getTenantId(), request.getFixedAsset());

        var outbound0 = SubmitStandingOrderRequest.newBuilder()
                .setContext(request.getContext())
                .setStandingOrder(StandingOrder.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setPostalCode(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getPostalCode();
                                    default -> "";
                                })
                                .setRegion(switch (request.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> request.getFixedAsset().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .addParticipants(StandingOrderParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(request.getDetails().getNotify().getName())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply0 = bankingStandingOrderApprovalServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).submitStandingOrder(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getFixedAsset().getFixedAssetId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleFixedAssetExpiry", request.getFixedAsset().getFixedAssetId());
        return ScheduleFixedAssetExpiryResponse.newBuilder()
                .setFixedAsset(FixedAsset.newBuilder()
                        .setCapacity(request.getFixedAsset().getCapacity())
                        .setFixedAssetId(request.getFixedAssetId())
                        .build())
                .setOutcome(ScheduleFixedAssetExpiryResponse.Outcome.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
