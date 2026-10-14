package com.acme.finance.domains.assets;

import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.admissions.v1.WatchAdmissionsRequest;
import com.acme.admissions.v1.WatchAdmissionsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DeleteAssetTransferReportRequest;
import com.acme.assets.v1.DeleteAssetTransferReportResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.FetchAssetTransferReportRequest;
import com.acme.assets.v1.FetchAssetTransferReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.GenerateAssetTransferReportRequest;
import com.acme.assets.v1.GenerateAssetTransferReportResponse;
import com.acme.assets.v1.GetAssetTransferRulesRequest;
import com.acme.assets.v1.GetAssetTransferRulesResponse;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.ListAssetTransferReportsRequest;
import com.acme.assets.v1.ListAssetTransferReportsResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.ShareAssetTransferReportRequest;
import com.acme.assets.v1.ShareAssetTransferReportResponse;
import com.acme.assets.v1.StreamAssetTransferMetricsRequest;
import com.acme.assets.v1.StreamAssetTransferMetricsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.experiments.v1.EstimateExperimentRequest;
import com.acme.experiments.v1.EstimateExperimentResponse;
import com.acme.experiments.v1.Experiment;
import com.acme.experiments.v1.ExperimentCalculationServiceGrpc;
import com.acme.experiments.v1.ExperimentParticipant;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.modelregistry.v1.SimulateRegisteredModelRequest;
import com.acme.modelregistry.v1.SimulateRegisteredModelResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.scheduling.v1.SuggestShiftsRequest;
import com.acme.scheduling.v1.SuggestShiftsResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about asset transfers. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class AssetTransferReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferReportingApplication.class);

    private final AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking;

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceFutureStub assetsAssetTransferSyncServiceFuture;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking;

    private final FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking;

    private final AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking;

    private final ExperimentCalculationServiceGrpc.ExperimentCalculationServiceBlockingStub experimentsExperimentCalculationServiceBlocking;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking;

    private final FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking;

    private final LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking;

    private final CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking;

    private final RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking;

    private final AssetsRepository repository;

    private final ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking;

    public AssetTransferReportingApplication(
            AdmissionServiceGrpc.AdmissionServiceBlockingStub admissionsAdmissionServiceBlocking,
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceFutureStub assetsAssetTransferSyncServiceFuture,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationCalculationServiceGrpc.DepreciationCalculationServiceBlockingStub assetsDepreciationCalculationServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceBlockingStub assetsDisposalSyncServiceBlocking,
            FixedAssetLifecycleServiceGrpc.FixedAssetLifecycleServiceBlockingStub assetsFixedAssetLifecycleServiceBlocking,
            AuditTrailAdminServiceGrpc.AuditTrailAdminServiceBlockingStub auditAuditTrailAdminServiceBlocking,
            ExperimentCalculationServiceGrpc.ExperimentCalculationServiceBlockingStub experimentsExperimentCalculationServiceBlocking,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            PremiumSyncServiceGrpc.PremiumSyncServiceBlockingStub insurancePremiumSyncServiceBlocking,
            FiscalPeriodHistoryServiceGrpc.FiscalPeriodHistoryServiceBlockingStub ledgerFiscalPeriodHistoryServiceBlocking,
            LedgerAccountQueryServiceGrpc.LedgerAccountQueryServiceBlockingStub ledgerLedgerAccountQueryServiceBlocking,
            CollateralWorkflowServiceGrpc.CollateralWorkflowServiceBlockingStub lendingCollateralWorkflowServiceBlocking,
            RegisteredModelCalculationServiceGrpc.RegisteredModelCalculationServiceBlockingStub modelregistryRegisteredModelCalculationServiceBlocking,
            AssetsRepository repository,
            ShiftQueryServiceGrpc.ShiftQueryServiceBlockingStub schedulingShiftQueryServiceBlocking) {
        this.admissionsAdmissionServiceBlocking = admissionsAdmissionServiceBlocking;
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsAssetTransferSyncServiceFuture = assetsAssetTransferSyncServiceFuture;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationCalculationServiceBlocking = assetsDepreciationCalculationServiceBlocking;
        this.assetsDisposalSyncServiceBlocking = assetsDisposalSyncServiceBlocking;
        this.assetsFixedAssetLifecycleServiceBlocking = assetsFixedAssetLifecycleServiceBlocking;
        this.auditAuditTrailAdminServiceBlocking = auditAuditTrailAdminServiceBlocking;
        this.experimentsExperimentCalculationServiceBlocking = experimentsExperimentCalculationServiceBlocking;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insurancePremiumSyncServiceBlocking = insurancePremiumSyncServiceBlocking;
        this.ledgerFiscalPeriodHistoryServiceBlocking = ledgerFiscalPeriodHistoryServiceBlocking;
        this.ledgerLedgerAccountQueryServiceBlocking = ledgerLedgerAccountQueryServiceBlocking;
        this.lendingCollateralWorkflowServiceBlocking = lendingCollateralWorkflowServiceBlocking;
        this.modelregistryRegisteredModelCalculationServiceBlocking = modelregistryRegisteredModelCalculationServiceBlocking;
        this.repository = repository;
        this.schedulingShiftQueryServiceBlocking = schedulingShiftQueryServiceBlocking;
    }

    /**
     * Generates an asset transfer.
     */
    public GenerateAssetTransferReportResponse generateAssetTransferReport(GenerateAssetTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateAssetTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GenerateAssetTransferReport");

        var outbound0 = SuggestShiftsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestShiftsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getContext().getDeadline())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getAssetTransferId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.scheduling.v1.ShiftQueryService/SuggestShifts", outbound0);
        var reply0 = schedulingShiftQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestShifts(command0.payload());

        var outbound1 = BeanCopies.copy(GetAssetTransferSyncStateRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransferId())
                .setContext(request.getContext())
                .build(), GetAssetTransferSyncStateRequest.class);
        var reply1 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAssetTransferSyncState(outbound1);

        var outbound2 = ResolveLedgerAccountsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        var reply2 = Stream.of(outbound2).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(ledgerLedgerAccountQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveLedgerAccounts(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound3 = RepairAuditTrailRequest.newBuilder()
                .setAuditTrail(AuditTrail.newBuilder()
                        .setBank(AuditTrailBankSettlement.newBuilder()
                                .setAccountHolder(switch (reply1.getAssetTransfer().getSettlementCase()) {
                                    case BANK -> reply1.getAssetTransfer().getBank().getAccountHolder();
                                    default -> "";
                                })
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getAssetTransfer().getBillingAddress().getCountryCode())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(RepairAuditTrailRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply3 = auditAuditTrailAdminServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).repairAuditTrail(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateAssetTransferReport", request.getDetails().getFormat());
        return GenerateAssetTransferReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .setStatus(reply2.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public FetchAssetTransferReportResponse fetchAssetTransferReport(FetchAssetTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchAssetTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "FetchAssetTransferReport");
        return FetchAssetTransferReportResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .build();
    }

    /**
     * Schedules an asset transfer.
     */
    public ScheduleAssetTransferReportResponse scheduleAssetTransferReport(ScheduleAssetTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleAssetTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "ScheduleAssetTransferReport");

        var outbound0 = ScheduleFixedAssetExpiryRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ScheduleFixedAssetExpiryRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setUserId(request.getContext().getActor().getUserId())
                                .build())
                        .build())
                .setReason(request.getReason())
                .build();
        var command0 = new DispatchCommand<>("acme.assets.v1.FixedAssetLifecycleService/ScheduleFixedAssetExpiry", outbound0);
        var reply0 = assetsFixedAssetLifecycleServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleFixedAssetExpiry(command0.payload());

        var outbound1 = ResolvePremiumConflictRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ResolvePremiumConflictRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setPremium(Premium.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getFixedAsset().getAudit().getCreatedBy())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getCountryCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .build();
        var reply1 = insurancePremiumSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolvePremiumConflict(outbound1);

        var outbound2 = RpcSupport.unpack(Any.pack(ReconcileDisposalsRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ReconcileDisposalsRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(reply1.getPremium().getCounterparty().getDisplayName())
                                .build())
                        .build())
                .build()), ReconcileDisposalsRequest.class);
        var reply2 = assetsDisposalSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileDisposals(outbound2);

        var outbound3 = EstimateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(switch (reply0.getFixedAsset().getLocatorCase()) {
                                    case ADDRESS -> reply0.getFixedAsset().getAddress().getRecipient();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAssetTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .addAllLines(reply0.getFixedAsset().getAddress().getLinesList())
                                .setLocality(reply1.getPremium().getBillingAddress().getLocality())
                                .setRecipient(reply1.getPremium().getBillingAddress().getRecipient())
                                .build())
                        .build())
                .build();
        var reply3 = assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAssetTransfer(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleAssetTransferReport", request.getAssetTransferId());
        return ScheduleAssetTransferReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply2.getResult().getCorrelationId())
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply0.getOutcome().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Streams the changes of asset transfers as they happen.
     */
    public StreamAssetTransferMetricsResponse streamAssetTransferMetrics(StreamAssetTransferMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamAssetTransferMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "StreamAssetTransferMetrics");

        var outbound0 = RecordFiscalPeriodVersionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RecordFiscalPeriodVersionRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setFiscalPeriod(FiscalPeriod.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerFiscalPeriodHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recordFiscalPeriodVersion(outbound0);

        var outbound1 = ReconcileAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getFiscalPeriod().getAudit().getEtag())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var pending1 = assetsAssetTransferSyncServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAssetTransfers(outbound1);
        var reply1 = RpcSupport.await(pending1);

        var outbound2 = WatchAdmissionsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(WatchAdmissionsRequest.Filter.newBuilder()
                        .setRegion(reply0.getFiscalPeriod().getLocation().getRegion())
                        .build())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                        .setPhone(request.getSubscriber().getPhone())
                        .build())
                .build();
        var stream2 = admissionsAdmissionServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchAdmissions(outbound2);
        var reply2 = WatchAdmissionsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = RpcSupport.unpack(Any.pack(EstimateExperimentRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(EstimateExperimentRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setLocality(reply1.getAssetTransfer().getBillingAddress().getLocality())
                                .build())
                        .build())
                .setExperiment(Experiment.newBuilder()
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(reply0.getFiscalPeriod().getOrganizer().getEmail())
                                .build())
                        .addParticipants(ExperimentParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build()), EstimateExperimentRequest.class);
        var reply3 = experimentsExperimentCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateExperiment(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamAssetTransferMetrics", request.getFilter().getQuery());
        return StreamAssetTransferMetricsResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .addAllLines(reply1.getAssetTransfer().getBillingAddress().getLinesList())
                                .setLocality(reply0.getFiscalPeriod().getLocation().getLocality())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setIssuedAt(reply1.getAssetTransfer().getIssuedAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply3.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Lists asset transfers a page at a time.
     */
    public ListAssetTransferReportsResponse listAssetTransferReports(ListAssetTransferReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListAssetTransferReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "ListAssetTransferReports");

        var outbound0 = ListCollateralWorkflowsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListCollateralWorkflowsRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .build())
                .build();
        var adapter0 = new Function<ListCollateralWorkflowsRequest, ListCollateralWorkflowsResponse>() {
            @Override
            public ListCollateralWorkflowsResponse apply(ListCollateralWorkflowsRequest input) {
                return lendingCollateralWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listCollateralWorkflows(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListAssetTransferReports", request.getPage().getPageToken());
        return ListAssetTransferReportsResponse.newBuilder()
                .setPageInfo(PageInfo.newBuilder()
                        .setTotalSize(reply0.getPageInfo().getTotalSize())
                        .build())
                .build();
    }

    /**
     * Shares an asset transfer.
     */
    public ShareAssetTransferReportResponse shareAssetTransferReport(ShareAssetTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareAssetTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "ShareAssetTransferReport");

        var outbound0 = CloneUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setRelationshipManager(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneUser(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = (GetAssetTransferRulesRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransferId())
                .setContext(request.getContext())
                .build()).toBuilder().build();
        var reply1 = assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAssetTransferRules(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getRecipient().getPhone()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareAssetTransferReport", request.getDetails().getRecipient().getPhone());
        return ShareAssetTransferReportResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getAssetTransfer().getAudit().getCreatedAt())
                                .setUpdatedAt(reply0.getUser().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .build();
    }

    /**
     * Counts asset transfers by status.
     */
    public CountAssetTransferTotalsResponse countAssetTransferTotals(CountAssetTransferTotalsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "CountAssetTransferTotals")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getContext().getTraceId(), "CountAssetTransferTotals");

        var outbound0 = ReconcileAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ReconcileAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.assets.v1.AssetTransferSyncService/ReconcileAssetTransfers", outbound0);
        var reply0 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).reconcileAssetTransfers(command0.payload());

        var outbound1 = CalculateDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getAssetTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(reply0.getAssetTransfer().getBillingAddress().getLocality())
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setDetails(CalculateDepreciationRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setPostalCode(reply0.getAssetTransfer().getBillingAddress().getPostalCode())
                                .build())
                        .build())
                .build();
        var reply1 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).calculateDepreciation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "CountAssetTransferTotals", request.getPage().getPageToken());
        return CountAssetTransferTotalsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }

    /**
     * Deletes an asset transfer.
     */
    public DeleteAssetTransferReportResponse deleteAssetTransferReport(DeleteAssetTransferReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteAssetTransferReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "DeleteAssetTransferReport");

        var outbound0 = SimulateRegisteredModelRequest.newBuilder()
                .setContext(request.getContext())
                .setReason(request.getReason())
                .setRegisteredModel(RegisteredModel.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(request.getEtag())
                                .build())
                        .build())
                .build();
        var reply0 = Stream.of(outbound0).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(modelregistryRegisteredModelCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).simulateRegisteredModel(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound1 = ExplainDepreciationCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getRegisteredModel().getAudit().getUpdatedAt())
                                .build())
                        .setLocation(PostalAddress.newBuilder()
                                .setLocality(switch (reply0.getRegisteredModel().getLocatorCase()) {
                                    case ADDRESS -> reply0.getRegisteredModel().getAddress().getLocality();
                                    default -> "";
                                })
                                .setRegion(switch (reply0.getRegisteredModel().getLocatorCase()) {
                                    case ADDRESS -> reply0.getRegisteredModel().getAddress().getRegion();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setDetails(ExplainDepreciationCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply0.getRegisteredModel().getOwner().getEmail())
                                .build())
                        .build())
                .build();
        var reply1 = assetsDepreciationCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainDepreciationCalculation(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getAssetTransferId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteAssetTransferReport", request.getAssetTransferId());
        return DeleteAssetTransferReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
