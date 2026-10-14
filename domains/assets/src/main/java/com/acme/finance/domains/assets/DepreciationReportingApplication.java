package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferBankSettlement;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.DeleteDepreciationReportRequest;
import com.acme.assets.v1.DeleteDepreciationReportResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.FetchDepreciationReportRequest;
import com.acme.assets.v1.FetchDepreciationReportResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.GenerateDepreciationReportRequest;
import com.acme.assets.v1.GenerateDepreciationReportResponse;
import com.acme.assets.v1.GetDepreciationRulesRequest;
import com.acme.assets.v1.GetDepreciationRulesResponse;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.assets.v1.PeekDepreciationRequest;
import com.acme.assets.v1.PeekDepreciationResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Builds and schedules reports about depreciations. Coordinates validation, durable audit writes, and downstream
 * operations.
 */
@Service
public class DepreciationReportingApplication {
    private static final Logger log = LoggerFactory.getLogger(DepreciationReportingApplication.class);

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking;

    private final DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking;

    private final DepreciationValidationServiceGrpc.DepreciationValidationServiceFutureStub assetsDepreciationValidationServiceFuture;

    private final FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking;

    private final AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking;

    private final ChannelRegistry channels;

    private final ApplicationEventPublisher events;

    private final UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking;

    private final CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking;

    private final FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking;

    private final LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking;

    private final DepreciationReportingRequestMapper mapper;

    private final AssetsRepository repository;

    public DepreciationReportingApplication(
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            AssetTransferValidationServiceGrpc.AssetTransferValidationServiceBlockingStub assetsAssetTransferValidationServiceBlocking,
            DepreciationQueryServiceGrpc.DepreciationQueryServiceBlockingStub assetsDepreciationQueryServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceBlockingStub assetsDepreciationValidationServiceBlocking,
            DepreciationValidationServiceGrpc.DepreciationValidationServiceFutureStub assetsDepreciationValidationServiceFuture,
            FixedAssetServiceGrpc.FixedAssetServiceBlockingStub assetsFixedAssetServiceBlocking,
            AuditTrailExportServiceGrpc.AuditTrailExportServiceBlockingStub auditAuditTrailExportServiceBlocking,
            ChannelRegistry channels,
            ApplicationEventPublisher events,
            UserServiceGrpc.UserServiceBlockingStub identityUserServiceBlocking,
            CoverageReportingServiceGrpc.CoverageReportingServiceBlockingStub insuranceCoverageReportingServiceBlocking,
            FiscalPeriodNotificationServiceGrpc.FiscalPeriodNotificationServiceBlockingStub ledgerFiscalPeriodNotificationServiceBlocking,
            LedgerAccountHistoryServiceGrpc.LedgerAccountHistoryServiceBlockingStub ledgerLedgerAccountHistoryServiceBlocking,
            DepreciationReportingRequestMapper mapper,
            AssetsRepository repository) {
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsAssetTransferValidationServiceBlocking = assetsAssetTransferValidationServiceBlocking;
        this.assetsDepreciationQueryServiceBlocking = assetsDepreciationQueryServiceBlocking;
        this.assetsDepreciationValidationServiceBlocking = assetsDepreciationValidationServiceBlocking;
        this.assetsDepreciationValidationServiceFuture = assetsDepreciationValidationServiceFuture;
        this.assetsFixedAssetServiceBlocking = assetsFixedAssetServiceBlocking;
        this.auditAuditTrailExportServiceBlocking = auditAuditTrailExportServiceBlocking;
        this.channels = channels;
        this.events = events;
        this.identityUserServiceBlocking = identityUserServiceBlocking;
        this.insuranceCoverageReportingServiceBlocking = insuranceCoverageReportingServiceBlocking;
        this.ledgerFiscalPeriodNotificationServiceBlocking = ledgerFiscalPeriodNotificationServiceBlocking;
        this.ledgerLedgerAccountHistoryServiceBlocking = ledgerLedgerAccountHistoryServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
    }

    public record ShareDepreciationReportStage1Requested(ScoreDepreciationRequest request, CompletableFuture<ScoreDepreciationResponse> completion, io.grpc.Context context) {}

    /**
     * Generates a depreciation.
     */
    public GenerateDepreciationReportResponse generateDepreciationReport(GenerateDepreciationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GenerateDepreciationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "GenerateDepreciationReport");

        var outbound0 = RpcSupport.unpack(Any.pack(PeekDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciationId(request.getDepreciationId())
                .build()), PeekDepreciationRequest.class);
        var reply0 = assetsDepreciationQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).peekDepreciation(outbound0);

        var outbound1 = mapper.generateDepreciationReportStage1(request);
        var reply1 = assetsDepreciationValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getDepreciationRules(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getFormat()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "GenerateDepreciationReport", request.getDetails().getFormat());
        return GenerateDepreciationReportResponse.newBuilder()
                .setPeriod(request.getDetails().getPeriod())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .setMessage(reply0.getResult().getMessage())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns a depreciation.
     */
    public FetchDepreciationReportResponse fetchDepreciationReport(FetchDepreciationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "FetchDepreciationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "FetchDepreciationReport");

        var outbound0 = RevertLedgerAccountRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(RevertLedgerAccountRequest.Details.newBuilder()
                        .setSnapshot(JsonCodec.encode(request.getContext()))
                        .build())
                .setLedgerAccount(LedgerAccount.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setExternalAccountId(request.getDepreciationId())
                        .setOpenedAt(request.getContext().getDeadline())
                        .build())
                .build();
        var reply0 = RpcSupport.call(outbound0, value -> ledgerLedgerAccountHistoryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).revertLedgerAccount(value));

        var outbound1 = PullAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply0.getLedgerAccount().getAudit().getCreatedAt())
                                .setEtag(reply0.getLedgerAccount().getAudit().getEtag())
                                .build())
                        .setBank(AssetTransferBankSettlement.newBuilder()
                                .setAccountHolder(reply0.getLedgerAccount().getBank().getAccountHolder())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply1 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAssetTransfers(outbound1);
        return FetchDepreciationReportResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(reply0.getLedgerAccount().getAudit().getUpdatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(UserRef.newBuilder()
                                .setDisplayName(reply0.getLedgerAccount().getHolder().getDisplayName())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply1.getOutcome().getMessage())
                        .build())
                .build();
    }

    /**
     * Streams the changes of depreciations as they happen.
     */
    public StreamDepreciationMetricsResponse streamDepreciationMetrics(StreamDepreciationMetricsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "StreamDepreciationMetrics")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "StreamDepreciationMetrics");

        var outbound0 = CheckDepreciationEligibilityRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setPreferredChannel(request.getSubscriber().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .setDetails(CheckDepreciationEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setPhone(request.getSubscriber().getPhone())
                                .build())
                        .build())
                .build();
        var pending0 = assetsDepreciationValidationServiceFuture.withDeadlineAfter(8, TimeUnit.SECONDS).checkDepreciationEligibility(outbound0);
        var reply0 = RpcSupport.await(pending0);

        var outbound1 = QuoteAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRecipient(reply0.getDepreciation().getLocation().getRecipient())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(QuoteAssetTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setCountryCode(reply0.getDepreciation().getLocation().getCountryCode())
                                .build())
                        .build())
                .build();
        var channel1 = channels.lookup("acme.assets.v1.AssetTransferCalculationService");
        var serviceName1 = "acme.assets.v1" + "." + "AssetTransferCalculationService";
        var methodName1 = "Quo" + "teAssetTransfer";
        var descriptor1 = MethodDescriptor.<QuoteAssetTransferRequest, QuoteAssetTransferResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName1, methodName1))
                .setRequestMarshaller(ProtoUtils.marshaller(QuoteAssetTransferRequest.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(QuoteAssetTransferResponse.getDefaultInstance()))
                .build();
        var reply1 = ClientCalls.blockingUnaryCall(channel1, descriptor1,
                CallOptions.DEFAULT.withDeadlineAfter(8, TimeUnit.SECONDS), outbound1);
        log.atDebug().addKeyValue("auditValue", request.getFilter().getQuery()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "StreamDepreciationMetrics", request.getFilter().getQuery());
        return StreamDepreciationMetricsResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setLocation(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .putAllMetadata(reply1.getAssetTransfer().getMetadataMap())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getSubscriber().getEmail())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Lists depreciations a page at a time.
     */
    public ListDepreciationReportsResponse listDepreciationReports(ListDepreciationReportsRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ListDepreciationReports")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getContext().getTraceId(), "ListDepreciationReports");

        var outbound0 = ScheduleCoverageReportRequest.newBuilder()
                .putAllAnnotations(request.getFilter().getLabelsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleCoverageReportRequest.Details.newBuilder()
                        .putAllFilters(request.getContext().getBaggageMap())
                        .build())
                .build();
        var reply0 = Optional.of(outbound0).map(value -> value.toBuilder().build())
                .map(value -> insuranceCoverageReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleCoverageReport(value)).orElseThrow();

        var outbound1 = CheckAssetTransferEligibilityRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getFilter().getCreatedAfter())
                                .build())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(CheckAssetTransferEligibilityRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
        final class Stage1Adapter {
            CheckAssetTransferEligibilityResponse fetch(CheckAssetTransferEligibilityRequest input) {
                return assetsAssetTransferValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).checkAssetTransferEligibility(input);
            }
        }
        var reply1 = new Stage1Adapter().fetch(outbound1);
        log.atDebug().addKeyValue("auditValue", request.getPage().getPageToken()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ListDepreciationReports", request.getPage().getPageToken());
        return ListDepreciationReportsResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply1.getResult().getCorrelationId())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Shares a depreciation.
     */
    public ShareDepreciationReportResponse shareDepreciationReport(ShareDepreciationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ShareDepreciationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "ShareDepreciationReport");

        var outbound0 = CloneUserRequest.newBuilder()
                .setContext(request.getContext())
                .setUser(User.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setName(request.getDetails().getRecipient().getName())
                                .setPhone(request.getDetails().getRecipient().getPhone())
                                .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var reply0 = identityUserServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).cloneUser(outbound0);

        var outbound1 = ScoreDepreciationRequest.newBuilder()
                .setContext(request.getContext())
                .setDepreciation(Depreciation.newBuilder()
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(reply0.getUser().getRelationshipManager().getEmail())
                                        .build())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<ScoreDepreciationResponse>();
        events.publishEvent(new ShareDepreciationReportStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = StreamFiscalPeriodNoticesRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(reply1.getDepreciation().getOrganizer().getEmail())
                        .setName(request.getDetails().getRecipient().getName())
                        .setPhone(reply0.getUser().getContact().getPhone())
                        .setPreferredChannel(request.getDetails().getRecipient().getPreferredChannel())
                        .build())
                .build();
        var stream2 = ledgerFiscalPeriodNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).streamFiscalPeriodNotices(outbound2);
        var reply2 = StreamFiscalPeriodNoticesResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getDetails().getTemplateId()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ShareDepreciationReport", request.getDetails().getTemplateId());
        return ShareDepreciationReportResponse.newBuilder()
                .setDepreciation(Depreciation.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(reply1.getDepreciation().getAudit().getCreatedAt())
                                .build())
                        .setDepreciationId(request.getDepreciationId())
                        .setOrganizer(UserRef.newBuilder()
                                .setEmail(request.getDetails().getRecipient().getEmail())
                                .setUserId(reply1.getDepreciation().getOrganizer().getUserId())
                                .build())
                        .addParticipants(DepreciationParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setName(reply0.getUser().getName())
                                        .build())
                                .build())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Deletes a depreciation.
     */
    public DeleteDepreciationReportResponse deleteDepreciationReport(DeleteDepreciationReportRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "DeleteDepreciationReport")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "Depreciation", request.getDepreciationId(), "DeleteDepreciationReport");

        var outbound0 = ListAuditTrailExportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListAuditTrailExportsRequest.Filter.newBuilder()
                        .setOwnerId(request.getDepreciationId())
                        .build())
                .build();
        var command0 = new DispatchCommand<>("acme.audit.v1.AuditTrailExportService/ListAuditTrailExports", outbound0);
        var reply0 = auditAuditTrailExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listAuditTrailExports(command0.payload());

        var outbound1 = WatchFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setSubscriber(ContactInfo.newBuilder()
                        .setEmail(request.getContext().getActor().getEmail())
                        .build())
                .build();
        var stream1 = assetsFixedAssetServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).watchFixedAssets(outbound1);
        var reply1 = WatchFixedAssetsResponse.getDefaultInstance();
        while (stream1.hasNext()) {
            reply1 = stream1.next();
        }
        log.atDebug().addKeyValue("auditValue", request.getReason()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "DeleteDepreciationReport", request.getReason());
        return DeleteDepreciationReportResponse.newBuilder()
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }
}
