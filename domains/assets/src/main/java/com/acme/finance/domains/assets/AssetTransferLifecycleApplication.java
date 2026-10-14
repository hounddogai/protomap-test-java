package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateAssetTransferRequest;
import com.acme.assets.v1.ActivateAssetTransferResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferBankSettlement;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExpireAssetTransferRequest;
import com.acme.assets.v1.ExpireAssetTransferResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetAssetTransferLifecycleRequest;
import com.acme.assets.v1.GetAssetTransferLifecycleResponse;
import com.acme.assets.v1.GetAssetTransferRatesRequest;
import com.acme.assets.v1.GetAssetTransferRatesResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ReinstateAssetTransferRequest;
import com.acme.assets.v1.ReinstateAssetTransferResponse;
import com.acme.assets.v1.RenewAssetTransferRequest;
import com.acme.assets.v1.RenewAssetTransferResponse;
import com.acme.assets.v1.ScheduleAssetTransferExpiryRequest;
import com.acme.assets.v1.ScheduleAssetTransferExpiryResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SuspendAssetTransferRequest;
import com.acme.assets.v1.SuspendAssetTransferResponse;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.assets.v1.TerminateAssetTransferRequest;
import com.acme.assets.v1.TerminateAssetTransferResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.BrowseAuditEventsRequest;
import com.acme.audit.v1.BrowseAuditEventsResponse;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ValidateAccessPolicyRequest;
import com.acme.authorization.v1.ValidateAccessPolicyResponse;
import com.acme.campaigns.v1.BrowseMarketingCampaignsRequest;
import com.acme.campaigns.v1.BrowseMarketingCampaignsResponse;
import com.acme.campaigns.v1.MarketingCampaignQueryServiceGrpc;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowRequest;
import com.acme.collections.v1.AdvancePaymentPromiseWorkflowResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.collections.v1.PaymentPromiseWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.ExportForumsRequest;
import com.acme.community.v1.ExportForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.compliance.v1.CountSanctionTotalsRequest;
import com.acme.compliance.v1.CountSanctionTotalsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.AdvancePolicyWorkflowRequest;
import com.acme.insurance.v1.AdvancePolicyWorkflowResponse;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.ListUnderwritingImportsRequest;
import com.acme.insurance.v1.ListUnderwritingImportsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.PolicyWorkflowServiceGrpc;
import com.acme.insurance.v1.ShareUnderwritingReportRequest;
import com.acme.insurance.v1.ShareUnderwritingReportResponse;
import com.acme.insurance.v1.UnderwritingImportServiceGrpc;
import com.acme.insurance.v1.UnderwritingReportingServiceGrpc;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesRequest;
import com.acme.ledger.v1.SubscribePostingBatchUpdatesResponse;
import com.acme.telematics.v1.BatchGetTelemetryReadingsRequest;
import com.acme.telematics.v1.BatchGetTelemetryReadingsResponse;
import com.acme.telematics.v1.TelemetryReadingServiceGrpc;
import com.google.protobuf.Any;
import java.util.Optional;
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
 * Moves an asset transfer between the states of its lifecycle. Coordinates validation, durable audit writes, and
 * downstream operations.
 */
@Service
public class AssetTransferLifecycleApplication {
    private static final Logger log = LoggerFactory.getLogger(AssetTransferLifecycleApplication.class);

    private final AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking;

    private final AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking;

    private final AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking;

    private final DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync;

    private final FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking;

    private final AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking;

    private final AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking;

    private final AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking;

    private final MarketingCampaignQueryServiceGrpc.MarketingCampaignQueryServiceBlockingStub campaignsMarketingCampaignQueryServiceBlocking;

    private final ChannelRegistry channels;

    private final PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking;

    private final ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking;

    private final SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking;

    private final ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync;

    private final ApplicationEventPublisher events;

    private final UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking;

    private final UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking;

    private final JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking;

    private final PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking;

    private final AssetTransferLifecycleRequestMapper mapper;

    private final AssetsRepository repository;

    private final Executor rpcExecutor;

    private final TelemetryReadingServiceGrpc.TelemetryReadingServiceBlockingStub telematicsTelemetryReadingServiceBlocking;

    public AssetTransferLifecycleApplication(
            AssetTransferCalculationServiceGrpc.AssetTransferCalculationServiceBlockingStub assetsAssetTransferCalculationServiceBlocking,
            AssetTransferReportingServiceGrpc.AssetTransferReportingServiceBlockingStub assetsAssetTransferReportingServiceBlocking,
            AssetTransferSyncServiceGrpc.AssetTransferSyncServiceBlockingStub assetsAssetTransferSyncServiceBlocking,
            DisposalSyncServiceGrpc.DisposalSyncServiceStub assetsDisposalSyncServiceAsync,
            FixedAssetCalculationServiceGrpc.FixedAssetCalculationServiceBlockingStub assetsFixedAssetCalculationServiceBlocking,
            AuditEventExportServiceGrpc.AuditEventExportServiceBlockingStub auditAuditEventExportServiceBlocking,
            AuditEventQueryServiceGrpc.AuditEventQueryServiceBlockingStub auditAuditEventQueryServiceBlocking,
            AccessPolicyValidationServiceGrpc.AccessPolicyValidationServiceBlockingStub authorizationAccessPolicyValidationServiceBlocking,
            MarketingCampaignQueryServiceGrpc.MarketingCampaignQueryServiceBlockingStub campaignsMarketingCampaignQueryServiceBlocking,
            ChannelRegistry channels,
            PaymentPromiseWorkflowServiceGrpc.PaymentPromiseWorkflowServiceBlockingStub collectionsPaymentPromiseWorkflowServiceBlocking,
            ForumQueryServiceGrpc.ForumQueryServiceBlockingStub communityForumQueryServiceBlocking,
            SanctionReportingServiceGrpc.SanctionReportingServiceBlockingStub complianceSanctionReportingServiceBlocking,
            ContractServiceGrpc.ContractServiceStub contractsContractServiceAsync,
            ApplicationEventPublisher events,
            UnderwritingImportServiceGrpc.UnderwritingImportServiceBlockingStub insuranceUnderwritingImportServiceBlocking,
            UnderwritingReportingServiceGrpc.UnderwritingReportingServiceBlockingStub insuranceUnderwritingReportingServiceBlocking,
            JournalEntryQueryServiceGrpc.JournalEntryQueryServiceBlockingStub ledgerJournalEntryQueryServiceBlocking,
            PostingBatchNotificationServiceGrpc.PostingBatchNotificationServiceBlockingStub ledgerPostingBatchNotificationServiceBlocking,
            AssetTransferLifecycleRequestMapper mapper,
            AssetsRepository repository,
            @Qualifier("rpcExecutor") Executor rpcExecutor,
            TelemetryReadingServiceGrpc.TelemetryReadingServiceBlockingStub telematicsTelemetryReadingServiceBlocking) {
        this.assetsAssetTransferCalculationServiceBlocking = assetsAssetTransferCalculationServiceBlocking;
        this.assetsAssetTransferReportingServiceBlocking = assetsAssetTransferReportingServiceBlocking;
        this.assetsAssetTransferSyncServiceBlocking = assetsAssetTransferSyncServiceBlocking;
        this.assetsDisposalSyncServiceAsync = assetsDisposalSyncServiceAsync;
        this.assetsFixedAssetCalculationServiceBlocking = assetsFixedAssetCalculationServiceBlocking;
        this.auditAuditEventExportServiceBlocking = auditAuditEventExportServiceBlocking;
        this.auditAuditEventQueryServiceBlocking = auditAuditEventQueryServiceBlocking;
        this.authorizationAccessPolicyValidationServiceBlocking = authorizationAccessPolicyValidationServiceBlocking;
        this.campaignsMarketingCampaignQueryServiceBlocking = campaignsMarketingCampaignQueryServiceBlocking;
        this.channels = channels;
        this.collectionsPaymentPromiseWorkflowServiceBlocking = collectionsPaymentPromiseWorkflowServiceBlocking;
        this.communityForumQueryServiceBlocking = communityForumQueryServiceBlocking;
        this.complianceSanctionReportingServiceBlocking = complianceSanctionReportingServiceBlocking;
        this.contractsContractServiceAsync = contractsContractServiceAsync;
        this.events = events;
        this.insuranceUnderwritingImportServiceBlocking = insuranceUnderwritingImportServiceBlocking;
        this.insuranceUnderwritingReportingServiceBlocking = insuranceUnderwritingReportingServiceBlocking;
        this.ledgerJournalEntryQueryServiceBlocking = ledgerJournalEntryQueryServiceBlocking;
        this.ledgerPostingBatchNotificationServiceBlocking = ledgerPostingBatchNotificationServiceBlocking;
        this.mapper = mapper;
        this.repository = repository;
        this.rpcExecutor = rpcExecutor;
        this.telematicsTelemetryReadingServiceBlocking = telematicsTelemetryReadingServiceBlocking;
    }

    public record TerminateAssetTransferStage1Requested(PushDisposalRequest request, CompletableFuture<PushDisposalResponse> completion, io.grpc.Context context) {}

    /**
     * Activates an asset transfer.
     */
    public ActivateAssetTransferResponse activateAssetTransfer(ActivateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ActivateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "ActivateAssetTransfer");

        var outbound0 = ListUnderwritingImportsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListUnderwritingImportsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingImportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).listUnderwritingImports(outbound0), rpcExecutor);

        var outbound1 = ShareUnderwritingReportRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ShareUnderwritingReportRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> insuranceUnderwritingReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).shareUnderwritingReport(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ExplainFixedAssetCalculationRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainFixedAssetCalculationRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(reply1.getUnderwriting().getRequester().getEmail())
                                .build())
                        .build())
                .build();
        var reply2 = assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).explainFixedAssetCalculation(outbound2);

        var outbound3 = AdvancePaymentPromiseWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setPaymentPromise(PaymentPromise.newBuilder()
                        .addParticipants(PaymentPromiseParticipant.newBuilder()
                                .setContact(ContactInfo.newBuilder()
                                        .setEmail(request.getDetails().getNotifyContact().getEmail())
                                        .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                        .build())
                                .build())
                        .build())
                .build();
        var reply3 = Stream.of(outbound3).map(value -> value.toBuilder().build())
                .flatMap(value -> Stream.of(collectionsPaymentPromiseWorkflowServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).advancePaymentPromiseWorkflow(value)))
                .collect(Collectors.toList()).getFirst();

        var outbound4 = BatchCreateContractsRequest.newBuilder()
                .setContext(request.getContext())
                .setHeader(BatchCreateContractsRequest.StreamHeader.newBuilder()
                        .setSource(request.getAssetTransferId())
                        .setSubmittedBy(request.getDetails().getNotifyContact())
                        .build())
                .build();
        var collector4 = new StreamCollector<>(BatchCreateContractsResponse.getDefaultInstance());
        var input4 = contractsContractServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).batchCreateContracts(collector4);
        input4.onNext(outbound4);
        input4.onCompleted();
        var reply4 = collector4.await();

        var outbound5 = StreamDisposalChangesRequest.newBuilder()
                .setChange(Disposal.newBuilder()
                        .setAssignee(request.getDetails().getNewOwner())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedAt(request.getContext().getDeadline())
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addComments(DisposalComment.newBuilder()
                                .setAuthorEmail(request.getContext().getActor().getEmail())
                                .build())
                        .build())
                .build();
        var collector5 = new StreamCollector<>(StreamDisposalChangesResponse.getDefaultInstance());
        var input5 = assetsDisposalSyncServiceAsync.withDeadlineAfter(8, TimeUnit.SECONDS).streamDisposalChanges(collector5);
        input5.onNext(outbound5);
        input5.onCompleted();
        var reply5 = collector5.await();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ActivateAssetTransfer", request.getDetails().getReasonCode());
        return ActivateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setPostalCode(reply3.getPaymentPromise().getLocation().getPostalCode())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ActivateAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply2.getOutcome().getEffectiveAt())
                        .setReferenceId(reply1.getOutcome().getReferenceId())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Suspends an asset transfer.
     */
    public SuspendAssetTransferResponse suspendAssetTransfer(SuspendAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "SuspendAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "SuspendAssetTransfer");

        var outbound0 = SubscribePostingBatchUpdatesRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(SubscribePostingBatchUpdatesRequest.Details.newBuilder()
                        .setPayload(Any.pack(request.getContext()))
                        .setRecipient(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNewOwner().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .build())
                        .build())
                .build();
        var reply0 = ledgerPostingBatchNotificationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).subscribePostingBatchUpdates(outbound0);

        var outbound1 = BrowseMarketingCampaignsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseMarketingCampaignsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getPostingBatch().getDueAt())
                        .setRegion(reply0.getPostingBatch().getBillingAddress().getRegion())
                        .build())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> campaignsMarketingCampaignQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseMarketingCampaigns(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "SuspendAssetTransfer", request.getDetails().getReasonCode());
        return SuspendAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setOutcome(SuspendAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply1.getResult().getDetailsMap())
                        .setMessage(reply1.getResult().getMessage())
                        .setStatus(reply0.getOutcome().getStatus())
                        .build())
                .build();
    }

    /**
     * Reinstates an asset transfer.
     */
    public ReinstateAssetTransferResponse reinstateAssetTransfer(ReinstateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ReinstateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "ReinstateAssetTransfer");

        var outbound0 = CountSanctionTotalsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(CountSanctionTotalsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .putAllLabels(request.getAnnotationsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> complianceSanctionReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).countSanctionTotals(outbound0), rpcExecutor);

        var outbound1 = BrowseAuditEventsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(BrowseAuditEventsRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getDetails().getEffectiveAt())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).browseAuditEvents(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ReinstateAssetTransfer", request.getDetails().getReasonCode());
        return ReinstateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setOutcome(ReinstateAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply1.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setCorrelationId(reply0.getResult().getCorrelationId())
                        .build())
                .build();
    }

    /**
     * Terminates an asset transfer.
     */
    public TerminateAssetTransferResponse terminateAssetTransfer(TerminateAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "TerminateAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "TerminateAssetTransfer");

        var outbound0 = BatchGetTelemetryReadingsRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage0Adapter {
            BatchGetTelemetryReadingsResponse fetch(BatchGetTelemetryReadingsRequest input) {
                return telematicsTelemetryReadingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).batchGetTelemetryReadings(input);
            }
        }
        var reply0 = new Stage0Adapter().fetch(outbound0);

        var outbound1 = PushDisposalRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(PushDisposalRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .setDisposal(Disposal.newBuilder()
                        .setRequester(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setPhone(request.getDetails().getNotifyContact().getPhone())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var completion1 = new CompletableFuture<PushDisposalResponse>();
        events.publishEvent(new TerminateAssetTransferStage1Requested(outbound1, completion1, io.grpc.Context.current()));
        var reply1 = completion1.orTimeout(10, TimeUnit.SECONDS).join();

        var outbound2 = PullAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDisposal().getAudit().getCreatedBy())
                                .setEtag(reply1.getDisposal().getAudit().getEtag())
                                .setUpdatedAt(reply1.getDisposal().getAudit().getUpdatedAt())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setCode(switch (reply1.getDisposal().getOriginCase()) {
                                    case CUSTOMER -> reply1.getDisposal().getCustomer().getCode();
                                    default -> "";
                                })
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(PullAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var reply2 = assetsAssetTransferSyncServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).pullAssetTransfers(outbound2);

        var outbound3 = ResolveJournalEntriesRequest.newBuilder()
                .setContext(request.getContext())
                .build();
        final class Stage3Adapter {
            ResolveJournalEntriesResponse fetch(ResolveJournalEntriesRequest input) {
                return ledgerJournalEntryQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).resolveJournalEntries(input);
            }
        }
        var reply3 = new Stage3Adapter().fetch(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "TerminateAssetTransfer", request.getDetails().getReasonCode());
        return TerminateAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply1.getDisposal().getAudit().getCreatedBy())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setCode(reply2.getAssetTransfer().getCounterparty().getCode())
                                .build())
                        .setDueAt(reply2.getAssetTransfer().getDueAt())
                        .build())
                .setOutcome(TerminateAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setStatus(reply0.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply0.getResult().getReferenceIdsList())
                        .build())
                .build();
    }

    /**
     * Renews an asset transfer.
     */
    public RenewAssetTransferResponse renewAssetTransfer(RenewAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "RenewAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "RenewAssetTransfer");

        var outbound0 = ScheduleAssetTransferReportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .build();
        var pending0 = CompletableFuture.supplyAsync(() -> assetsAssetTransferReportingServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAssetTransferReport(outbound0), rpcExecutor);

        var outbound1 = RecalculateFixedAssetsRequest.newBuilder()
                .setContext(request.getContext())
                .setFixedAsset(FixedAsset.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .build())
                .build();
        var pending1 = CompletableFuture.supplyAsync(() -> assetsFixedAssetCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).recalculateFixedAssets(outbound1), rpcExecutor);
        CompletableFuture.allOf(pending0, pending1).join();
        var reply0 = pending0.join();
        var reply1 = pending1.join();

        var outbound2 = ScheduleAuditEventExportRequest.newBuilder()
                .putAllAnnotations(request.getAnnotationsMap())
                .setContext(request.getContext())
                .setDetails(ScheduleAuditEventExportRequest.Details.newBuilder()
                        .setPeriod(reply0.getPeriod())
                        .build())
                .setReason(request.getReason())
                .build();
        final class Stage2Adapter {
            ScheduleAuditEventExportResponse fetch(ScheduleAuditEventExportRequest input) {
                return auditAuditEventExportServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).scheduleAuditEventExport(input);
            }
        }
        var reply2 = new Stage2Adapter().fetch(outbound2);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "RenewAssetTransfer", request.getDetails().getReasonCode());
        return RenewAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setCountryCode(reply1.getFixedAsset().getAddress().getCountryCode())
                                .build())
                        .build())
                .setOutcome(RenewAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(request.getDetails().getEffectiveAt())
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply2.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Expires an asset transfer.
     */
    public ExpireAssetTransferResponse expireAssetTransfer(ExpireAssetTransferRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ExpireAssetTransfer")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "ExpireAssetTransfer");

        var outbound0 = ValidateAccessPolicyRequest.newBuilder()
                .setAccessPolicy(AccessPolicy.newBuilder()
                        .setOwner(UserRef.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(ValidateAccessPolicyRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .setPreferredChannel(request.getDetails().getNotifyContact().getPreferredChannel())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<ValidateAccessPolicyRequest, ValidateAccessPolicyResponse>() {
            @Override
            public ValidateAccessPolicyResponse apply(ValidateAccessPolicyRequest input) {
                return authorizationAccessPolicyValidationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).validateAccessPolicy(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = mapper.expireAssetTransferStage1(request, reply0);
        var reply1 = auditAuditEventQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).suggestAuditEvents(outbound1);

        var outbound2 = ExportForumsRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ExportForumsRequest.Filter.newBuilder()
                        .setCreatedAfter(reply0.getAccessPolicy().getAudit().getCreatedAt())
                        .putAllLabels(reply1.getResult().getDetailsMap())
                        .setOwnerId(request.getDetails().getNewOwner().getUserId())
                        .build())
                .build();
        var stream2 = communityForumQueryServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).exportForums(outbound2);
        var reply2 = ExportForumsResponse.getDefaultInstance();
        while (stream2.hasNext()) {
            reply2 = stream2.next();
        }

        var outbound3 = AdvancePolicyWorkflowRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(AdvancePolicyWorkflowRequest.Details.newBuilder()
                        .setNotify(ContactInfo.newBuilder()
                                .setEmail(reply0.getAccessPolicy().getOwner().getEmail())
                                .build())
                        .build())
                .setPolicy(Policy.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setCreatedBy(reply0.getAccessPolicy().getAudit().getCreatedBy())
                                .build())
                        .setHolder(CoverageRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .addLimits(PolicyLimit.newBuilder()
                                .setAmount(reply0.getAccessPolicy().getDiscount().getAmount())
                                .build())
                        .build())
                .build();
        var channel3 = channels.lookup("acme.insurance.v1.PolicyWorkflowService");
        var selected3 = PolicyWorkflowServiceGrpc.newBlockingStub(channel3)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply3 = selected3.advancePolicyWorkflow(outbound3);
        log.atDebug().addKeyValue("auditValue", request.getDetails().getReasonCode()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ExpireAssetTransfer", request.getDetails().getReasonCode());
        return ExpireAssetTransferResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply3.getPolicy().getAudit().getEtag())
                                .build())
                        .setCounterparty(FixedAssetRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .build())
                .setOutcome(ExpireAssetTransferResponse.Outcome.newBuilder()
                        .setEffectiveAt(reply0.getOutcome().getEffectiveAt())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .addAllReferenceIds(reply1.getResult().getReferenceIdsList())
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .build();
    }

    /**
     * Returns an asset transfer.
     */
    public GetAssetTransferLifecycleResponse getAssetTransferLifecycle(GetAssetTransferLifecycleRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "GetAssetTransferLifecycle")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.recordAction(request.getContext().getTenantId(), "AssetTransfer", request.getAssetTransferId(), "GetAssetTransferLifecycle");

        var outbound0 = SyncAssetTransfersRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getContext().getDeadline())
                                .build())
                        .addLines(AssetTransferLine.newBuilder()
                                .putAllAttributes(request.getContext().getBaggageMap())
                                .build())
                        .build())
                .setAssetTransferId(request.getAssetTransferId())
                .setContext(request.getContext())
                .setDetails(SyncAssetTransfersRequest.Details.newBuilder()
                        .setPayload(JsonCodec.encode(request.getContext()))
                        .build())
                .build();
        var channel0 = channels.lookup("acme.assets.v1.AssetTransferSyncService");
        var selected0 = AssetTransferSyncServiceGrpc.newBlockingStub(channel0)
                .withDeadlineAfter(8, TimeUnit.SECONDS);
        var reply0 = selected0.syncAssetTransfers(outbound0);
        return GetAssetTransferLifecycleResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .putAllDetails(reply0.getResult().getDetailsMap())
                        .build())
                .build();
    }

    /**
     * Schedules an asset transfer.
     */
    public ScheduleAssetTransferExpiryResponse scheduleAssetTransferExpiry(ScheduleAssetTransferExpiryRequest request) {
        RpcSupport.validate(request.getContext());
        log.atInfo().addKeyValue("operation", "ScheduleAssetTransferExpiry")
                .addKeyValue("tenant", request.getContext().getTenantId())
                .addKeyValue("trace", request.getContext().getTraceId()).log("Finance operation started");
        repository.saveAssetTransfer(request.getContext().getTenantId(), request.getAssetTransfer());

        var outbound0 = EstimateAssetTransferRequest.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getAssetTransfer().getAudit().getUpdatedAt())
                                .build())
                        .setBank(AssetTransferBankSettlement.newBuilder()
                                .setIban(request.getAssetTransfer().getBank().getIban())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(EstimateAssetTransferRequest.Details.newBuilder()
                        .setDestination(PostalAddress.newBuilder()
                                .setRegion(request.getAssetTransfer().getBillingAddress().getRegion())
                                .build())
                        .build())
                .build();
        var adapter0 = new Function<EstimateAssetTransferRequest, EstimateAssetTransferResponse>() {
            @Override
            public EstimateAssetTransferResponse apply(EstimateAssetTransferRequest input) {
                return assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).estimateAssetTransfer(input);
            }
        };
        var reply0 = adapter0.apply(outbound0);

        var outbound1 = GetAssetTransferRatesRequest.newBuilder()
                .setAssetTransferId(request.getAssetTransfer().getAssetTransferId())
                .setContext(request.getContext())
                .build();
        var reply1 = Optional.of(outbound1).map(value -> value.toBuilder().build())
                .map(value -> assetsAssetTransferCalculationServiceBlocking.withDeadlineAfter(8, TimeUnit.SECONDS).getAssetTransferRates(value)).orElseThrow();
        log.atDebug().addKeyValue("auditValue", request.getAssetTransfer().getNumber()).log("Local audit detail");
        repository.recordDetail(request.getContext().getTenantId(), "ScheduleAssetTransferExpiry", request.getAssetTransfer().getNumber());
        return ScheduleAssetTransferExpiryResponse.newBuilder()
                .setAssetTransfer(AssetTransfer.newBuilder()
                        .setAssetTransferId(request.getAssetTransferId())
                        .setStatus(request.getAssetTransfer().getStatus())
                        .build())
                .setOutcome(ScheduleAssetTransferExpiryResponse.Outcome.newBuilder()
                        .setStatus(reply1.getResult().getStatus())
                        .build())
                .setResult(OperationResult.newBuilder()
                        .setMessage(reply0.getResult().getMessage())
                        .build())
                .build();
    }
}
