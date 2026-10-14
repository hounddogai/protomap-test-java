package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateAssetTransferRequest;
import com.acme.assets.v1.ActivateAssetTransferResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferBankSettlement;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.Depreciation;
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
import com.acme.finance.testing.ProbeSupport;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class AssetTransferLifecycleApplicationTest {
    @Test
    void terminateAssetTransferPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new AssetTransferLifecycleApplicationEvents(DisposalSyncServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof AssetTransferLifecycleApplication.TerminateAssetTransferStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new AssetTransferLifecycleApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                AssetTransferReportingServiceGrpc.newBlockingStub(channel),
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                MarketingCampaignQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseWorkflowServiceGrpc.newBlockingStub(channel),
                ForumQueryServiceGrpc.newBlockingStub(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                ContractServiceGrpc.newStub(channel),
                events,
                UnderwritingImportServiceGrpc.newBlockingStub(channel),
                UnderwritingReportingServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                new AssetTransferLifecycleRequestMapperImpl(),
                repository,
                Runnable::run,
                TelemetryReadingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(TerminateAssetTransferRequest.getDefaultInstance());
        var response = application.terminateAssetTransfer(request);
        var sent0 = channel.request("acme.telematics.v1.TelemetryReadingService/BatchGetTelemetryReadings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        var sent1 = channel.request("acme.assets.v1.DisposalSyncService/PushDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent1, "disposal", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent1, "disposal", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent1, "disposal", "requester", "preferred_channel"));
        var sent2 = channel.request("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/PushDisposal"), "disposal", "audit", "created_by"),
                ProbeSupport.value(sent2, "asset_transfer", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/PushDisposal"), "disposal", "customer", "code"),
                ProbeSupport.value(sent2, "asset_transfer", "counterparty", "code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/PushDisposal"), "disposal", "audit", "etag"),
                ProbeSupport.value(sent2, "asset_transfer", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/PushDisposal"), "disposal", "audit", "updated_at"),
                ProbeSupport.value(sent2, "asset_transfer", "audit", "updated_at"));
        var sent3 = channel.request("acme.ledger.v1.JournalEntryQueryService/ResolveJournalEntries");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.telematics.v1.TelemetryReadingService/BatchGetTelemetryReadings"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.telematics.v1.TelemetryReadingService/BatchGetTelemetryReadings"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/PushDisposal"), "disposal", "audit", "created_by"),
                ProbeSupport.value(response, "asset_transfer", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "asset_transfer", "counterparty", "code"),
                ProbeSupport.value(response, "asset_transfer", "counterparty", "code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "asset_transfer", "due_at"),
                ProbeSupport.value(response, "asset_transfer", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer_id"),
                ProbeSupport.value(response, "asset_transfer", "asset_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new AssetTransferLifecycleApplicationEvents(DisposalSyncServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PushDisposalResponse>();
        listener.on(new AssetTransferLifecycleApplication.TerminateAssetTransferStage1Requested(ProbeSupport.sample(PushDisposalRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
