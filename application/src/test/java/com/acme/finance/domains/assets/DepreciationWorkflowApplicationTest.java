package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceDepreciationWorkflowRequest;
import com.acme.assets.v1.AdvanceDepreciationWorkflowResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCardSettlement;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CancelDepreciationWorkflowRequest;
import com.acme.assets.v1.CancelDepreciationWorkflowResponse;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.FetchDepreciationReportRequest;
import com.acme.assets.v1.FetchDepreciationReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.InspectDepreciationWorkflowRequest;
import com.acme.assets.v1.InspectDepreciationWorkflowResponse;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.assets.v1.PauseDepreciationWorkflowRequest;
import com.acme.assets.v1.PauseDepreciationWorkflowResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ResumeDepreciationWorkflowRequest;
import com.acme.assets.v1.ResumeDepreciationWorkflowResponse;
import com.acme.assets.v1.RetryDepreciationWorkflowRequest;
import com.acme.assets.v1.RetryDepreciationWorkflowResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.StartDepreciationWorkflowRequest;
import com.acme.assets.v1.StartDepreciationWorkflowResponse;
import com.acme.assets.v1.SubmitDepreciationRequest;
import com.acme.assets.v1.SubmitDepreciationResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.NotifyAuditTrailWatchersRequest;
import com.acme.audit.v1.NotifyAuditTrailWatchersResponse;
import com.acme.collections.v1.ListPendingWriteOffsRequest;
import com.acme.collections.v1.ListPendingWriteOffsResponse;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.devices.v1.ExplainManagedDeviceCalculationRequest;
import com.acme.devices.v1.ExplainManagedDeviceCalculationResponse;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.keymanagement.v1.CryptoKeyServiceGrpc;
import com.acme.keymanagement.v1.SearchCryptoKeysRequest;
import com.acme.keymanagement.v1.SearchCryptoKeysResponse;
import com.acme.ledger.v1.LedgerAccountRef;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DepreciationWorkflowApplicationTest {
    @Test
    void retryDepreciationWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DepreciationWorkflowApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                DepreciationApprovalServiceGrpc.newBlockingStub(channel),
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                ManagedDeviceCalculationServiceGrpc.newBlockingStub(channel),
                CryptoKeyServiceGrpc.newBlockingStub(channel),
                PostingBatchValidationServiceGrpc.newBlockingStub(channel),
                new DepreciationWorkflowRequestMapperImpl(),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(RetryDepreciationWorkflowRequest.getDefaultInstance());
        var response = application.retryDepreciationWorkflow(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailNotificationService/NotifyAuditTrailWatchers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "name"),
                ProbeSupport.value(sent0, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "phone"),
                ProbeSupport.value(sent0, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent0, "details", "recipient", "email"));
        var sent1 = channel.request("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "location", "country_code"),
                ProbeSupport.value(sent1, "asset_transfer", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailNotificationService/NotifyAuditTrailWatchers"), "audit_trail", "card", "holder_name"),
                ProbeSupport.value(sent1, "asset_transfer", "card", "holder_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "organizer", "display_name"),
                ProbeSupport.value(sent1, "asset_transfer", "counterparty", "display_name"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetService/WatchFixedAssets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent2, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/PullAssetTransfers"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "title"),
                ProbeSupport.value(response, "depreciation", "title"));
    }
}
