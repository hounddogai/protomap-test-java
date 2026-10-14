package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveDepreciationRequest;
import com.acme.assets.v1.ApproveDepreciationResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLifecycleServiceGrpc;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DelegateDepreciationApprovalRequest;
import com.acme.assets.v1.DelegateDepreciationApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationLifecycleServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EscalateDepreciationRequest;
import com.acme.assets.v1.EscalateDepreciationResponse;
import com.acme.assets.v1.ExpireDepreciationRequest;
import com.acme.assets.v1.ExpireDepreciationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.GetDepreciationApprovalStatusRequest;
import com.acme.assets.v1.GetDepreciationApprovalStatusResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ListPendingDepreciationsRequest;
import com.acme.assets.v1.ListPendingDepreciationsResponse;
import com.acme.assets.v1.ListPendingFixedAssetsRequest;
import com.acme.assets.v1.ListPendingFixedAssetsResponse;
import com.acme.assets.v1.RecallDepreciationRequest;
import com.acme.assets.v1.RecallDepreciationResponse;
import com.acme.assets.v1.RejectDepreciationRequest;
import com.acme.assets.v1.RejectDepreciationResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ScheduleAssetTransferExpiryRequest;
import com.acme.assets.v1.ScheduleAssetTransferExpiryResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.SubmitDepreciationRequest;
import com.acme.assets.v1.SubmitDepreciationResponse;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.assets.v1.SuspendDepreciationRequest;
import com.acme.assets.v1.SuspendDepreciationResponse;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeRequest;
import com.acme.audit.v1.SendRetentionPolicyTestNoticeResponse;
import com.acme.audit.v1.StreamRetentionPolicyNoticesRequest;
import com.acme.audit.v1.StreamRetentionPolicyNoticesResponse;
import com.acme.banking.v1.ListStandingOrderWorkflowsRequest;
import com.acme.banking.v1.ListStandingOrderWorkflowsResponse;
import com.acme.banking.v1.StandingOrderWorkflowServiceGrpc;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseApprovalServiceGrpc;
import com.acme.collections.v1.CollectionCaseStatusChange;
import com.acme.collections.v1.RejectCollectionCaseRequest;
import com.acme.collections.v1.RejectCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.experiments.v1.ExperimentQueryServiceGrpc;
import com.acme.experiments.v1.FindExperimentsRequest;
import com.acme.experiments.v1.FindExperimentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeRequest;
import com.acme.ledger.v1.SendLedgerAccountTestNoticeResponse;
import com.acme.risk.v1.RetryRiskScenarioWorkflowRequest;
import com.acme.risk.v1.RetryRiskScenarioWorkflowResponse;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioWorkflowServiceGrpc;
import com.acme.secrets.v1.Secret;
import com.acme.secrets.v1.SecretServiceGrpc;
import com.acme.secrets.v1.UpdateSecretRequest;
import com.acme.secrets.v1.UpdateSecretResponse;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
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

class DepreciationApprovalApplicationTest {
    @Test
    void escalateDepreciationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DepreciationApprovalApplicationEvents(FixedAssetApprovalServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DepreciationApprovalApplication.SubmitDepreciationStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DepreciationApprovalApplication(
                AssetTransferLifecycleServiceGrpc.newBlockingStub(channel),
                AssetTransferReportingServiceGrpc.newBlockingStub(channel),
                DepreciationLifecycleServiceGrpc.newBlockingStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                FixedAssetQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                StandingOrderWorkflowServiceGrpc.newFutureStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseApprovalServiceGrpc.newBlockingStub(channel),
                events,
                ExperimentQueryServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                new DepreciationApprovalRequestMapperImpl(),
                repository,
                Runnable::run,
                SecretServiceGrpc.newBlockingStub(channel),
                CashPositionServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(EscalateDepreciationRequest.getDefaultInstance());
        var response = application.escalateDepreciation(request);
        var sent0 = channel.request("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "location", "country_code"),
                ProbeSupport.value(sent0, "asset_transfer", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "audit", "updated_by"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "updated_by"));
        var sent1 = channel.request("acme.assets.v1.DisposalReportingService/ListDisposalReports");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "location", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry"), "asset_transfer", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "organizer", "user_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "metadata"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.banking.v1.StandingOrderWorkflowService/ListStandingOrderWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry"), "asset_transfer", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry"), "asset_transfer", "due_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.secrets.v1.SecretService/UpdateSecret");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "phone"),
                ProbeSupport.value(sent3, "secret", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferLifecycleService/ScheduleAssetTransferExpiry"), "asset_transfer", "metadata"),
                ProbeSupport.value(response, "depreciation", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.StandingOrderWorkflowService/ListStandingOrderWorkflows"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "status"),
                ProbeSupport.value(response, "depreciation", "status"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DepreciationApprovalApplicationEvents(FixedAssetApprovalServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RejectFixedAssetResponse>();
        listener.on(new DepreciationApprovalApplication.SubmitDepreciationStage0Requested(ProbeSupport.sample(RejectFixedAssetRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
