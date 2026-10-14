package com.acme.finance.domains.assets;

import com.acme.assets.v1.ActivateDepreciationRequest;
import com.acme.assets.v1.ActivateDepreciationResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.CountDepreciationsRequest;
import com.acme.assets.v1.CountDepreciationsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.ExpireDepreciationRequest;
import com.acme.assets.v1.ExpireDepreciationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetDepreciationLifecycleRequest;
import com.acme.assets.v1.GetDepreciationLifecycleResponse;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.assets.v1.PushAssetTransferRequest;
import com.acme.assets.v1.PushAssetTransferResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.ReinstateDepreciationRequest;
import com.acme.assets.v1.ReinstateDepreciationResponse;
import com.acme.assets.v1.RenewDepreciationRequest;
import com.acme.assets.v1.RenewDepreciationResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.SuspendDepreciationRequest;
import com.acme.assets.v1.SuspendDepreciationResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.UpdateAccessPolicyRequest;
import com.acme.authorization.v1.UpdateAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ListSanctionReportsRequest;
import com.acme.compliance.v1.ListSanctionReportsResponse;
import com.acme.compliance.v1.SanctionReportingServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumLine;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSyncServiceGrpc;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.ratelimiting.v1.RateLimit;
import com.acme.ratelimiting.v1.RateLimitBlockAction;
import com.acme.ratelimiting.v1.RateLimitServiceGrpc;
import com.acme.ratelimiting.v1.UpdateRateLimitRequest;
import com.acme.ratelimiting.v1.UpdateRateLimitResponse;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DepreciationLifecycleApplicationTest {
    @Test
    void suspendDepreciationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DepreciationLifecycleApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newBlockingStub(channel),
                FixedAssetLifecycleServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                SanctionReportingServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                UnderwritingSyncServiceGrpc.newStub(channel),
                InvoiceBatchServiceGrpc.newStub(channel),
                RateLimitServiceGrpc.newFutureStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(SuspendDepreciationRequest.getDefaultInstance());
        var response = application.suspendDepreciation(request);
        var sent0 = channel.request("acme.assets.v1.DisposalCalculationService/EstimateDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "user_id"),
                ProbeSupport.value(sent0, "disposal", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "email"),
                ProbeSupport.value(sent0, "disposal", "assignee", "email"));
        var sent1 = channel.request("acme.assets.v1.DepreciationValidationService/ValidateDepreciation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "phone"),
                ProbeSupport.value(sent1, "depreciation", "participants", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(sent1, "depreciation", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify_contact", "preferred_channel"),
                ProbeSupport.value(sent1, "depreciation", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent1, "depreciation", "organizer", "display_name"));
        var sent2 = channel.request("acme.assets.v1.DisposalSyncService/ReconcileDisposals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "requester", "email"),
                ProbeSupport.value(sent2, "disposal", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "organizer", "display_name"),
                ProbeSupport.value(sent2, "disposal", "customer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "audit", "created_at"),
                ProbeSupport.value(sent2, "disposal", "audit", "created_at"));
        var sent3 = channel.request("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/ReconcileDisposals"), "disposal", "audit", "etag"),
                ProbeSupport.value(sent3, "access_policy", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "audit", "updated_by"),
                ProbeSupport.value(sent3, "access_policy", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(sent3, "access_policy", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "disposal", "assignee", "user_id"),
                ProbeSupport.value(sent3, "access_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalCalculationService/EstimateDisposal"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "audit", "created_by"),
                ProbeSupport.value(response, "depreciation", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/ReconcileDisposals"), "disposal", "audit", "etag"),
                ProbeSupport.value(response, "depreciation", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy"), "access_policy", "audit", "updated_by"),
                ProbeSupport.value(response, "depreciation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.authorization.v1.AccessPolicyService/UpdateAccessPolicy"), "access_policy", "owner", "email"),
                ProbeSupport.value(response, "depreciation", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }
}
