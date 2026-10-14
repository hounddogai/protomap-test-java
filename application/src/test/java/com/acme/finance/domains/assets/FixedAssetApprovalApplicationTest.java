package com.acme.finance.domains.assets;

import com.acme.assets.v1.ApproveFixedAssetRequest;
import com.acme.assets.v1.ApproveFixedAssetResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferReportingServiceGrpc;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DelegateFixedAssetApprovalRequest;
import com.acme.assets.v1.DelegateFixedAssetApprovalResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.EscalateFixedAssetRequest;
import com.acme.assets.v1.EscalateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.GetFixedAssetApprovalStatusRequest;
import com.acme.assets.v1.GetFixedAssetApprovalStatusResponse;
import com.acme.assets.v1.ListPendingFixedAssetsRequest;
import com.acme.assets.v1.ListPendingFixedAssetsResponse;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.assets.v1.RecallFixedAssetRequest;
import com.acme.assets.v1.RecallFixedAssetResponse;
import com.acme.assets.v1.RejectFixedAssetRequest;
import com.acme.assets.v1.RejectFixedAssetResponse;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.assets.v1.SubmitFixedAssetRequest;
import com.acme.assets.v1.SubmitFixedAssetResponse;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ScheduleAuditTrailExportRequest;
import com.acme.audit.v1.ScheduleAuditTrailExportResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.collections.v1.EscalatePaymentPromiseRequest;
import com.acme.collections.v1.EscalatePaymentPromiseResponse;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.PaymentPromiseParticipant;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.documents.v1.WatchDocumentsRequest;
import com.acme.documents.v1.WatchDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fleet.v1.ListVehiclesRequest;
import com.acme.fleet.v1.ListVehiclesResponse;
import com.acme.identity.v1.CheckUserEligibilityRequest;
import com.acme.identity.v1.CheckUserEligibilityResponse;
import com.acme.identity.v1.ExportUsersRequest;
import com.acme.identity.v1.ExportUsersResponse;
import com.acme.identity.v1.NormalizeUserRequest;
import com.acme.identity.v1.NormalizeUserResponse;
import com.acme.identity.v1.SuggestUsersRequest;
import com.acme.identity.v1.SuggestUsersResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserQueryServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserValidationServiceGrpc;
import com.acme.identity.v1.ValidateUserRequest;
import com.acme.identity.v1.ValidateUserResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentLifecycleServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.ScheduleRepaymentExpiryRequest;
import com.acme.lending.v1.ScheduleRepaymentExpiryResponse;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.notifications.v1.CheckNotificationEligibilityRequest;
import com.acme.notifications.v1.CheckNotificationEligibilityResponse;
import com.acme.notifications.v1.LintNotificationsRequest;
import com.acme.notifications.v1.LintNotificationsResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FixedAssetApprovalApplicationTest {
    @Test
    void recallFixedAssetPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetApprovalApplication(
                AssetTransferReportingServiceGrpc.newBlockingStub(channel),
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                UserQueryServiceGrpc.newBlockingStub(channel),
                UserValidationServiceGrpc.newBlockingStub(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newStub(channel),
                RepaymentLifecycleServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                new FixedAssetApprovalRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(RecallFixedAssetRequest.getDefaultInstance());
        var response = application.recallFixedAsset(request);
        var sent0 = channel.request("acme.assets.v1.DisposalValidationService/PreviewDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "audit", "created_at"),
                ProbeSupport.value(sent0, "disposal", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "approver", "email"),
                ProbeSupport.value(sent0, "disposal", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner", "display_name"),
                ProbeSupport.value(sent0, "disposal", "customer", "display_name"));
        var sent1 = channel.request("acme.identity.v1.UserValidationService/NormalizeUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/PreviewDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(sent1, "user", "contact", "email"));
        var sent2 = channel.request("acme.fleet.v1.VehicleService/ListVehicles");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.lending.v1.RepaymentLifecycleService/ScheduleRepaymentExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/PreviewDisposal"), "disposal", "assignee", "email"),
                ProbeSupport.value(sent3, "repayment", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/NormalizeUser"), "user", "audit", "created_by"),
                ProbeSupport.value(sent3, "repayment", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner", "email"),
                ProbeSupport.value(sent3, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/PreviewDisposal"), "disposal", "requester", "name"),
                ProbeSupport.value(sent3, "repayment", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/PreviewDisposal"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserValidationService/NormalizeUser"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fleet.v1.VehicleService/ListVehicles"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.fleet.v1.VehicleService/ListVehicles"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.RepaymentLifecycleService/ScheduleRepaymentExpiry"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset_id"),
                ProbeSupport.value(response, "fixed_asset", "fixed_asset_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "name"),
                ProbeSupport.value(response, "fixed_asset", "name"));
    }
}
