package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.ExportDepreciationsRequest;
import com.acme.assets.v1.ExportDepreciationsResponse;
import com.acme.assets.v1.FindFixedAssetsRequest;
import com.acme.assets.v1.FindFixedAssetsResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetQueryServiceGrpc;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.ListAssetTransferConflictsRequest;
import com.acme.assets.v1.ListAssetTransferConflictsResponse;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.PushAssetTransferRequest;
import com.acme.assets.v1.PushAssetTransferResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.SyncAssetTransfersRequest;
import com.acme.assets.v1.SyncAssetTransfersResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.collections.v1.PaymentPromise;
import com.acme.collections.v1.PaymentPromiseApprovalServiceGrpc;
import com.acme.collections.v1.RejectPaymentPromiseRequest;
import com.acme.collections.v1.RejectPaymentPromiseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseApprovalServiceGrpc;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalRequest;
import com.acme.compliance.v1.DelegateComplianceCaseApprovalResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationApprovalServiceGrpc;
import com.acme.lending.v1.LoanApplicationCardSettlement;
import com.acme.lending.v1.RecallLoanApplicationRequest;
import com.acme.lending.v1.RecallLoanApplicationResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class AssetTransferSyncApplicationTest {
    @Test
    void pullAssetTransfersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new AssetTransferSyncApplication(
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PaymentPromiseApprovalServiceGrpc.newBlockingStub(channel),
                ComplianceCaseApprovalServiceGrpc.newBlockingStub(channel),
                LoanApplicationApprovalServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(PullAssetTransfersRequest.getDefaultInstance());
        var response = application.pullAssetTransfers(request);
        var sent0 = channel.request("acme.collections.v1.PaymentPromiseApprovalService/RejectPaymentPromise");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "audit", "created_at"),
                ProbeSupport.value(sent0, "payment_promise", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "payment_promise", "organizer", "display_name"));
        var sent1 = channel.request("acme.compliance.v1.ComplianceCaseApprovalService/DelegateComplianceCaseApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/RejectPaymentPromise"), "payment_promise", "audit", "etag"),
                ProbeSupport.value(sent1, "compliance_case", "audit", "etag"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetQueryService/FindFixedAssets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/RejectPaymentPromise"), "payment_promise", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "lines", "line_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "lines", "attributes"),
                ProbeSupport.value(sent2, "filter", "labels"));
        var sent3 = channel.request("acme.assets.v1.DepreciationQueryService/ExportDepreciations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/RejectPaymentPromise"), "payment_promise", "location", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.PaymentPromiseApprovalService/RejectPaymentPromise"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseApprovalService/DelegateComplianceCaseApproval"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseApprovalService/DelegateComplianceCaseApproval"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer_id"),
                ProbeSupport.value(response, "asset_transfer", "asset_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "notes"),
                ProbeSupport.value(response, "asset_transfer", "notes"));
    }
}
