package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDisposalRatesRequest;
import com.acme.assets.v1.GetDisposalRatesResponse;
import com.acme.assets.v1.GetDisposalRulesRequest;
import com.acme.assets.v1.GetDisposalRulesResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.NormalizeDisposalRequest;
import com.acme.assets.v1.NormalizeDisposalResponse;
import com.acme.assets.v1.PreviewDisposalRequest;
import com.acme.assets.v1.PreviewDisposalResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AnnotateAuditEventVersionRequest;
import com.acme.audit.v1.AnnotateAuditEventVersionResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventHistoryServiceGrpc;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.AttestationLine;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.CollateralDimensions;
import com.acme.lending.v1.SimulateCollateralRequest;
import com.acme.lending.v1.SimulateCollateralResponse;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DisposalValidationApplicationTest {
    @Test
    void normalizeDisposalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DisposalValidationApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventHistoryServiceGrpc.newBlockingStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(NormalizeDisposalRequest.getDefaultInstance());
        var response = application.normalizeDisposal(request);
        var sent0 = channel.request("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "audit", "etag"),
                ProbeSupport.value(sent0, "fixed_asset", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "contact", "email"),
                ProbeSupport.value(sent0, "fixed_asset", "owner", "email"));
        var sent1 = channel.request("acme.audit.v1.AuditEventHistoryService/AnnotateAuditEventVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "customer", "display_name"),
                ProbeSupport.value(sent1, "audit_event", "subject", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "result", "correlation_id"),
                ProbeSupport.value(sent1, "audit_event", "correlation_id"));
        var sent2 = channel.request("acme.lending.v1.CollateralCalculationService/SimulateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "fixed_asset", "dimensions", "width"),
                ProbeSupport.value(sent2, "collateral", "dimensions", "width"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/AnnotateAuditEventVersion"), "audit_event", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventHistoryService/AnnotateAuditEventVersion"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/SimulateCollateral"), "collateral", "audit", "created_at"),
                ProbeSupport.value(response, "disposal", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "priority"),
                ProbeSupport.value(response, "disposal", "priority"));
    }
}
