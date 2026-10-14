package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.BrowseDepreciationsRequest;
import com.acme.assets.v1.BrowseDepreciationsResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.CountDepreciationsRequest;
import com.acme.assets.v1.CountDepreciationsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.ExportDepreciationsRequest;
import com.acme.assets.v1.ExportDepreciationsResponse;
import com.acme.assets.v1.FindDepreciationsRequest;
import com.acme.assets.v1.FindDepreciationsResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.LookupDepreciationRequest;
import com.acme.assets.v1.LookupDepreciationResponse;
import com.acme.assets.v1.PeekDepreciationRequest;
import com.acme.assets.v1.PeekDepreciationResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.QuoteFixedAssetRequest;
import com.acme.assets.v1.QuoteFixedAssetResponse;
import com.acme.assets.v1.ResolveDepreciationsRequest;
import com.acme.assets.v1.ResolveDepreciationsResponse;
import com.acme.assets.v1.ScoreDisposalRequest;
import com.acme.assets.v1.ScoreDisposalResponse;
import com.acme.assets.v1.SuggestDepreciationsRequest;
import com.acme.assets.v1.SuggestDepreciationsResponse;
import com.acme.assets.v1.SummarizeDepreciationsRequest;
import com.acme.assets.v1.SummarizeDepreciationsResponse;
import com.acme.assets.v1.ValidateAssetTransferRequest;
import com.acme.assets.v1.ValidateAssetTransferResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.boxoffice.v1.BoxOfficeSaleQueryServiceGrpc;
import com.acme.boxoffice.v1.SuggestBoxOfficeSalesRequest;
import com.acme.boxoffice.v1.SuggestBoxOfficeSalesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ClaimQueryServiceGrpc;
import com.acme.insurance.v1.CountClaimsRequest;
import com.acme.insurance.v1.CountClaimsResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.NormalizeCollateralRequest;
import com.acme.lending.v1.NormalizeCollateralResponse;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.acme.notifications.v1.BrowseNotificationsRequest;
import com.acme.notifications.v1.BrowseNotificationsResponse;
import com.acme.risk.v1.PreviewRiskScenarioRequest;
import com.acme.risk.v1.PreviewRiskScenarioResponse;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DepreciationQueryApplicationTest {
    @Test
    void exportDepreciationsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DepreciationQueryApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newFutureStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                BoxOfficeSaleQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newFutureStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(ExportDepreciationsRequest.getDefaultInstance());
        var response = application.exportDepreciations(request);
        var sent0 = channel.request("acme.risk.v1.RiskScenarioValidationService/PreviewRiskScenario");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/PreviewRiskScenario"), "risk_scenario", "audit", "created_by"),
                ProbeSupport.value(sent1, "asset_transfer", "audit", "created_by"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetValidationService/ValidateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/PreviewRiskScenario"), "risk_scenario", "audit", "updated_at"),
                ProbeSupport.value(sent2, "fixed_asset", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer"), "asset_transfer", "audit", "updated_by"),
                ProbeSupport.value(sent2, "fixed_asset", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskScenarioValidationService/PreviewRiskScenario"), "risk_scenario", "assignee", "email"),
                ProbeSupport.value(response, "depreciation", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer"), "asset_transfer", "billing_address", "lines"),
                ProbeSupport.value(response, "depreciation", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ValidateFixedAsset"), "fixed_asset", "owner", "display_name"),
                ProbeSupport.value(response, "depreciation", "organizer", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "depreciation", "location", "region"));
        var endpoint = new DepreciationQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportDepreciationsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportDepreciationsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportDepreciations(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeDepreciationsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DepreciationQueryApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newFutureStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                BoxOfficeSaleQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ClaimQueryServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskScenarioValidationServiceGrpc.newFutureStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SummarizeDepreciationsRequest.getDefaultInstance());
        var response = application.summarizeDepreciations(request);
        var sent0 = channel.request("acme.lending.v1.CollateralValidationService/NormalizeCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        var sent1 = channel.request("acme.assets.v1.DisposalValidationService/ScoreDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/NormalizeCollateral"), "collateral", "owner", "user_id"),
                ProbeSupport.value(sent1, "disposal", "assignee", "user_id"));
        var sent2 = channel.request("acme.boxoffice.v1.BoxOfficeSaleQueryService/SuggestBoxOfficeSales");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/NormalizeCollateral"), "collateral", "address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ScoreDisposal"), "disposal", "audit", "updated_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ScoreDisposal"), "disposal", "assignee", "user_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralValidationService/NormalizeCollateral"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ScoreDisposal"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
