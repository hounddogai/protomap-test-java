package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.BrowseFixedAssetsRequest;
import com.acme.assets.v1.BrowseFixedAssetsResponse;
import com.acme.assets.v1.CountFixedAssetsRequest;
import com.acme.assets.v1.CountFixedAssetsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.DepreciationWeeklyRule;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.ExplainAssetTransferDecisionRequest;
import com.acme.assets.v1.ExplainAssetTransferDecisionResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.FindFixedAssetsRequest;
import com.acme.assets.v1.FindFixedAssetsResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.LookupFixedAssetRequest;
import com.acme.assets.v1.LookupFixedAssetResponse;
import com.acme.assets.v1.ResolveFixedAssetsRequest;
import com.acme.assets.v1.ResolveFixedAssetsResponse;
import com.acme.assets.v1.SummarizeFixedAssetsRequest;
import com.acme.assets.v1.SummarizeFixedAssetsResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.RetentionPolicyExportServiceGrpc;
import com.acme.audit.v1.ShareRetentionPolicyExportRequest;
import com.acme.audit.v1.ShareRetentionPolicyExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralValidationServiceGrpc;
import com.acme.lending.v1.ScoreCollateralRequest;
import com.acme.lending.v1.ScoreCollateralResponse;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FixedAssetQueryApplicationTest {
    @Test
    void summarizeFixedAssetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetQueryApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyExportServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                CollateralValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(SummarizeFixedAssetsRequest.getDefaultInstance());
        var response = application.summarizeFixedAssets(request);
        var sent0 = channel.request("acme.assets.v1.DepreciationValidationService/ValidateDepreciation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "depreciation", "participants", "contact", "email"));
        var sent1 = channel.request("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "organizer", "email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        var sent2 = channel.request("acme.assets.v1.AssetTransferValidationService/ExplainAssetTransferDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "organizer", "display_name"),
                ProbeSupport.value(sent2, "asset_transfer", "counterparty", "display_name"));
        var sent3 = channel.request("acme.assets.v1.DepreciationCalculationService/ExplainDepreciationCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "retention_policy", "owner", "email"),
                ProbeSupport.value(sent3, "depreciation", "participants", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "audit", "created_at"),
                ProbeSupport.value(sent3, "depreciation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "depreciation", "daily", "interval"),
                ProbeSupport.value(sent3, "depreciation", "weekly", "interval"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ValidateDepreciation"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyExportService/ShareRetentionPolicyExport"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferValidationService/ExplainAssetTransferDecision"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
