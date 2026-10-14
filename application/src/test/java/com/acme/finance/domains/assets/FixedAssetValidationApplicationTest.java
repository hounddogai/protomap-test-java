package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.GetFixedAssetRulesRequest;
import com.acme.assets.v1.GetFixedAssetRulesResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.PreviewFixedAssetRequest;
import com.acme.assets.v1.PreviewFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.ScoreMandateRequest;
import com.acme.banking.v1.ScoreMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.PreviewSanctionRequest;
import com.acme.compliance.v1.PreviewSanctionResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.devices.v1.SimulateManagedDeviceRequest;
import com.acme.devices.v1.SimulateManagedDeviceResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersRequest;
import com.acme.ledger.v1.NotifyLedgerAccountWatchersResponse;
import com.acme.lending.v1.RecalculateRepaymentsRequest;
import com.acme.lending.v1.RecalculateRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentCalculationServiceGrpc;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.risk.v1.ExplainStressTestDecisionRequest;
import com.acme.risk.v1.ExplainStressTestDecisionResponse;
import com.acme.risk.v1.ResolveRiskRegistersRequest;
import com.acme.risk.v1.ResolveRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.StressTest;
import com.acme.risk.v1.StressTestLine;
import com.acme.risk.v1.StressTestValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class FixedAssetValidationApplicationTest {
    @Test
    void scoreFixedAssetPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetValidationApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                ManagedDeviceCalculationServiceGrpc.newBlockingStub(channel),
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                RepaymentCalculationServiceGrpc.newBlockingStub(channel),
                new FixedAssetValidationRequestMapperImpl(),
                repository,
                RiskRegisterQueryServiceGrpc.newBlockingStub(channel),
                StressTestValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ScoreFixedAssetRequest.getDefaultInstance());
        var response = application.scoreFixedAsset(request);
        var sent0 = channel.request("acme.devices.v1.ManagedDeviceCalculationService/SimulateManagedDevice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "postal_code"),
                ProbeSupport.value(sent0, "managed_device", "address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "audit", "created_at"),
                ProbeSupport.value(sent0, "managed_device", "audit", "created_at"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterQueryService/ResolveRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetCalculationService/ExplainFixedAssetCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "audit", "updated_at"),
                ProbeSupport.value(sent2, "fixed_asset", "audit", "updated_at"));
        var sent3 = channel.request("acme.compliance.v1.SanctionValidationService/PreviewSanction");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/ExplainFixedAssetCalculation"), "fixed_asset", "address", "lines"),
                ProbeSupport.value(sent3, "sanction", "children", "children", "location", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/ExplainFixedAssetCalculation"), "fixed_asset", "address", "locality"),
                ProbeSupport.value(sent3, "sanction", "children", "children", "location", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.devices.v1.ManagedDeviceCalculationService/SimulateManagedDevice"), "managed_device", "address", "postal_code"),
                ProbeSupport.value(sent3, "sanction", "children", "children", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.devices.v1.ManagedDeviceCalculationService/SimulateManagedDevice"), "managed_device", "owner", "display_name"),
                ProbeSupport.value(sent3, "sanction", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.devices.v1.ManagedDeviceCalculationService/SimulateManagedDevice"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.devices.v1.ManagedDeviceCalculationService/SimulateManagedDevice"), "breakdown", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterQueryService/ResolveRiskRegisters"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/ExplainFixedAssetCalculation"), "fixed_asset", "components", "component_id"),
                ProbeSupport.value(response, "fixed_asset", "components", "component_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.SanctionValidationService/PreviewSanction"), "sanction", "audit", "etag"),
                ProbeSupport.value(response, "fixed_asset", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset_id"),
                ProbeSupport.value(response, "fixed_asset", "fixed_asset_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "code"),
                ProbeSupport.value(response, "fixed_asset", "code"));
    }
}
