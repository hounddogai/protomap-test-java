package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateAssetTransferRequest;
import com.acme.assets.v1.CalculateAssetTransferResponse;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CompareAssetTransferOptionsRequest;
import com.acme.assets.v1.CompareAssetTransferOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExplainAssetTransferCalculationRequest;
import com.acme.assets.v1.ExplainAssetTransferCalculationResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetRef;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetAssetTransferRatesRequest;
import com.acme.assets.v1.GetAssetTransferRatesResponse;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.SimulateAssetTransferRequest;
import com.acme.assets.v1.SimulateAssetTransferResponse;
import com.acme.assets.v1.ValidateFixedAssetRequest;
import com.acme.assets.v1.ValidateFixedAssetResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailSyncServiceGrpc;
import com.acme.audit.v1.ResolveAuditTrailConflictRequest;
import com.acme.audit.v1.ResolveAuditTrailConflictResponse;
import com.acme.authorization.v1.AccessPolicyQueryServiceGrpc;
import com.acme.authorization.v1.ExportAccessPoliciesRequest;
import com.acme.authorization.v1.ExportAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BrowseComplianceCasesRequest;
import com.acme.compliance.v1.BrowseComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityRequest;
import com.acme.featureflags.v1.CheckFeatureFlagEligibilityResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagDiscountAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.helpdesk.v1.ListTicketsRequest;
import com.acme.helpdesk.v1.ListTicketsResponse;
import com.acme.helpdesk.v1.TicketServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportRequest;
import com.acme.ledger.v1.ScheduleFiscalPeriodExportResponse;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.risk.v1.RiskScenario;
import com.acme.risk.v1.RiskScenarioValidationServiceGrpc;
import com.acme.risk.v1.ValidateRiskScenarioRequest;
import com.acme.risk.v1.ValidateRiskScenarioResponse;
import com.acme.seating.v1.RecalculateSeatMapsRequest;
import com.acme.seating.v1.RecalculateSeatMapsResponse;
import com.acme.seating.v1.SeatMap;
import com.acme.seating.v1.SeatMapCalculationServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class AssetTransferCalculationApplicationTest {
    @Test
    void explainAssetTransferCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new AssetTransferCalculationApplication(
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailSyncServiceGrpc.newBlockingStub(channel),
                AccessPolicyQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseQueryServiceGrpc.newBlockingStub(channel),
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                TicketServiceGrpc.newBlockingStub(channel),
                new AssetTransferCalculationRequestMapperImpl(),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                RiskScenarioValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SeatMapCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainAssetTransferCalculationRequest.getDefaultInstance());
        var response = application.explainAssetTransferCalculation(request);
        var sent0 = channel.request("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "audit", "etag"),
                ProbeSupport.value(sent0, "audit_trail", "audit", "etag"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetValidationService/ValidateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "lines", "quantity"),
                ProbeSupport.value(sent1, "fixed_asset", "components", "quantity"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "lines", "description"),
                ProbeSupport.value(sent1, "fixed_asset", "components", "description"));
        var sent2 = channel.request("acme.helpdesk.v1.TicketService/ListTickets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/ValidateFixedAsset"), "fixed_asset", "address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "lines", "line_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        var sent3 = channel.request("acme.assets.v1.FixedAssetCalculationService/CalculateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "billing_address", "region"),
                ProbeSupport.value(sent3, "fixed_asset", "address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailSyncService/ResolveAuditTrailConflict"), "audit_trail", "total"),
                ProbeSupport.value(response, "asset_transfer", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/CalculateFixedAsset"), "fixed_asset", "audit", "created_at"),
                ProbeSupport.value(response, "asset_transfer", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer_id"),
                ProbeSupport.value(response, "asset_transfer", "asset_transfer_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "asset_transfer", "counterparty"),
                ProbeSupport.value(response, "asset_transfer", "counterparty"));
    }
}
