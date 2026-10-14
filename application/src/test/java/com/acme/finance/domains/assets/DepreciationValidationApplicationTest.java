package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainDepreciationDecisionRequest;
import com.acme.assets.v1.ExplainDepreciationDecisionResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDepreciationRulesRequest;
import com.acme.assets.v1.GetDepreciationRulesResponse;
import com.acme.assets.v1.LintDepreciationsRequest;
import com.acme.assets.v1.LintDepreciationsResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.NormalizeDepreciationRequest;
import com.acme.assets.v1.NormalizeDepreciationResponse;
import com.acme.assets.v1.PreviewDepreciationRequest;
import com.acme.assets.v1.PreviewDepreciationResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.ValidateDepreciationRequest;
import com.acme.assets.v1.ValidateDepreciationResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.NormalizeAccessPolicyRequest;
import com.acme.authorization.v1.NormalizeAccessPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PreviewUnderwritingRequest;
import com.acme.insurance.v1.PreviewUnderwritingResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.lending.v1.ExportLoanApplicationsRequest;
import com.acme.lending.v1.ExportLoanApplicationsResponse;
import com.acme.lending.v1.LoanApplicationQueryServiceGrpc;
import com.acme.risk.v1.ExplainExposureCalculationRequest;
import com.acme.risk.v1.ExplainExposureCalculationResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.tours.v1.CountToursRequest;
import com.acme.tours.v1.CountToursResponse;
import com.acme.tours.v1.TourQueryServiceGrpc;
import com.acme.venues.v1.BrowseVenuesRequest;
import com.acme.venues.v1.BrowseVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.workplaces.v1.BuildingServiceGrpc;
import com.acme.workplaces.v1.WatchBuildingsRequest;
import com.acme.workplaces.v1.WatchBuildingsResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
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

class DepreciationValidationApplicationTest {
    @Test
    void validateDepreciationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DepreciationValidationApplication(
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                AccessPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                LoanApplicationQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                TourQueryServiceGrpc.newBlockingStub(channel),
                VenueQueryServiceGrpc.newBlockingStub(channel),
                BuildingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ValidateDepreciationRequest.getDefaultInstance());
        var response = application.validateDepreciation(request);
        var sent0 = channel.request("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "compliance_case", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent0, "compliance_case", "assignee", "email"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetCalculationService/RecalculateFixedAssets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase"), "compliance_case", "audit", "created_by"),
                ProbeSupport.value(sent1, "fixed_asset", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.ComplianceCaseCalculationService/QuoteComplianceCase"), "compliance_case", "assignee", "email"),
                ProbeSupport.value(sent1, "fixed_asset", "owner", "email"));
        var sent2 = channel.request("acme.assets.v1.FixedAssetValidationService/ScoreFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent2, "fixed_asset", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/RecalculateFixedAssets"), "fixed_asset", "components", "description"),
                ProbeSupport.value(sent2, "fixed_asset", "components", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/RecalculateFixedAssets"), "fixed_asset", "owner", "user_id"),
                ProbeSupport.value(sent2, "fixed_asset", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "location", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        var sent3 = channel.request("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent3, "fixed_asset", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "audit", "updated_by"),
                ProbeSupport.value(sent3, "fixed_asset", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/RecalculateFixedAssets"), "fixed_asset", "audit", "updated_at"),
                ProbeSupport.value(response, "depreciation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/RecalculateFixedAssets"), "fixed_asset", "address", "country_code"),
                ProbeSupport.value(response, "depreciation", "location", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "metadata"),
                ProbeSupport.value(response, "depreciation", "metadata"));
    }
}
