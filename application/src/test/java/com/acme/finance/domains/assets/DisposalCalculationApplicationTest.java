package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferLine;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CalculateDisposalRequest;
import com.acme.assets.v1.CalculateDisposalResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.DepreciationWeeklyRule;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.EstimateDepreciationRequest;
import com.acme.assets.v1.EstimateDepreciationResponse;
import com.acme.assets.v1.EstimateDisposalRequest;
import com.acme.assets.v1.EstimateDisposalResponse;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainDisposalCalculationRequest;
import com.acme.assets.v1.ExplainDisposalCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetDisposalRatesRequest;
import com.acme.assets.v1.GetDisposalRatesResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.QuoteDisposalRequest;
import com.acme.assets.v1.QuoteDisposalResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.assets.v1.RecalculateDisposalsRequest;
import com.acme.assets.v1.RecalculateDisposalsResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ScoreFixedAssetRequest;
import com.acme.assets.v1.ScoreFixedAssetResponse;
import com.acme.assets.v1.SimulateDisposalRequest;
import com.acme.assets.v1.SimulateDisposalResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.StreamAuditTrailExportRequest;
import com.acme.audit.v1.StreamAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.ExportFeatureFlagsRequest;
import com.acme.featureflags.v1.ExportFeatureFlagsResponse;
import com.acme.featureflags.v1.FeatureFlagQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.NormalizePremiumRequest;
import com.acme.insurance.v1.NormalizePremiumResponse;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsRequest;
import com.acme.ledger.v1.CompareFiscalPeriodVersionsResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.risk.v1.CheckRiskRegisterEligibilityRequest;
import com.acme.risk.v1.CheckRiskRegisterEligibilityResponse;
import com.acme.risk.v1.ExplainRiskRegisterDecisionRequest;
import com.acme.risk.v1.ExplainRiskRegisterDecisionResponse;
import com.acme.risk.v1.RiskRegister;
import com.acme.risk.v1.RiskRegisterValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import java.util.ArrayList;
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

class DisposalCalculationApplicationTest {
    @Test
    void simulateDisposalPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DisposalCalculationApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                FeatureFlagQueryServiceGrpc.newBlockingStub(channel),
                PremiumValidationServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                new DisposalCalculationRequestMapperImpl(),
                repository,
                RiskRegisterValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(SimulateDisposalRequest.getDefaultInstance());
        var response = application.simulateDisposal(request);
        var sent0 = channel.request("acme.assets.v1.DepreciationCalculationService/QuoteDepreciation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "requester", "name"),
                ProbeSupport.value(sent0, "depreciation", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "destination", "postal_code"),
                ProbeSupport.value(sent0, "depreciation", "location", "postal_code"));
        var sent1 = channel.request("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "assignee", "email"),
                ProbeSupport.value(sent1, "risk_register", "children", "children", "manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "audit", "updated_by"),
                ProbeSupport.value(sent1, "risk_register", "children", "children", "audit", "updated_by"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterValidationService/ExplainRiskRegisterDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "comments", "created_at"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "comments", "author_email"),
                ProbeSupport.value(sent2, "risk_register", "children", "children", "manager", "email"));
        var sent3 = channel.request("acme.assets.v1.DepreciationCalculationService/CompareDepreciationOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/ExplainRiskRegisterDecision"), "risk_register", "audit", "updated_by"),
                ProbeSupport.value(sent3, "depreciation", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/ExplainRiskRegisterDecision"), "risk_register", "location", "region"),
                ProbeSupport.value(sent3, "depreciation", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationCalculationService/QuoteDepreciation"), "depreciation", "audit", "created_by"),
                ProbeSupport.value(response, "disposal", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.RiskRegisterValidationService/CheckRiskRegisterEligibility"), "risk_register", "audit", "updated_at"),
                ProbeSupport.value(response, "disposal", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationCalculationService/CompareDepreciationOptions"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal", "custom_fields"),
                ProbeSupport.value(response, "disposal", "custom_fields"));
    }
}
