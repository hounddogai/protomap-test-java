package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.CompareDepreciationOptionsRequest;
import com.acme.assets.v1.CompareDepreciationOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateDepreciationRequest;
import com.acme.assets.v1.EstimateDepreciationResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.GetDepreciationRatesRequest;
import com.acme.assets.v1.GetDepreciationRatesResponse;
import com.acme.assets.v1.QuoteDepreciationRequest;
import com.acme.assets.v1.QuoteDepreciationResponse;
import com.acme.assets.v1.RecalculateDepreciationsRequest;
import com.acme.assets.v1.RecalculateDepreciationsResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.assets.v1.SimulateDepreciationRequest;
import com.acme.assets.v1.SimulateDepreciationResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityRequest;
import com.acme.audit.v1.CheckRetentionPolicyEligibilityResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.ExportForumsRequest;
import com.acme.community.v1.ExportForumsResponse;
import com.acme.community.v1.ForumQueryServiceGrpc;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationValidationServiceGrpc;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateAttestationRequest;
import com.acme.compliance.v1.ValidateAttestationResponse;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.deliveries.v1.CloneDeliveryRequest;
import com.acme.deliveries.v1.CloneDeliveryResponse;
import com.acme.deliveries.v1.Delivery;
import com.acme.deliveries.v1.DeliveryExceptionRef;
import com.acme.deliveries.v1.DeliveryLine;
import com.acme.deliveries.v1.DeliveryServiceGrpc;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.BatchGetUsersRequest;
import com.acme.identity.v1.BatchGetUsersResponse;
import com.acme.identity.v1.UpdateUserRequest;
import com.acme.identity.v1.UpdateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.lending.v1.Collateral;
import com.acme.lending.v1.CollateralCalculationServiceGrpc;
import com.acme.lending.v1.EstimateCollateralRequest;
import com.acme.lending.v1.EstimateCollateralResponse;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.yards.v1.CountYardsRequest;
import com.acme.yards.v1.CountYardsResponse;
import com.acme.yards.v1.YardQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DepreciationCalculationApplicationTest {
    @Test
    void estimateDepreciationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DepreciationCalculationApplicationEvents(AttestationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DepreciationCalculationApplication.CalculateDepreciationStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DepreciationCalculationApplication(
                FixedAssetCalculationServiceGrpc.newBlockingStub(channel),
                AuditEventValidationServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                ForumQueryServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                DeliveryServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                CollateralCalculationServiceGrpc.newBlockingStub(channel),
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                new DepreciationCalculationRequestMapperImpl(),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                YardQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(EstimateDepreciationRequest.getDefaultInstance());
        var response = application.estimateDepreciation(request);
        var sent0 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.lending.v1.CollateralCalculationService/EstimateCollateral");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "participants", "contact", "email"),
                ProbeSupport.value(sent1, "collateral", "owner", "email"));
        var sent2 = channel.request("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag"), "feature_flag", "block", "reason"),
                ProbeSupport.value(sent2, "feature_flag", "block", "reason"));
        var sent3 = channel.request("acme.deliveries.v1.DeliveryService/CloneDelivery");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lending.v1.CollateralCalculationService/EstimateCollateral"), "collateral", "owner", "display_name"),
                ProbeSupport.value(sent3, "delivery", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/ValidateFeatureFlag"), "feature_flag", "discount", "amount"),
                ProbeSupport.value(sent3, "delivery", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag"), "feature_flag", "owner", "user_id"),
                ProbeSupport.value(response, "depreciation", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featureflags.v1.FeatureFlagValidationService/NormalizeFeatureFlag"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.deliveries.v1.DeliveryService/CloneDelivery"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.deliveries.v1.DeliveryService/CloneDelivery"), "delivery", "total"),
                ProbeSupport.value(response, "breakdown", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation", "location"),
                ProbeSupport.value(response, "depreciation", "location"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DepreciationCalculationApplicationEvents(AttestationValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateAttestationResponse>();
        listener.on(new DepreciationCalculationApplication.CalculateDepreciationStage0Requested(ProbeSupport.sample(ValidateAttestationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
