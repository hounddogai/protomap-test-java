package com.acme.finance.domains.assets;

import com.acme.assets.v1.AdvanceFixedAssetWorkflowRequest;
import com.acme.assets.v1.AdvanceFixedAssetWorkflowResponse;
import com.acme.assets.v1.ApproveDepreciationRequest;
import com.acme.assets.v1.ApproveDepreciationResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CommentFixedAssetApprovalRequest;
import com.acme.assets.v1.CommentFixedAssetApprovalResponse;
import com.acme.assets.v1.CompareDisposalOptionsRequest;
import com.acme.assets.v1.CompareDisposalOptionsResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalCalculationServiceGrpc;
import com.acme.assets.v1.DisposalReportingServiceGrpc;
import com.acme.assets.v1.ExpireFixedAssetRequest;
import com.acme.assets.v1.ExpireFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetApprovalServiceGrpc;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.InspectFixedAssetWorkflowRequest;
import com.acme.assets.v1.InspectFixedAssetWorkflowResponse;
import com.acme.assets.v1.ListFixedAssetWorkflowsRequest;
import com.acme.assets.v1.ListFixedAssetWorkflowsResponse;
import com.acme.assets.v1.ResumeFixedAssetWorkflowRequest;
import com.acme.assets.v1.ResumeFixedAssetWorkflowResponse;
import com.acme.assets.v1.RetryFixedAssetWorkflowRequest;
import com.acme.assets.v1.RetryFixedAssetWorkflowResponse;
import com.acme.assets.v1.SignalFixedAssetWorkflowRequest;
import com.acme.assets.v1.SignalFixedAssetWorkflowResponse;
import com.acme.assets.v1.StartFixedAssetWorkflowRequest;
import com.acme.assets.v1.StartFixedAssetWorkflowResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.audit.v1.ResolveRetentionPolicyConflictRequest;
import com.acme.audit.v1.ResolveRetentionPolicyConflictResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featurestore.v1.FeatureView;
import com.acme.featurestore.v1.FeatureViewCalculationServiceGrpc;
import com.acme.featurestore.v1.FeatureViewDimensions;
import com.acme.featurestore.v1.QuoteFeatureViewRequest;
import com.acme.featurestore.v1.QuoteFeatureViewResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.PolicyApprovalServiceGrpc;
import com.acme.insurance.v1.RecallPolicyRequest;
import com.acme.insurance.v1.RecallPolicyResponse;
import com.acme.licensing.v1.FindSoftwareLicensesRequest;
import com.acme.licensing.v1.FindSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseQueryServiceGrpc;
import com.acme.metrics.v1.MetricSeriesServiceGrpc;
import com.acme.metrics.v1.WatchMetricSeriesesRequest;
import com.acme.metrics.v1.WatchMetricSeriesesResponse;
import com.acme.onboarding.v1.CountOnboardingPlansRequest;
import com.acme.onboarding.v1.CountOnboardingPlansResponse;
import com.acme.onboarding.v1.OnboardingPlanQueryServiceGrpc;
import com.acme.routing.v1.ExportRoutesRequest;
import com.acme.routing.v1.ExportRoutesResponse;
import com.acme.routing.v1.RouteQueryServiceGrpc;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class FixedAssetWorkflowApplicationTest {
    @Test
    void retryFixedAssetWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetWorkflowApplication(
                DisposalCalculationServiceGrpc.newBlockingStub(channel),
                DisposalReportingServiceGrpc.newBlockingStub(channel),
                FixedAssetApprovalServiceGrpc.newBlockingStub(channel),
                FixedAssetLifecycleServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                FeatureViewCalculationServiceGrpc.newBlockingStub(channel),
                PolicyApprovalServiceGrpc.newBlockingStub(channel),
                SoftwareLicenseQueryServiceGrpc.newBlockingStub(channel),
                MetricSeriesServiceGrpc.newBlockingStub(channel),
                OnboardingPlanQueryServiceGrpc.newBlockingStub(channel),
                repository,
                RouteQueryServiceGrpc.newBlockingStub(channel),
                Runnable::run);
        var request = ProbeSupport.sample(RetryFixedAssetWorkflowRequest.getDefaultInstance());
        var response = application.retryFixedAssetWorkflow(request);
        var sent0 = channel.request("acme.featurestore.v1.FeatureViewCalculationService/QuoteFeatureView");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "dimensions", "weight"),
                ProbeSupport.value(sent0, "feature_view", "dimensions", "weight"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner", "email"),
                ProbeSupport.value(sent0, "feature_view", "owner", "email"));
        var sent1 = channel.request("acme.onboarding.v1.OnboardingPlanQueryService/CountOnboardingPlans");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewCalculationService/QuoteFeatureView"), "feature_view", "specs"),
                ProbeSupport.value(sent1, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewCalculationService/QuoteFeatureView"), "result", "correlation_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.assets.v1.DisposalReportingService/StreamDisposalMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent2, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent2, "subscriber", "preferred_channel"));
        var sent3 = channel.request("acme.assets.v1.FixedAssetLifecycleService/ExpireFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset_id"),
                ProbeSupport.value(sent3, "fixed_asset_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "name"),
                ProbeSupport.value(sent3, "details", "notify_contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewCalculationService/QuoteFeatureView"), "feature_view", "owner", "email"),
                ProbeSupport.value(sent3, "details", "notify_contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.featurestore.v1.FeatureViewCalculationService/QuoteFeatureView"), "feature_view", "audit", "etag"),
                ProbeSupport.value(response, "fixed_asset", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.onboarding.v1.OnboardingPlanQueryService/CountOnboardingPlans"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset_id"),
                ProbeSupport.value(response, "fixed_asset", "fixed_asset_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "specs"),
                ProbeSupport.value(response, "fixed_asset", "specs"));
    }
}
