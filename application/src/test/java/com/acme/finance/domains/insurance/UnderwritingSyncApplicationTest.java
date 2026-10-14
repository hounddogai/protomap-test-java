package com.acme.finance.domains.insurance;

import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.collections.v1.BatchCreateCollectionCasesRequest;
import com.acme.collections.v1.BatchCreateCollectionCasesResponse;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.community.v1.Forum;
import com.acme.community.v1.ForumServiceGrpc;
import com.acme.community.v1.UpdateForumRequest;
import com.acme.community.v1.UpdateForumResponse;
import com.acme.content.v1.Article;
import com.acme.content.v1.ArticleServiceGrpc;
import com.acme.content.v1.ContentRevisionRef;
import com.acme.content.v1.UpdateArticleRequest;
import com.acme.content.v1.UpdateArticleResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimComment;
import com.acme.insurance.v1.ClaimLifecycleServiceGrpc;
import com.acme.insurance.v1.ComparePremiumOptionsRequest;
import com.acme.insurance.v1.ComparePremiumOptionsResponse;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExpirePolicyRequest;
import com.acme.insurance.v1.ExpirePolicyResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.GetPolicyLifecycleRequest;
import com.acme.insurance.v1.GetPolicyLifecycleResponse;
import com.acme.insurance.v1.GetUnderwritingSyncStateRequest;
import com.acme.insurance.v1.GetUnderwritingSyncStateResponse;
import com.acme.insurance.v1.ListUnderwritingConflictsRequest;
import com.acme.insurance.v1.ListUnderwritingConflictsResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyLifecycleServiceGrpc;
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumAttachment;
import com.acme.insurance.v1.PremiumCalculationServiceGrpc;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.PremiumValidationServiceGrpc;
import com.acme.insurance.v1.PreviewPremiumRequest;
import com.acme.insurance.v1.PreviewPremiumResponse;
import com.acme.insurance.v1.PullPremiumsRequest;
import com.acme.insurance.v1.PullPremiumsResponse;
import com.acme.insurance.v1.PullUnderwritingsRequest;
import com.acme.insurance.v1.PullUnderwritingsResponse;
import com.acme.insurance.v1.PushPremiumRequest;
import com.acme.insurance.v1.PushPremiumResponse;
import com.acme.insurance.v1.PushUnderwritingRequest;
import com.acme.insurance.v1.PushUnderwritingResponse;
import com.acme.insurance.v1.ReconcileUnderwritingsRequest;
import com.acme.insurance.v1.ReconcileUnderwritingsResponse;
import com.acme.insurance.v1.ResolveUnderwritingConflictRequest;
import com.acme.insurance.v1.ResolveUnderwritingConflictResponse;
import com.acme.insurance.v1.ScheduleClaimExpiryRequest;
import com.acme.insurance.v1.ScheduleClaimExpiryResponse;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.insurance.v1.StreamPremiumChangesRequest;
import com.acme.insurance.v1.StreamPremiumChangesResponse;
import com.acme.insurance.v1.StreamUnderwritingChangesRequest;
import com.acme.insurance.v1.StreamUnderwritingChangesResponse;
import com.acme.insurance.v1.SyncUnderwritingsRequest;
import com.acme.insurance.v1.SyncUnderwritingsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.ledger.v1.PostingBatchNotificationServiceGrpc;
import com.acme.ledger.v1.SendPostingBatchTestNoticeRequest;
import com.acme.ledger.v1.SendPostingBatchTestNoticeResponse;
import com.acme.metrics.v1.LintMetricSeriesesRequest;
import com.acme.metrics.v1.LintMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.acme.risk.v1.Exposure;
import com.acme.risk.v1.ExposureBankFunding;
import com.acme.risk.v1.ExposureValidationServiceGrpc;
import com.acme.risk.v1.LintExposuresRequest;
import com.acme.risk.v1.LintExposuresResponse;
import com.acme.risk.v1.ValidateExposureRequest;
import com.acme.risk.v1.ValidateExposureResponse;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class UnderwritingSyncApplicationTest {
    @Test
    void syncUnderwritingsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InsuranceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveClaim(String tenant, Claim value) {}
            public void saveCoverage(String tenant, Coverage value) {}
            public void savePolicy(String tenant, Policy value) {}
            public void savePremium(String tenant, Premium value) {}
            public void saveUnderwriting(String tenant, Underwriting value) {}
        };
        var listener = new UnderwritingSyncApplicationEvents(PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof UnderwritingSyncApplication.ReconcileUnderwritingsStage3Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new UnderwritingSyncApplication(
                FixedAssetServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                ForumServiceGrpc.newBlockingStub(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                events,
                ClaimLifecycleServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SyncUnderwritingsRequest.getDefaultInstance());
        var response = application.syncUnderwritings(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierContractQueryService/CountSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "sla", "due_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "sla", "policy_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "assignee", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        var sent2 = channel.request("acme.insurance.v1.PremiumSyncService/PullPremiums");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "comments", "created_at"),
                ProbeSupport.value(sent2, "premium", "audit", "created_at"));
        var sent3 = channel.request("acme.insurance.v1.PolicyLifecycleService/ExpirePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "sla", "policy_id"),
                ProbeSupport.value(sent3, "policy_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses"), "metric_series", "owner", "email"),
                ProbeSupport.value(sent3, "details", "new_owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "assignee", "display_name"),
                ProbeSupport.value(sent3, "details", "new_owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "assignee", "email"),
                ProbeSupport.value(sent3, "details", "notify_contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PremiumSyncService/PullPremiums"), "outcome", "effective_at"),
                ProbeSupport.value(sent3, "details", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractQueryService/CountSupplierContracts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses"), "metric_series", "owner", "user_id"),
                ProbeSupport.value(response, "underwriting", "assignee", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.metrics.v1.MetricSeriesValidationService/LintMetricSerieses"), "metric_series", "audit", "updated_by"),
                ProbeSupport.value(response, "underwriting", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.PolicyLifecycleService/ExpirePolicy"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting_id"),
                ProbeSupport.value(response, "underwriting", "underwriting_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "underwriting", "custom_fields"),
                ProbeSupport.value(response, "underwriting", "custom_fields"));
    }

    @Test
    void streamUnderwritingChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InsuranceRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveClaim(String tenant, Claim value) {}
            public void saveCoverage(String tenant, Coverage value) {}
            public void savePolicy(String tenant, Policy value) {}
            public void savePremium(String tenant, Premium value) {}
            public void saveUnderwriting(String tenant, Underwriting value) {}
        };
        var listener = new UnderwritingSyncApplicationEvents(PremiumValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof UnderwritingSyncApplication.ReconcileUnderwritingsStage3Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new UnderwritingSyncApplication(
                FixedAssetServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newStub(channel),
                ForumServiceGrpc.newBlockingStub(channel),
                ArticleServiceGrpc.newBlockingStub(channel),
                events,
                ClaimLifecycleServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                PolicyLifecycleServiceGrpc.newBlockingStub(channel),
                PremiumCalculationServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                PostingBatchNotificationServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                ExposureValidationServiceGrpc.newBlockingStub(channel),
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamUnderwritingChangesRequest.getDefaultInstance());
        var response = application.streamUnderwritingChanges(request);
        var sent0 = channel.request("acme.community.v1.ForumService/UpdateForum");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "created_at"),
                ProbeSupport.value(sent0, "forum", "children", "audit", "created_at"));
        var sent1 = channel.request("acme.ledger.v1.PostingBatchNotificationService/SendPostingBatchTestNotice");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "author_email"),
                ProbeSupport.value(sent1, "details", "recipient", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "subject"),
                ProbeSupport.value(response, "underwriting", "subject"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "ack", "sequence"),
                ProbeSupport.value(response, "sequence"));
        var endpoint = new UnderwritingSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamUnderwritingChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamUnderwritingChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamUnderwritingChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new UnderwritingSyncApplicationEvents(PremiumValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PreviewPremiumResponse>();
        listener.on(new UnderwritingSyncApplication.ReconcileUnderwritingsStage3Requested(ProbeSupport.sample(PreviewPremiumRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
