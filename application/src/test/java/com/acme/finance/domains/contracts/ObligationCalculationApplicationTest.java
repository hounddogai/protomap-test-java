package com.acme.finance.domains.contracts;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.LintDepreciationsRequest;
import com.acme.assets.v1.LintDepreciationsResponse;
import com.acme.assets.v1.RecalculateAssetTransfersRequest;
import com.acme.assets.v1.RecalculateAssetTransfersResponse;
import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.BatchCreateAuditEventsRequest;
import com.acme.audit.v1.BatchCreateAuditEventsResponse;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.NormalizeRetentionPolicyRequest;
import com.acme.audit.v1.NormalizeRetentionPolicyResponse;
import com.acme.audit.v1.ReconcileRetentionPoliciesRequest;
import com.acme.audit.v1.ReconcileRetentionPoliciesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.StreamAuditEventNoticesRequest;
import com.acme.audit.v1.StreamAuditEventNoticesResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesRequest;
import com.acme.audit.v1.SubscribeRetentionPolicyUpdatesResponse;
import com.acme.audit.v1.ValidateRetentionPolicyRequest;
import com.acme.audit.v1.ValidateRetentionPolicyResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.contracts.v1.CalculateObligationRequest;
import com.acme.contracts.v1.CalculateObligationResponse;
import com.acme.contracts.v1.CompareObligationOptionsRequest;
import com.acme.contracts.v1.CompareObligationOptionsResponse;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractClause;
import com.acme.contracts.v1.EstimateObligationRequest;
import com.acme.contracts.v1.EstimateObligationResponse;
import com.acme.contracts.v1.ExplainObligationCalculationRequest;
import com.acme.contracts.v1.ExplainObligationCalculationResponse;
import com.acme.contracts.v1.GetObligationRatesRequest;
import com.acme.contracts.v1.GetObligationRatesResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.QuoteObligationRequest;
import com.acme.contracts.v1.QuoteObligationResponse;
import com.acme.contracts.v1.RecalculateObligationsRequest;
import com.acme.contracts.v1.RecalculateObligationsResponse;
import com.acme.contracts.v1.Renewal;
import com.acme.contracts.v1.SimulateObligationRequest;
import com.acme.contracts.v1.SimulateObligationResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CalculateCoverageRequest;
import com.acme.insurance.v1.CalculateCoverageResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.PreviewCoverageRequest;
import com.acme.insurance.v1.PreviewCoverageResponse;
import com.acme.insurance.v1.RecalculateClaimsRequest;
import com.acme.insurance.v1.RecalculateClaimsResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingComment;
import com.acme.insurance.v1.UnderwritingSla;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.insurance.v1.ValidateUnderwritingRequest;
import com.acme.insurance.v1.ValidateUnderwritingResponse;
import com.acme.pipelines.v1.CalculatePipelineRequest;
import com.acme.pipelines.v1.CalculatePipelineResponse;
import com.acme.pipelines.v1.Pipeline;
import com.acme.pipelines.v1.PipelineCalculationServiceGrpc;
import com.acme.pipelines.v1.PipelineParticipant;
import com.acme.surveys.v1.BatchGetSurveysRequest;
import com.acme.surveys.v1.BatchGetSurveysResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class ObligationCalculationApplicationTest {
    @Test
    void explainObligationCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ContractsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveContract(String tenant, Contract value) {}
            public void saveContractClause(String tenant, ContractClause value) {}
            public void saveObligation(String tenant, Obligation value) {}
            public void saveRenewal(String tenant, Renewal value) {}
        };
        var listener = new ObligationCalculationApplicationEvents(ClaimCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof ObligationCalculationApplication.SimulateObligationStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new ObligationCalculationApplication(
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                AuditEventNotificationServiceGrpc.newBlockingStub(channel),
                AuditEventServiceGrpc.newStub(channel),
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicySyncServiceGrpc.newBlockingStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                CoverageCalculationServiceGrpc.newBlockingStub(channel),
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                UnderwritingValidationServiceGrpc.newBlockingStub(channel),
                PipelineCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExplainObligationCalculationRequest.getDefaultInstance());
        var response = application.explainObligationCalculation(request);
        var sent0 = channel.request("acme.assets.v1.AssetTransferCalculationService/RecalculateAssetTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "updated_by"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "etag"),
                ProbeSupport.value(sent0, "asset_transfer", "audit", "etag"));
        var sent1 = channel.request("acme.insurance.v1.CoverageCalculationService/CalculateCoverage");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "created_at"),
                ProbeSupport.value(sent1, "coverage", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "audit", "updated_at"),
                ProbeSupport.value(sent1, "coverage", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/RecalculateAssetTransfers"), "asset_transfer", "audit", "etag"),
                ProbeSupport.value(sent1, "coverage", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "assignee", "user_id"),
                ProbeSupport.value(sent1, "coverage", "owner", "user_id"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyValidationService/NormalizeRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "comments", "created_at"),
                ProbeSupport.value(sent2, "retention_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "comments", "author_email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/RecalculateAssetTransfers"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CalculateCoverage"), "coverage", "priority"),
                ProbeSupport.value(response, "obligation", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.CoverageCalculationService/CalculateCoverage"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation_id"),
                ProbeSupport.value(response, "obligation", "obligation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "obligation", "custom_fields"),
                ProbeSupport.value(response, "obligation", "custom_fields"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new ObligationCalculationApplicationEvents(ClaimCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RecalculateClaimsResponse>();
        listener.on(new ObligationCalculationApplication.SimulateObligationStage0Requested(ProbeSupport.sample(RecalculateClaimsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
