package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CountDisposalTotalsRequest;
import com.acme.assets.v1.CountDisposalTotalsResponse;
import com.acme.assets.v1.DeleteDisposalReportRequest;
import com.acme.assets.v1.DeleteDisposalReportResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FetchDisposalReportRequest;
import com.acme.assets.v1.FetchDisposalReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.GenerateDisposalReportRequest;
import com.acme.assets.v1.GenerateDisposalReportResponse;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.ListDisposalReportsRequest;
import com.acme.assets.v1.ListDisposalReportsResponse;
import com.acme.assets.v1.ScheduleDisposalReportRequest;
import com.acme.assets.v1.ScheduleDisposalReportResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.ShareDisposalReportRequest;
import com.acme.assets.v1.ShareDisposalReportResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.StreamDisposalMetricsRequest;
import com.acme.assets.v1.StreamDisposalMetricsResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.audit.v1.AuditEventQueryServiceGrpc;
import com.acme.audit.v1.CountAuditEventsRequest;
import com.acme.audit.v1.CountAuditEventsResponse;
import com.acme.audit.v1.SuggestAuditEventsRequest;
import com.acme.audit.v1.SuggestAuditEventsResponse;
import com.acme.collections.v1.CommentWriteOffApprovalRequest;
import com.acme.collections.v1.CommentWriteOffApprovalResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagBlockAction;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.NormalizeFeatureFlagRequest;
import com.acme.featureflags.v1.NormalizeFeatureFlagResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.venues.v1.ExplainVenueDecisionRequest;
import com.acme.venues.v1.ExplainVenueDecisionResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DisposalReportingApplicationTest {
    @Test
    void streamDisposalMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DisposalReportingApplicationEvents(VenueValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DisposalReportingApplication.ShareDisposalReportStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof DisposalReportingApplication.DeleteDisposalReportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DisposalReportingApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamDisposalMetricsRequest.getDefaultInstance());
        var response = application.streamDisposalMetrics(request);
        var sent0 = channel.request("acme.assets.v1.DisposalValidationService/ValidateDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "disposal", "requester", "phone"));
        var sent1 = channel.request("acme.assets.v1.DepreciationReportingService/StreamDepreciationMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ValidateDisposal"), "disposal", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ValidateDisposal"), "disposal", "requester", "email"),
                ProbeSupport.value(sent1, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ValidateDisposal"), "disposal", "requester", "name"),
                ProbeSupport.value(sent1, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ValidateDisposal"), "disposal", "requester", "preferred_channel"),
                ProbeSupport.value(response, "disposal", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "disposal", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(response, "disposal", "requester", "phone"));
        var endpoint = new DisposalReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamDisposalMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamDisposalMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamDisposalMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void shareDisposalReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DisposalReportingApplicationEvents(VenueValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DisposalReportingApplication.ShareDisposalReportStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof DisposalReportingApplication.DeleteDisposalReportStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DisposalReportingApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                AuditEventQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagValidationServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ShareDisposalReportRequest.getDefaultInstance());
        var response = application.shareDisposalReport(request);
        var sent0 = channel.request("acme.venues.v1.VenueValidationService/ValidateVenue");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        var sent1 = channel.request("acme.collections.v1.WriteOffApprovalService/CommentWriteOffApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "audit", "updated_by"),
                ProbeSupport.value(sent1, "write_off", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "address", "region"),
                ProbeSupport.value(sent1, "write_off", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "owner", "email"),
                ProbeSupport.value(sent1, "details", "approver", "email"));
        var sent2 = channel.request("acme.assets.v1.DepreciationReportingService/ShareDepreciationReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent2, "details", "recipient", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(sent2, "details", "recipient", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "recipient", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "owner", "email"),
                ProbeSupport.value(sent2, "details", "recipient", "email"));
        var sent3 = channel.request("acme.assets.v1.DisposalSyncService/GetDisposalSyncState");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "disposal_id"),
                ProbeSupport.value(sent3, "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "venue", "owner", "display_name"),
                ProbeSupport.value(response, "disposal", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/ValidateVenue"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.WriteOffApprovalService/CommentWriteOffApproval"), "outcome", "reference_id"),
                ProbeSupport.value(response, "outcome", "reference_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalSyncService/GetDisposalSyncState"), "disposal", "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(response, "disposal", "requester", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(response, "disposal", "requester", "phone"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DisposalReportingApplicationEvents(VenueValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ValidateVenueResponse>();
        listener.on(new DisposalReportingApplication.ShareDisposalReportStage0Requested(ProbeSupport.sample(ValidateVenueRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ExplainVenueDecisionResponse>();
        listener.on(new DisposalReportingApplication.DeleteDisposalReportStage0Requested(ProbeSupport.sample(ExplainVenueDecisionRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
