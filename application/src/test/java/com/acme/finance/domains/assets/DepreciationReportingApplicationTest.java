package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferBankSettlement;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CheckAssetTransferEligibilityRequest;
import com.acme.assets.v1.CheckAssetTransferEligibilityResponse;
import com.acme.assets.v1.CheckDepreciationEligibilityRequest;
import com.acme.assets.v1.CheckDepreciationEligibilityResponse;
import com.acme.assets.v1.DeleteDepreciationReportRequest;
import com.acme.assets.v1.DeleteDepreciationReportResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.DepreciationValidationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.FetchDepreciationReportRequest;
import com.acme.assets.v1.FetchDepreciationReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.assets.v1.GenerateDepreciationReportRequest;
import com.acme.assets.v1.GenerateDepreciationReportResponse;
import com.acme.assets.v1.GetDepreciationRulesRequest;
import com.acme.assets.v1.GetDepreciationRulesResponse;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.assets.v1.PeekDepreciationRequest;
import com.acme.assets.v1.PeekDepreciationResponse;
import com.acme.assets.v1.PullAssetTransfersRequest;
import com.acme.assets.v1.PullAssetTransfersResponse;
import com.acme.assets.v1.QuoteAssetTransferRequest;
import com.acme.assets.v1.QuoteAssetTransferResponse;
import com.acme.assets.v1.ScoreDepreciationRequest;
import com.acme.assets.v1.ScoreDepreciationResponse;
import com.acme.assets.v1.ShareDepreciationReportRequest;
import com.acme.assets.v1.ShareDepreciationReportResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ListAuditTrailExportsRequest;
import com.acme.audit.v1.ListAuditTrailExportsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CloneUserRequest;
import com.acme.identity.v1.CloneUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.insurance.v1.CoverageReportingServiceGrpc;
import com.acme.insurance.v1.ScheduleCoverageReportRequest;
import com.acme.insurance.v1.ScheduleCoverageReportResponse;
import com.acme.ledger.v1.FiscalPeriodNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.RevertLedgerAccountRequest;
import com.acme.ledger.v1.RevertLedgerAccountResponse;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesRequest;
import com.acme.ledger.v1.StreamFiscalPeriodNoticesResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class DepreciationReportingApplicationTest {
    @Test
    void streamDepreciationMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DepreciationReportingApplicationEvents(DepreciationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DepreciationReportingApplication.ShareDepreciationReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DepreciationReportingApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newFutureStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                new DepreciationReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(StreamDepreciationMetricsRequest.getDefaultInstance());
        var response = application.streamDepreciationMetrics(request);
        var sent0 = channel.request("acme.assets.v1.DepreciationValidationService/CheckDepreciationEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "depreciation", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "depreciation", "location", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "depreciation", "organizer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        var sent1 = channel.request("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/CheckDepreciationEligibility"), "depreciation", "location", "recipient"),
                ProbeSupport.value(sent1, "asset_transfer", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/CheckDepreciationEligibility"), "depreciation", "location", "country_code"),
                ProbeSupport.value(sent1, "details", "destination", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/CheckDepreciationEligibility"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferCalculationService/QuoteAssetTransfer"), "asset_transfer", "metadata"),
                ProbeSupport.value(response, "depreciation", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "depreciation", "organizer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "depreciation", "location", "region"));
        var endpoint = new DepreciationReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamDepreciationMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamDepreciationMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamDepreciationMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void shareDepreciationReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new DepreciationReportingApplicationEvents(DepreciationValidationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof DepreciationReportingApplication.ShareDepreciationReportStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new DepreciationReportingApplication(
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationQueryServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newBlockingStub(channel),
                DepreciationValidationServiceGrpc.newFutureStub(channel),
                FixedAssetServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                UserServiceGrpc.newBlockingStub(channel),
                CoverageReportingServiceGrpc.newBlockingStub(channel),
                FiscalPeriodNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                new DepreciationReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ShareDepreciationReportRequest.getDefaultInstance());
        var response = application.shareDepreciationReport(request);
        var sent0 = channel.request("acme.identity.v1.UserService/CloneUser");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent0, "user", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(sent0, "user", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent0, "user", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(sent0, "user", "contact", "email"));
        var sent1 = channel.request("acme.assets.v1.DepreciationValidationService/ScoreDepreciation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CloneUser"), "user", "relationship_manager", "email"),
                ProbeSupport.value(sent1, "depreciation", "participants", "contact", "email"));
        var sent2 = channel.request("acme.ledger.v1.FiscalPeriodNotificationService/StreamFiscalPeriodNotices");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CloneUser"), "user", "contact", "phone"),
                ProbeSupport.value(sent2, "subscriber", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent2, "subscriber", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "organizer", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent2, "subscriber", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CloneUser"), "user", "name"),
                ProbeSupport.value(response, "depreciation", "participants", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.identity.v1.UserService/CloneUser"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "audit", "created_at"),
                ProbeSupport.value(response, "depreciation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DepreciationValidationService/ScoreDepreciation"), "depreciation", "organizer", "user_id"),
                ProbeSupport.value(response, "depreciation", "organizer", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "depreciation_id"),
                ProbeSupport.value(response, "depreciation", "depreciation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(response, "depreciation", "organizer", "email"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new DepreciationReportingApplicationEvents(DepreciationValidationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<ScoreDepreciationResponse>();
        listener.on(new DepreciationReportingApplication.ShareDepreciationReportStage1Requested(ProbeSupport.sample(ScoreDepreciationRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
