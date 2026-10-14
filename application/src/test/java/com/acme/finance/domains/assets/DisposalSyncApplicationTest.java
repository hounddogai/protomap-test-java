package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetCalculationServiceGrpc;
import com.acme.assets.v1.GetDisposalSyncStateRequest;
import com.acme.assets.v1.GetDisposalSyncStateResponse;
import com.acme.assets.v1.LintAssetTransfersRequest;
import com.acme.assets.v1.LintAssetTransfersResponse;
import com.acme.assets.v1.ListDisposalConflictsRequest;
import com.acme.assets.v1.ListDisposalConflictsResponse;
import com.acme.assets.v1.PushDisposalRequest;
import com.acme.assets.v1.PushDisposalResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.StreamDepreciationMetricsRequest;
import com.acme.assets.v1.StreamDepreciationMetricsResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.assets.v1.SyncDisposalsRequest;
import com.acme.assets.v1.SyncDisposalsResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.ScheduleAuditEventExportRequest;
import com.acme.audit.v1.ScheduleAuditEventExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.dispatch.v1.DispatchOrderServiceGrpc;
import com.acme.dispatch.v1.SearchDispatchOrdersRequest;
import com.acme.dispatch.v1.SearchDispatchOrdersResponse;
import com.acme.documents.v1.DocumentQueryServiceGrpc;
import com.acme.documents.v1.FindDocumentsRequest;
import com.acme.documents.v1.FindDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimApprovalServiceGrpc;
import com.acme.insurance.v1.SubmitClaimRequest;
import com.acme.insurance.v1.SubmitClaimResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodExportServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.FiscalPeriodParticipant;
import com.acme.ledger.v1.RevertFiscalPeriodRequest;
import com.acme.ledger.v1.RevertFiscalPeriodResponse;
import com.acme.ledger.v1.StreamFiscalPeriodExportRequest;
import com.acme.ledger.v1.StreamFiscalPeriodExportResponse;
import com.acme.lending.v1.ApproveLoanRequest;
import com.acme.lending.v1.ApproveLoanResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanApprovalServiceGrpc;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityRequest;
import com.acme.metrics.v1.CheckMetricSeriesEligibilityResponse;
import com.acme.metrics.v1.MetricSeries;
import com.acme.metrics.v1.MetricSeriesValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
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
import org.springframework.stereotype.Service;

class DisposalSyncApplicationTest {
    @Test
    void streamDisposalChangesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new DisposalSyncApplication(
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                DispatchOrderServiceGrpc.newBlockingStub(channel),
                DocumentQueryServiceGrpc.newBlockingStub(channel),
                ClaimApprovalServiceGrpc.newBlockingStub(channel),
                FiscalPeriodExportServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                LoanApprovalServiceGrpc.newBlockingStub(channel),
                MetricSeriesValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(StreamDisposalChangesRequest.getDefaultInstance());
        var response = application.streamDisposalChanges(request);
        var sent0 = channel.request("acme.dispatch.v1.DispatchOrderService/SearchDispatchOrders");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "assignee", "user_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "custom_fields"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "created_by"),
                ProbeSupport.value(sent1, "fixed_asset", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "customer", "display_name"),
                ProbeSupport.value(sent1, "fixed_asset", "owner", "display_name"));
        var sent2 = channel.request("acme.assets.v1.DepreciationReportingService/StreamDepreciationMetrics");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "author_email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "comments", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.insurance.v1.ClaimApprovalService/SubmitClaim");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "audit", "updated_by"),
                ProbeSupport.value(sent3, "claim", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.dispatch.v1.DispatchOrderService/SearchDispatchOrders"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "fixed_asset", "owner", "display_name"),
                ProbeSupport.value(response, "disposal", "assignee", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetCalculationService/EstimateFixedAsset"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.insurance.v1.ClaimApprovalService/SubmitClaim"), "claim", "requester", "phone"),
                ProbeSupport.value(response, "disposal", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "priority"),
                ProbeSupport.value(response, "disposal", "priority"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "change", "disposal_id"),
                ProbeSupport.value(response, "disposal", "disposal_id"));
        var endpoint = new DisposalSyncEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamDisposalChangesResponse> observer = new StreamObserver<>() {
            public void onNext(StreamDisposalChangesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.streamDisposalChanges(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "value", "completed"), signals);
    }
}
