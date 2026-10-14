package com.acme.finance.domains.assets;

import com.acme.admissions.v1.AdmissionServiceGrpc;
import com.acme.admissions.v1.WatchAdmissionsRequest;
import com.acme.admissions.v1.WatchAdmissionsResponse;
import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.AssetTransferCalculationServiceGrpc;
import com.acme.assets.v1.AssetTransferSyncServiceGrpc;
import com.acme.assets.v1.AssetTransferValidationServiceGrpc;
import com.acme.assets.v1.CalculateDepreciationRequest;
import com.acme.assets.v1.CalculateDepreciationResponse;
import com.acme.assets.v1.CountAssetTransferTotalsRequest;
import com.acme.assets.v1.CountAssetTransferTotalsResponse;
import com.acme.assets.v1.DeleteAssetTransferReportRequest;
import com.acme.assets.v1.DeleteAssetTransferReportResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationCalculationServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.EstimateAssetTransferRequest;
import com.acme.assets.v1.EstimateAssetTransferResponse;
import com.acme.assets.v1.ExplainDepreciationCalculationRequest;
import com.acme.assets.v1.ExplainDepreciationCalculationResponse;
import com.acme.assets.v1.FetchAssetTransferReportRequest;
import com.acme.assets.v1.FetchAssetTransferReportResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetLifecycleServiceGrpc;
import com.acme.assets.v1.GenerateAssetTransferReportRequest;
import com.acme.assets.v1.GenerateAssetTransferReportResponse;
import com.acme.assets.v1.GetAssetTransferRulesRequest;
import com.acme.assets.v1.GetAssetTransferRulesResponse;
import com.acme.assets.v1.GetAssetTransferSyncStateRequest;
import com.acme.assets.v1.GetAssetTransferSyncStateResponse;
import com.acme.assets.v1.ListAssetTransferReportsRequest;
import com.acme.assets.v1.ListAssetTransferReportsResponse;
import com.acme.assets.v1.ReconcileAssetTransfersRequest;
import com.acme.assets.v1.ReconcileAssetTransfersResponse;
import com.acme.assets.v1.ReconcileDisposalsRequest;
import com.acme.assets.v1.ReconcileDisposalsResponse;
import com.acme.assets.v1.ScheduleAssetTransferReportRequest;
import com.acme.assets.v1.ScheduleAssetTransferReportResponse;
import com.acme.assets.v1.ScheduleFixedAssetExpiryRequest;
import com.acme.assets.v1.ScheduleFixedAssetExpiryResponse;
import com.acme.assets.v1.ShareAssetTransferReportRequest;
import com.acme.assets.v1.ShareAssetTransferReportResponse;
import com.acme.assets.v1.StreamAssetTransferMetricsRequest;
import com.acme.assets.v1.StreamAssetTransferMetricsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailAdminServiceGrpc;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.RepairAuditTrailRequest;
import com.acme.audit.v1.RepairAuditTrailResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.experiments.v1.EstimateExperimentRequest;
import com.acme.experiments.v1.EstimateExperimentResponse;
import com.acme.experiments.v1.Experiment;
import com.acme.experiments.v1.ExperimentCalculationServiceGrpc;
import com.acme.experiments.v1.ExperimentParticipant;
import com.acme.finance.platform.BeanCopies;
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
import com.acme.insurance.v1.Premium;
import com.acme.insurance.v1.PremiumSyncServiceGrpc;
import com.acme.insurance.v1.ResolvePremiumConflictRequest;
import com.acme.insurance.v1.ResolvePremiumConflictResponse;
import com.acme.ledger.v1.FiscalPeriod;
import com.acme.ledger.v1.FiscalPeriodHistoryServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.RecordFiscalPeriodVersionRequest;
import com.acme.ledger.v1.RecordFiscalPeriodVersionResponse;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.lending.v1.CollateralWorkflowServiceGrpc;
import com.acme.lending.v1.ListCollateralWorkflowsRequest;
import com.acme.lending.v1.ListCollateralWorkflowsResponse;
import com.acme.modelregistry.v1.RegisteredModel;
import com.acme.modelregistry.v1.RegisteredModelCalculationServiceGrpc;
import com.acme.modelregistry.v1.SimulateRegisteredModelRequest;
import com.acme.modelregistry.v1.SimulateRegisteredModelResponse;
import com.acme.scheduling.v1.ShiftQueryServiceGrpc;
import com.acme.scheduling.v1.SuggestShiftsRequest;
import com.acme.scheduling.v1.SuggestShiftsResponse;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AssetTransferReportingApplicationTest {
    @Test
    void streamAssetTransferMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new AssetTransferReportingApplication(
                AdmissionServiceGrpc.newBlockingStub(channel),
                AssetTransferCalculationServiceGrpc.newBlockingStub(channel),
                AssetTransferSyncServiceGrpc.newBlockingStub(channel),
                AssetTransferSyncServiceGrpc.newFutureStub(channel),
                AssetTransferValidationServiceGrpc.newBlockingStub(channel),
                DepreciationCalculationServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newBlockingStub(channel),
                FixedAssetLifecycleServiceGrpc.newBlockingStub(channel),
                AuditTrailAdminServiceGrpc.newBlockingStub(channel),
                ExperimentCalculationServiceGrpc.newBlockingStub(channel),
                UserServiceGrpc.newBlockingStub(channel),
                PremiumSyncServiceGrpc.newBlockingStub(channel),
                FiscalPeriodHistoryServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                CollateralWorkflowServiceGrpc.newBlockingStub(channel),
                RegisteredModelCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                ShiftQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StreamAssetTransferMetricsRequest.getDefaultInstance());
        var response = application.streamAssetTransferMetrics(request);
        var sent0 = channel.request("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "fiscal_period", "organizer", "email"));
        var sent1 = channel.request("acme.assets.v1.AssetTransferSyncService/ReconcileAssetTransfers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion"), "fiscal_period", "audit", "etag"),
                ProbeSupport.value(sent1, "asset_transfer", "audit", "etag"));
        var sent2 = channel.request("acme.admissions.v1.AdmissionService/WatchAdmissions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion"), "fiscal_period", "organizer", "email"),
                ProbeSupport.value(sent2, "subscriber", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion"), "fiscal_period", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent2, "subscriber", "phone"));
        var sent3 = channel.request("acme.experiments.v1.ExperimentCalculationService/EstimateExperiment");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/ReconcileAssetTransfers"), "asset_transfer", "billing_address", "locality"),
                ProbeSupport.value(sent3, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent3, "experiment", "participants", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion"), "fiscal_period", "organizer", "email"),
                ProbeSupport.value(sent3, "experiment", "organizer", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.FiscalPeriodHistoryService/RecordFiscalPeriodVersion"), "fiscal_period", "location", "locality"),
                ProbeSupport.value(response, "asset_transfer", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/ReconcileAssetTransfers"), "asset_transfer", "billing_address", "lines"),
                ProbeSupport.value(response, "asset_transfer", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.AssetTransferSyncService/ReconcileAssetTransfers"), "asset_transfer", "issued_at"),
                ProbeSupport.value(response, "asset_transfer", "issued_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.experiments.v1.ExperimentCalculationService/EstimateExperiment"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "asset_transfer", "billing_address", "region"));
        var endpoint = new AssetTransferReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamAssetTransferMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamAssetTransferMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamAssetTransferMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
