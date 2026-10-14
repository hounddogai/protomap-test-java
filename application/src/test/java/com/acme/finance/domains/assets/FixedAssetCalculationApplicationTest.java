package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.CalculateFixedAssetRequest;
import com.acme.assets.v1.CalculateFixedAssetResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.EstimateFixedAssetRequest;
import com.acme.assets.v1.EstimateFixedAssetResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.GetFixedAssetRatesRequest;
import com.acme.assets.v1.GetFixedAssetRatesResponse;
import com.acme.assets.v1.QuoteFixedAssetRequest;
import com.acme.assets.v1.QuoteFixedAssetResponse;
import com.acme.assets.v1.RecalculateFixedAssetsRequest;
import com.acme.assets.v1.RecalculateFixedAssetsResponse;
import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.audit.v1.RevertRetentionPolicyRequest;
import com.acme.audit.v1.RevertRetentionPolicyResponse;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.banking.v1.EstimateMandateRequest;
import com.acme.banking.v1.EstimateMandateResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateCalculationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.ControlQueryServiceGrpc;
import com.acme.compliance.v1.FindControlsRequest;
import com.acme.compliance.v1.FindControlsResponse;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionValidationServiceGrpc;
import com.acme.compliance.v1.ValidateSanctionRequest;
import com.acme.compliance.v1.ValidateSanctionResponse;
import com.acme.drivers.v1.Driver;
import com.acme.drivers.v1.DriverCalculationServiceGrpc;
import com.acme.drivers.v1.EstimateDriverRequest;
import com.acme.drivers.v1.EstimateDriverResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.labeling.v1.CloneLabelingTaskRequest;
import com.acme.labeling.v1.CloneLabelingTaskResponse;
import com.acme.labeling.v1.LabelSetRef;
import com.acme.labeling.v1.LabelingTask;
import com.acme.labeling.v1.LabelingTaskServiceGrpc;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.NormalizeLoanRequest;
import com.acme.lending.v1.NormalizeLoanResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.lending.v1.ScoreRepaymentRequest;
import com.acme.lending.v1.ScoreRepaymentResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.metrics.v1.SummarizeMetricSeriesesRequest;
import com.acme.metrics.v1.SummarizeMetricSeriesesResponse;
import com.acme.venues.v1.PreviewVenueRequest;
import com.acme.venues.v1.PreviewVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueDimensions;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class FixedAssetCalculationApplicationTest {
    @Test
    void explainFixedAssetCalculationPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var listener = new FixedAssetCalculationApplicationEvents(ControlQueryServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof FixedAssetCalculationApplication.EstimateFixedAssetStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new FixedAssetCalculationApplication(
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                MandateCalculationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                SanctionValidationServiceGrpc.newBlockingStub(channel),
                DriverCalculationServiceGrpc.newBlockingStub(channel),
                events,
                CoverageValidationServiceGrpc.newBlockingStub(channel),
                LabelingTaskServiceGrpc.newBlockingStub(channel),
                LoanValidationServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                repository,
                VenueValidationServiceGrpc.newFutureStub(channel));
        var request = ProbeSupport.sample(ExplainFixedAssetCalculationRequest.getDefaultInstance());
        var response = application.explainFixedAssetCalculation(request);
        var sent0 = channel.request("acme.banking.v1.MandateCalculationService/EstimateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "recipient"),
                ProbeSupport.value(sent0, "mandate", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "audit", "created_at"),
                ProbeSupport.value(sent0, "mandate", "audit", "created_at"));
        var sent1 = channel.request("acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "mandate", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "audit_event", "subject", "display_name"));
        var sent2 = channel.request("acme.audit.v1.RetentionPolicyHistoryService/RevertRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner", "email"),
                ProbeSupport.value(sent2, "retention_policy", "owner", "email"));
        var sent3 = channel.request("acme.audit.v1.AuditEventExportService/StreamAuditEventExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateCalculationService/EstimateMandate"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditEventValidationService/NormalizeAuditEvent"), "outcome", "next_steps"),
                ProbeSupport.value(response, "outcome", "next_steps"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset_id"),
                ProbeSupport.value(response, "fixed_asset", "fixed_asset_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "name"),
                ProbeSupport.value(response, "fixed_asset", "name"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new FixedAssetCalculationApplicationEvents(ControlQueryServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<FindControlsResponse>();
        listener.on(new FixedAssetCalculationApplication.EstimateFixedAssetStage0Requested(ProbeSupport.sample(FindControlsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
