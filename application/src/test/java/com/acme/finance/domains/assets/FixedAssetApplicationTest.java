package com.acme.finance.domains.assets;

import com.acme.assets.v1.AssetTransfer;
import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.BatchGetFixedAssetsRequest;
import com.acme.assets.v1.BatchGetFixedAssetsResponse;
import com.acme.assets.v1.CreateFixedAssetRequest;
import com.acme.assets.v1.CreateFixedAssetResponse;
import com.acme.assets.v1.DeleteFixedAssetRequest;
import com.acme.assets.v1.DeleteFixedAssetResponse;
import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalValidationServiceGrpc;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.assets.v1.FixedAssetDimensions;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.assets.v1.GetFixedAssetRequest;
import com.acme.assets.v1.GetFixedAssetResponse;
import com.acme.assets.v1.LintDisposalsRequest;
import com.acme.assets.v1.LintDisposalsResponse;
import com.acme.assets.v1.LintFixedAssetsRequest;
import com.acme.assets.v1.LintFixedAssetsResponse;
import com.acme.assets.v1.ListFixedAssetsRequest;
import com.acme.assets.v1.ListFixedAssetsResponse;
import com.acme.assets.v1.LookupFixedAssetRequest;
import com.acme.assets.v1.LookupFixedAssetResponse;
import com.acme.assets.v1.SearchFixedAssetsRequest;
import com.acme.assets.v1.SearchFixedAssetsResponse;
import com.acme.assets.v1.SuggestDepreciationsRequest;
import com.acme.assets.v1.SuggestDepreciationsResponse;
import com.acme.assets.v1.ValidateDisposalRequest;
import com.acme.assets.v1.ValidateDisposalResponse;
import com.acme.assets.v1.WatchFixedAssetsRequest;
import com.acme.assets.v1.WatchFixedAssetsResponse;
import com.acme.audit.v1.AuditEventExportServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamAuditEventExportRequest;
import com.acme.audit.v1.StreamAuditEventExportResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicy;
import com.acme.authorization.v1.AccessPolicyValidationServiceGrpc;
import com.acme.authorization.v1.ScoreAccessPolicyRequest;
import com.acme.authorization.v1.ScoreAccessPolicyResponse;
import com.acme.banking.v1.Mandate;
import com.acme.banking.v1.MandateValidationServiceGrpc;
import com.acme.banking.v1.PreviewMandateRequest;
import com.acme.banking.v1.PreviewMandateResponse;
import com.acme.banking.v1.ValidateMandateRequest;
import com.acme.banking.v1.ValidateMandateResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationCalculationServiceGrpc;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.RecalculateAttestationsRequest;
import com.acme.compliance.v1.RecalculateAttestationsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageBlockAction;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatchRef;
import com.acme.ledger.v1.ValidateLedgerAccountRequest;
import com.acme.ledger.v1.ValidateLedgerAccountResponse;
import com.acme.maintenance.v1.ListWorkOrdersRequest;
import com.acme.maintenance.v1.ListWorkOrdersResponse;
import com.acme.maintenance.v1.WorkOrderServiceGrpc;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class FixedAssetApplicationTest {
    @Test
    void createFixedAssetPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                WorkOrderServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(CreateFixedAssetRequest.getDefaultInstance());
        var response = application.createFixedAsset(request);
        var sent0 = channel.request("acme.assets.v1.DisposalValidationService/LintDisposals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner", "display_name"),
                ProbeSupport.value(sent0, "disposal", "assignee", "display_name"));
        var sent1 = channel.request("acme.audit.v1.AuditEventExportService/StreamAuditEventExport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        var sent2 = channel.request("acme.banking.v1.MandateValidationService/ValidateMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/LintDisposals"), "disposal", "requester", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/LintDisposals"), "disposal", "requester", "preferred_channel"),
                ProbeSupport.value(sent2, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "region"),
                ProbeSupport.value(sent2, "mandate", "billing_address", "region"));
        var sent3 = channel.request("acme.compliance.v1.AttestationCalculationService/RecalculateAttestations");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "address", "postal_code"),
                ProbeSupport.value(sent3, "attestation", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/LintDisposals"), "disposal", "customer", "code"),
                ProbeSupport.value(response, "fixed_asset", "code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/ValidateMandate"), "mandate", "audit", "etag"),
                ProbeSupport.value(response, "fixed_asset", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationCalculationService/RecalculateAttestations"), "attestation", "audit", "updated_by"),
                ProbeSupport.value(response, "fixed_asset", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "owner"),
                ProbeSupport.value(response, "fixed_asset", "owner"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "fixed_asset", "specs"),
                ProbeSupport.value(response, "fixed_asset", "specs"));
    }

    @Test
    void watchFixedAssetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                WorkOrderServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(WatchFixedAssetsRequest.getDefaultInstance());
        var response = application.watchFixedAssets(request);
        var sent0 = channel.request("acme.assets.v1.DisposalValidationService/ValidateDisposal");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "disposal", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "disposal", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.DisposalValidationService/ValidateDisposal"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "fixed_asset", "owner", "email"));
        var endpoint = new FixedAssetEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<WatchFixedAssetsResponse> observer = new StreamObserver<>() {
            public void onNext(WatchFixedAssetsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.watchFixedAssets(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void batchCreateFixedAssetsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AssetsRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAssetTransfer(String tenant, AssetTransfer value) {}
            public void saveDepreciation(String tenant, Depreciation value) {}
            public void saveDisposal(String tenant, Disposal value) {}
            public void saveFixedAsset(String tenant, FixedAsset value) {}
        };
        var application = new FixedAssetApplication(
                DisposalValidationServiceGrpc.newBlockingStub(channel),
                FixedAssetValidationServiceGrpc.newBlockingStub(channel),
                AuditEventExportServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                MandateValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationCalculationServiceGrpc.newBlockingStub(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                WorkOrderServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(BatchCreateFixedAssetsRequest.getDefaultInstance());
        var response = application.batchCreateFixedAssets(request);
        var sent0 = channel.request("acme.banking.v1.MandateValidationService/PreviewMandate");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "audit", "created_by"),
                ProbeSupport.value(sent0, "mandate", "audit", "created_by"));
        var sent1 = channel.request("acme.assets.v1.FixedAssetValidationService/LintFixedAssets");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/PreviewMandate"), "mandate", "counterparty", "email"),
                ProbeSupport.value(sent1, "fixed_asset", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/PreviewMandate"), "mandate", "billing_address", "locality"),
                ProbeSupport.value(sent1, "fixed_asset", "address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "record", "components", "description"),
                ProbeSupport.value(sent1, "fixed_asset", "components", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.banking.v1.MandateValidationService/PreviewMandate"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.assets.v1.FixedAssetValidationService/LintFixedAssets"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        var endpoint = new FixedAssetEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<BatchCreateFixedAssetsResponse> observer = new StreamObserver<>() {
            public void onNext(BatchCreateFixedAssetsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        var input = endpoint.batchCreateFixedAssets(observer);
        input.onNext(request);
        input.onNext(request);
        input.onCompleted();
        input.onCompleted();
        input.onNext(request);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }
}
