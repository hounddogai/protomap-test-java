package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualLine;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationBlockAction;
import com.acme.accounting.v1.AllocationDiscountAction;
import com.acme.accounting.v1.AllocationRef;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseAccrualsRequest;
import com.acme.accounting.v1.BrowseAccrualsResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.CountAccrualsRequest;
import com.acme.accounting.v1.CountAccrualsResponse;
import com.acme.accounting.v1.EstimateReconciliationRequest;
import com.acme.accounting.v1.EstimateReconciliationResponse;
import com.acme.accounting.v1.ExportAccrualsRequest;
import com.acme.accounting.v1.ExportAccrualsResponse;
import com.acme.accounting.v1.FindAccrualsRequest;
import com.acme.accounting.v1.FindAccrualsResponse;
import com.acme.accounting.v1.LookupAccrualRequest;
import com.acme.accounting.v1.LookupAccrualResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PeekAccrualRequest;
import com.acme.accounting.v1.PeekAccrualResponse;
import com.acme.accounting.v1.PreviewAccrualRequest;
import com.acme.accounting.v1.PreviewAccrualResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResolveAccrualsRequest;
import com.acme.accounting.v1.ResolveAccrualsResponse;
import com.acme.accounting.v1.ScoreAllocationRequest;
import com.acme.accounting.v1.ScoreAllocationResponse;
import com.acme.accounting.v1.ScoreReconciliationRequest;
import com.acme.accounting.v1.ScoreReconciliationResponse;
import com.acme.accounting.v1.SuggestAccrualsRequest;
import com.acme.accounting.v1.SuggestAccrualsResponse;
import com.acme.accounting.v1.SummarizeAccrualsRequest;
import com.acme.accounting.v1.SummarizeAccrualsResponse;
import com.acme.accounting.v1.ValidateAccrualRequest;
import com.acme.accounting.v1.ValidateAccrualResponse;
import com.acme.assets.v1.ExplainFixedAssetCalculationRequest;
import com.acme.assets.v1.ExplainFixedAssetCalculationResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetComponent;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.ShareAuditTrailExportRequest;
import com.acme.audit.v1.ShareAuditTrailExportResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.CreateUserRequest;
import com.acme.identity.v1.CreateUserResponse;
import com.acme.identity.v1.User;
import com.acme.identity.v1.UserRef;
import com.acme.identity.v1.UserServiceGrpc;
import com.acme.payroll.v1.ExplainPayRunDecisionRequest;
import com.acme.payroll.v1.ExplainPayRunDecisionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunValidationServiceGrpc;
import com.acme.sponsorships.v1.CalculateSponsorRequest;
import com.acme.sponsorships.v1.CalculateSponsorResponse;
import com.acme.sponsorships.v1.Sponsor;
import com.acme.sponsorships.v1.SponsorCalculationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class AccrualQueryApplicationTest {
    @Test
    void exportAccrualsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AccrualQueryApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserServiceGrpc.newBlockingStub(channel),
                new AccrualQueryRequestMapperImpl(),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                repository,
                SponsorCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportAccrualsRequest.getDefaultInstance());
        var response = application.exportAccruals(request);
        var sent0 = channel.request("acme.accounting.v1.AllocationValidationService/PreviewAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "allocation", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.accounting.v1.AccrualValidationService/ValidateAccrual");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/PreviewAllocation"), "allocation", "audit", "created_by"),
                ProbeSupport.value(sent1, "accrual", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/PreviewAllocation"), "allocation", "discount", "amount"),
                ProbeSupport.value(sent1, "accrual", "lines", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/PreviewAllocation"), "allocation", "audit", "updated_at"),
                ProbeSupport.value(sent1, "accrual", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/PreviewAllocation"), "allocation", "description"),
                ProbeSupport.value(response, "accrual", "lines", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AllocationValidationService/PreviewAllocation"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/ValidateAccrual"), "accrual", "counterparty"),
                ProbeSupport.value(response, "accrual", "counterparty"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "accrual", "billing_address", "region"));
        var endpoint = new AccrualQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportAccrualsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportAccrualsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportAccruals(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeAccrualsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new AccrualQueryApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationCalculationServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                AuditTrailExportServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                UserServiceGrpc.newBlockingStub(channel),
                new AccrualQueryRequestMapperImpl(),
                PayRunValidationServiceGrpc.newBlockingStub(channel),
                repository,
                SponsorCalculationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeAccrualsRequest.getDefaultInstance());
        var response = application.summarizeAccruals(request);
        var sent0 = channel.request("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "reconciliation", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "reconciliation", "assignee", "user_id"));
        var sent1 = channel.request("acme.accounting.v1.AccrualValidationService/PreviewAccrual");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation"), "reconciliation", "requester", "phone"),
                ProbeSupport.value(sent1, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation"), "reconciliation", "audit", "updated_at"),
                ProbeSupport.value(sent1, "accrual", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ScoreReconciliation"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/PreviewAccrual"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
