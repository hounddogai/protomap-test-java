package com.acme.finance.domains.accounting;

import com.acme.accounting.v1.Accrual;
import com.acme.accounting.v1.AccrualValidationServiceGrpc;
import com.acme.accounting.v1.Allocation;
import com.acme.accounting.v1.AllocationValidationServiceGrpc;
import com.acme.accounting.v1.BrowseChartOfAccountsesRequest;
import com.acme.accounting.v1.BrowseChartOfAccountsesResponse;
import com.acme.accounting.v1.CalculateReconciliationRequest;
import com.acme.accounting.v1.CalculateReconciliationResponse;
import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.CountChartOfAccountsesRequest;
import com.acme.accounting.v1.CountChartOfAccountsesResponse;
import com.acme.accounting.v1.ExplainReconciliationDecisionRequest;
import com.acme.accounting.v1.ExplainReconciliationDecisionResponse;
import com.acme.accounting.v1.ExportChartOfAccountsesRequest;
import com.acme.accounting.v1.ExportChartOfAccountsesResponse;
import com.acme.accounting.v1.FindChartOfAccountsesRequest;
import com.acme.accounting.v1.FindChartOfAccountsesResponse;
import com.acme.accounting.v1.LintAccrualsRequest;
import com.acme.accounting.v1.LintAccrualsResponse;
import com.acme.accounting.v1.LookupChartOfAccountsRequest;
import com.acme.accounting.v1.LookupChartOfAccountsResponse;
import com.acme.accounting.v1.NormalizeAllocationRequest;
import com.acme.accounting.v1.NormalizeAllocationResponse;
import com.acme.accounting.v1.PeekChartOfAccountsRequest;
import com.acme.accounting.v1.PeekChartOfAccountsResponse;
import com.acme.accounting.v1.PreviewAllocationRequest;
import com.acme.accounting.v1.PreviewAllocationResponse;
import com.acme.accounting.v1.Reconciliation;
import com.acme.accounting.v1.ReconciliationCalculationServiceGrpc;
import com.acme.accounting.v1.ReconciliationValidationServiceGrpc;
import com.acme.accounting.v1.ResolveChartOfAccountsesRequest;
import com.acme.accounting.v1.ResolveChartOfAccountsesResponse;
import com.acme.accounting.v1.SummarizeChartOfAccountsesRequest;
import com.acme.accounting.v1.SummarizeChartOfAccountsesResponse;
import com.acme.billing.v1.BillingAccountQueryServiceGrpc;
import com.acme.billing.v1.SuggestBillingAccountsRequest;
import com.acme.billing.v1.SuggestBillingAccountsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.telematics.v1.SearchTelemetryReadingsRequest;
import com.acme.telematics.v1.SearchTelemetryReadingsResponse;
import com.acme.telematics.v1.TelemetryReadingServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class ChartOfAccountsQueryApplicationTest {
    @Test
    void exportChartOfAccountsesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsQueryApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                BillingAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                repository,
                TelemetryReadingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportChartOfAccountsesRequest.getDefaultInstance());
        var response = application.exportChartOfAccountses(request);
        var sent0 = channel.request("acme.accounting.v1.ReconciliationValidationService/ExplainReconciliationDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "user_id"),
                ProbeSupport.value(sent0, "reconciliation", "assignee", "user_id"));
        var sent1 = channel.request("acme.telematics.v1.TelemetryReadingService/SearchTelemetryReadings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ExplainReconciliationDecision"), "reconciliation", "assignee", "user_id"),
                ProbeSupport.value(response, "chart_of_accounts", "children", "children", "manager", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.ReconciliationValidationService/ExplainReconciliationDecision"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.telematics.v1.TelemetryReadingService/SearchTelemetryReadings"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "chart_of_accounts", "location", "region"));
        var endpoint = new ChartOfAccountsQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportChartOfAccountsesResponse> observer = new StreamObserver<>() {
            public void onNext(ExportChartOfAccountsesResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportChartOfAccountses(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizeChartOfAccountsesPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new AccountingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveAccrual(String tenant, Accrual value) {}
            public void saveAllocation(String tenant, Allocation value) {}
            public void saveChartOfAccounts(String tenant, ChartOfAccounts value) {}
            public void saveReconciliation(String tenant, Reconciliation value) {}
        };
        var application = new ChartOfAccountsQueryApplication(
                AccrualValidationServiceGrpc.newBlockingStub(channel),
                AllocationValidationServiceGrpc.newBlockingStub(channel),
                ReconciliationValidationServiceGrpc.newBlockingStub(channel),
                BillingAccountQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                repository,
                TelemetryReadingServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizeChartOfAccountsesRequest.getDefaultInstance());
        var response = application.summarizeChartOfAccountses(request);
        var sent0 = channel.request("acme.accounting.v1.AccrualValidationService/LintAccruals");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent0, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        var sent1 = channel.request("acme.accounting.v1.AllocationValidationService/NormalizeAllocation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/LintAccruals"), "accrual", "audit", "updated_at"),
                ProbeSupport.value(sent1, "allocation", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/LintAccruals"), "accrual", "audit", "etag"),
                ProbeSupport.value(sent1, "allocation", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/LintAccruals"), "accrual", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "allocation", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.accounting.v1.AccrualValidationService/LintAccruals"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
