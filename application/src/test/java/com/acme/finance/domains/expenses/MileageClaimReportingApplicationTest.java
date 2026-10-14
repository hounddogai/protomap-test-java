package com.acme.finance.domains.expenses;

import com.acme.audit.v1.ListRetentionPolicyVersionsRequest;
import com.acme.audit.v1.ListRetentionPolicyVersionsResponse;
import com.acme.audit.v1.RetentionPolicyHistoryServiceGrpc;
import com.acme.certificates.v1.CertificateQueryServiceGrpc;
import com.acme.certificates.v1.SuggestCertificatesRequest;
import com.acme.certificates.v1.SuggestCertificatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.PreviewHouseholdRequest;
import com.acme.customers.v1.PreviewHouseholdResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.expenses.v1.CalculateExpenseReportRequest;
import com.acme.expenses.v1.CalculateExpenseReportResponse;
import com.acme.expenses.v1.CountMileageClaimTotalsRequest;
import com.acme.expenses.v1.CountMileageClaimTotalsResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyBlockAction;
import com.acme.expenses.v1.ExpensePolicyCalculationServiceGrpc;
import com.acme.expenses.v1.ExpensePolicySyncServiceGrpc;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportCalculationServiceGrpc;
import com.acme.expenses.v1.ExplainMileageClaimCalculationRequest;
import com.acme.expenses.v1.ExplainMileageClaimCalculationResponse;
import com.acme.expenses.v1.FetchMileageClaimReportRequest;
import com.acme.expenses.v1.FetchMileageClaimReportResponse;
import com.acme.expenses.v1.GenerateMileageClaimReportRequest;
import com.acme.expenses.v1.GenerateMileageClaimReportResponse;
import com.acme.expenses.v1.ListMileageClaimReportsRequest;
import com.acme.expenses.v1.ListMileageClaimReportsResponse;
import com.acme.expenses.v1.MileageClaim;
import com.acme.expenses.v1.MileageClaimCalculationServiceGrpc;
import com.acme.expenses.v1.PreviewReceiptRequest;
import com.acme.expenses.v1.PreviewReceiptResponse;
import com.acme.expenses.v1.Receipt;
import com.acme.expenses.v1.ReceiptRef;
import com.acme.expenses.v1.ReceiptValidationServiceGrpc;
import com.acme.expenses.v1.ResolveExpensePolicyConflictRequest;
import com.acme.expenses.v1.ResolveExpensePolicyConflictResponse;
import com.acme.expenses.v1.ShareMileageClaimReportRequest;
import com.acme.expenses.v1.ShareMileageClaimReportResponse;
import com.acme.expenses.v1.SimulateExpensePolicyRequest;
import com.acme.expenses.v1.SimulateExpensePolicyResponse;
import com.acme.expenses.v1.StreamMileageClaimMetricsRequest;
import com.acme.expenses.v1.StreamMileageClaimMetricsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class MileageClaimReportingApplicationTest {
    @Test
    void streamMileageClaimMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimReportingApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                CertificateQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newFutureStub(channel),
                new MileageClaimReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(StreamMileageClaimMetricsRequest.getDefaultInstance());
        var response = application.streamMileageClaimMetrics(request);
        var sent0 = channel.request("acme.audit.v1.RetentionPolicyHistoryService/ListRetentionPolicyVersions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "query"),
                ProbeSupport.value(sent0, "filter", "query"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent0, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        var sent1 = channel.request("acme.expenses.v1.ExpensePolicySyncService/ResolveExpensePolicyConflict");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent1, "expense_policy", "block", "notify_email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/ListRetentionPolicyVersions"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyHistoryService/ListRetentionPolicyVersions"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "mileage_claim", "billing_address", "region"));
        var endpoint = new MileageClaimReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamMileageClaimMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamMileageClaimMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamMileageClaimMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void shareMileageClaimReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ExpensesRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveExpensePolicy(String tenant, ExpensePolicy value) {}
            public void saveExpenseReport(String tenant, ExpenseReport value) {}
            public void saveMileageClaim(String tenant, MileageClaim value) {}
            public void saveReceipt(String tenant, Receipt value) {}
        };
        var application = new MileageClaimReportingApplication(
                RetentionPolicyHistoryServiceGrpc.newBlockingStub(channel),
                CertificateQueryServiceGrpc.newBlockingStub(channel),
                HouseholdCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyCalculationServiceGrpc.newBlockingStub(channel),
                ExpensePolicySyncServiceGrpc.newBlockingStub(channel),
                ExpenseReportCalculationServiceGrpc.newBlockingStub(channel),
                MileageClaimCalculationServiceGrpc.newBlockingStub(channel),
                ReceiptValidationServiceGrpc.newFutureStub(channel),
                new MileageClaimReportingRequestMapperImpl(),
                repository);
        var request = ProbeSupport.sample(ShareMileageClaimReportRequest.getDefaultInstance());
        var response = application.shareMileageClaimReport(request);
        var sent0 = channel.request("acme.expenses.v1.ReceiptValidationService/PreviewReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "phone"),
                ProbeSupport.value(sent0, "details", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "recipient", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        var sent1 = channel.request("acme.customers.v1.HouseholdValidationService/PreviewHousehold");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/PreviewReceipt"), "receipt", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "household", "children", "children", "location", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/PreviewReceipt"), "receipt", "audit", "updated_at"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/PreviewReceipt"), "receipt", "audit", "updated_by"),
                ProbeSupport.value(sent1, "household", "children", "children", "audit", "updated_by"));
        var sent2 = channel.request("acme.expenses.v1.ExpensePolicyCalculationService/SimulateExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/PreviewHousehold"), "household", "manager", "user_id"),
                ProbeSupport.value(sent2, "expense_policy", "owner", "user_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/PreviewHousehold"), "household", "location", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/PreviewHousehold"), "household", "audit", "created_at"),
                ProbeSupport.value(sent2, "expense_policy", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/PreviewReceipt"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ReceiptValidationService/PreviewReceipt"), "receipt", "counterparty", "mileage_claim_id"),
                ProbeSupport.value(response, "mileage_claim", "mileage_claim_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdValidationService/PreviewHousehold"), "household", "location", "locality"),
                ProbeSupport.value(response, "mileage_claim", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/SimulateExpensePolicy"), "expense_policy", "audit", "created_at"),
                ProbeSupport.value(response, "mileage_claim", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpensePolicyCalculationService/SimulateExpensePolicy"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
    }
}
