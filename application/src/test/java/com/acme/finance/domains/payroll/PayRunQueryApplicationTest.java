package com.acme.finance.domains.payroll;

import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.authorization.v1.AccessPolicyServiceGrpc;
import com.acme.authorization.v1.WatchAccessPoliciesRequest;
import com.acme.authorization.v1.WatchAccessPoliciesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.BatchCreateComplianceCasesRequest;
import com.acme.compliance.v1.BatchCreateComplianceCasesResponse;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountHistoryServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountVersionsRequest;
import com.acme.ledger.v1.ListLedgerAccountVersionsResponse;
import com.acme.payroll.v1.BrowsePayRunsRequest;
import com.acme.payroll.v1.BrowsePayRunsResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.ExportPayRunsRequest;
import com.acme.payroll.v1.ExportPayRunsResponse;
import com.acme.payroll.v1.FindPayRunsRequest;
import com.acme.payroll.v1.FindPayRunsResponse;
import com.acme.payroll.v1.GetPayRunRatesRequest;
import com.acme.payroll.v1.GetPayRunRatesResponse;
import com.acme.payroll.v1.LintPayStubsRequest;
import com.acme.payroll.v1.LintPayStubsResponse;
import com.acme.payroll.v1.LookupPayRunRequest;
import com.acme.payroll.v1.LookupPayRunResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayRunRequest;
import com.acme.payroll.v1.PeekPayRunResponse;
import com.acme.payroll.v1.QuoteTaxWithholdingRequest;
import com.acme.payroll.v1.QuoteTaxWithholdingResponse;
import com.acme.payroll.v1.ResolvePayRunsRequest;
import com.acme.payroll.v1.ResolvePayRunsResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.SuggestPayRunsRequest;
import com.acme.payroll.v1.SuggestPayRunsResponse;
import com.acme.payroll.v1.SummarizePayRunsRequest;
import com.acme.payroll.v1.SummarizePayRunsResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingBlockAction;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.workplaces.v1.BrowseBuildingsRequest;
import com.acme.workplaces.v1.BrowseBuildingsResponse;
import com.acme.workplaces.v1.BuildingQueryServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PayRunQueryApplicationTest {
    @Test
    void exportPayRunsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayRunQueryApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                BuildingQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportPayRunsRequest.getDefaultInstance());
        var response = application.exportPayRuns(request);
        var sent0 = channel.request("acme.payroll.v1.PayStubValidationService/LintPayStubs");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "pay_stub", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/LintPayStubs"), "pay_stub", "billing_address", "country_code"),
                ProbeSupport.value(response, "pay_run", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/LintPayStubs"), "pay_stub", "metadata"),
                ProbeSupport.value(response, "pay_run", "metadata"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "pay_run", "billing_address", "region"));
        var endpoint = new PayRunQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportPayRunsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportPayRunsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportPayRuns(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void summarizePayRunsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayRunQueryApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                AccessPolicyServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ComplianceCaseServiceGrpc.newStub(channel),
                LedgerAccountHistoryServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                BuildingQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SummarizePayRunsRequest.getDefaultInstance());
        var response = application.summarizePayRuns(request);
        var sent0 = channel.request("acme.payroll.v1.PayRunCalculationService/GetPayRunRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run_id"),
                ProbeSupport.value(sent0, "pay_run_id"));
        var sent1 = channel.request("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "billing_address", "country_code"),
                ProbeSupport.value(sent1, "pay_stub", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "billing_address", "recipient"),
                ProbeSupport.value(sent1, "details", "destination", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "billing_address", "locality"),
                ProbeSupport.value(sent1, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "bank", "account_holder"),
                ProbeSupport.value(sent1, "pay_stub", "bank", "account_holder"));
        var sent2 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/SimulateTaxWithholding");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "billing_address", "region"),
                ProbeSupport.value(sent2, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "billing_address", "locality"),
                ProbeSupport.value(sent2, "details", "destination", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "pay_run", "audit", "updated_at"),
                ProbeSupport.value(sent2, "tax_withholding", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/GetPayRunRates"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/ComparePayStubOptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }
}
