package com.acme.finance.domains.payroll;

import com.acme.audit.v1.ListRetentionPolicySubscriptionsRequest;
import com.acme.audit.v1.ListRetentionPolicySubscriptionsResponse;
import com.acme.audit.v1.RepairRetentionPolicyRequest;
import com.acme.audit.v1.RepairRetentionPolicyResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyAdminServiceGrpc;
import com.acme.audit.v1.RetentionPolicyNotificationServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.expenses.v1.ReceiptLifecycleServiceGrpc;
import com.acme.expenses.v1.ScheduleReceiptExpiryRequest;
import com.acme.expenses.v1.ScheduleReceiptExpiryResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.fraud.v1.RiskAssessmentServiceGrpc;
import com.acme.fraud.v1.SearchRiskAssessmentsRequest;
import com.acme.fraud.v1.SearchRiskAssessmentsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.payroll.v1.ActivatePayStubRequest;
import com.acme.payroll.v1.ActivatePayStubResponse;
import com.acme.payroll.v1.BrowsePayStubsRequest;
import com.acme.payroll.v1.BrowsePayStubsResponse;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsRequest;
import com.acme.payroll.v1.CompareTaxWithholdingOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.DeductionReportingServiceGrpc;
import com.acme.payroll.v1.DeductionValidationServiceGrpc;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExpirePayStubRequest;
import com.acme.payroll.v1.ExpirePayStubResponse;
import com.acme.payroll.v1.FindPayStubsRequest;
import com.acme.payroll.v1.FindPayStubsResponse;
import com.acme.payroll.v1.GenerateDeductionReportRequest;
import com.acme.payroll.v1.GenerateDeductionReportResponse;
import com.acme.payroll.v1.GetPayStubLifecycleRequest;
import com.acme.payroll.v1.GetPayStubLifecycleResponse;
import com.acme.payroll.v1.GetPayStubRatesRequest;
import com.acme.payroll.v1.GetPayStubRatesResponse;
import com.acme.payroll.v1.LintTaxWithholdingsRequest;
import com.acme.payroll.v1.LintTaxWithholdingsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubQueryServiceGrpc;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PeekPayStubRequest;
import com.acme.payroll.v1.PeekPayStubResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculateTaxWithholdingsRequest;
import com.acme.payroll.v1.RecalculateTaxWithholdingsResponse;
import com.acme.payroll.v1.ReinstatePayStubRequest;
import com.acme.payroll.v1.ReinstatePayStubResponse;
import com.acme.payroll.v1.RenewPayStubRequest;
import com.acme.payroll.v1.RenewPayStubResponse;
import com.acme.payroll.v1.SchedulePayStubExpiryRequest;
import com.acme.payroll.v1.SchedulePayStubExpiryResponse;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.ScorePayStubRequest;
import com.acme.payroll.v1.ScorePayStubResponse;
import com.acme.payroll.v1.SuspendPayStubRequest;
import com.acme.payroll.v1.SuspendPayStubResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingValidationServiceGrpc;
import com.acme.payroll.v1.TerminatePayStubRequest;
import com.acme.payroll.v1.TerminatePayStubResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.acme.surveys.v1.CountSurveysRequest;
import com.acme.surveys.v1.CountSurveysResponse;
import com.acme.surveys.v1.SurveyQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PayStubLifecycleApplicationTest {
    @Test
    void suspendPayStubPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var listener = new PayStubLifecycleApplicationEvents(RetentionPolicyAdminServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PayStubLifecycleApplication.TerminatePayStubStage1Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PayStubLifecycleApplication(
                RetentionPolicyNotificationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                ReceiptLifecycleServiceGrpc.newBlockingStub(channel),
                RiskAssessmentServiceGrpc.newFutureStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                new PayStubLifecycleRequestMapperImpl(),
                DeductionReportingServiceGrpc.newBlockingStub(channel),
                DeductionValidationServiceGrpc.newFutureStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                PayStubQueryServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierWorkflowServiceGrpc.newBlockingStub(channel),
                SurveyQueryServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(SuspendPayStubRequest.getDefaultInstance());
        var response = application.suspendPayStub(request);
        var sent0 = channel.request("acme.payroll.v1.PayStubValidationService/ScorePayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "display_name"),
                ProbeSupport.value(sent0, "pay_stub", "counterparty", "display_name"));
        var sent1 = channel.request("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ScorePayStub"), "pay_stub", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "details", "destination", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "new_owner", "email"),
                ProbeSupport.value(sent1, "tax_withholding", "owner", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ScorePayStub"), "pay_stub", "counterparty", "display_name"),
                ProbeSupport.value(sent1, "tax_withholding", "owner", "display_name"));
        var sent2 = channel.request("acme.payroll.v1.PayStubCalculationService/GetPayStubRates");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub_id"),
                ProbeSupport.value(sent2, "pay_stub_id"));
        var sent3 = channel.request("acme.surveys.v1.SurveyQueryService/CountSurveys");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ScorePayStub"), "pay_stub", "billing_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/GetPayStubRates"), "pay_stub", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubValidationService/ScorePayStub"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings"), "tax_withholding", "audit", "created_by"),
                ProbeSupport.value(response, "pay_stub", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.TaxWithholdingCalculationService/RecalculateTaxWithholdings"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/GetPayStubRates"), "pay_stub", "counterparty"),
                ProbeSupport.value(response, "pay_stub", "counterparty"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/GetPayStubRates"), "pay_stub", "status"),
                ProbeSupport.value(response, "pay_stub", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_stub_id"),
                ProbeSupport.value(response, "pay_stub", "pay_stub_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PayStubLifecycleApplicationEvents(RetentionPolicyAdminServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<RepairRetentionPolicyResponse>();
        listener.on(new PayStubLifecycleApplication.TerminatePayStubStage1Requested(ProbeSupport.sample(RepairRetentionPolicyRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
