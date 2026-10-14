package com.acme.finance.domains.payroll;

import com.acme.anomalies.v1.AnomalyQueryServiceGrpc;
import com.acme.anomalies.v1.BrowseAnomaliesRequest;
import com.acme.anomalies.v1.BrowseAnomaliesResponse;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.BatchCreateContractsRequest;
import com.acme.contracts.v1.BatchCreateContractsResponse;
import com.acme.contracts.v1.ContractServiceGrpc;
import com.acme.expenses.v1.CheckExpenseReportEligibilityRequest;
import com.acme.expenses.v1.CheckExpenseReportEligibilityResponse;
import com.acme.expenses.v1.ExpenseReport;
import com.acme.expenses.v1.ExpenseReportBankSettlement;
import com.acme.expenses.v1.ExpenseReportValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.ScoreLedgerAccountRequest;
import com.acme.ledger.v1.ScoreLedgerAccountResponse;
import com.acme.payroll.v1.CheckPayRunEligibilityRequest;
import com.acme.payroll.v1.CheckPayRunEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.ExplainPayRunDecisionRequest;
import com.acme.payroll.v1.ExplainPayRunDecisionResponse;
import com.acme.payroll.v1.GetPayRunRulesRequest;
import com.acme.payroll.v1.GetPayRunRulesResponse;
import com.acme.payroll.v1.LintPayRunsRequest;
import com.acme.payroll.v1.LintPayRunsResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunCardSettlement;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubAttachment;
import com.acme.payroll.v1.PayStubBankSettlement;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PreviewPayRunRequest;
import com.acme.payroll.v1.PreviewPayRunResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.RecalculatePayStubsRequest;
import com.acme.payroll.v1.RecalculatePayStubsResponse;
import com.acme.payroll.v1.SimulatePayStubRequest;
import com.acme.payroll.v1.SimulatePayStubResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.ValidatePayRunRequest;
import com.acme.payroll.v1.ValidatePayRunResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

class PayRunValidationApplicationTest {
    @Test
    void validatePayRunPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new PayRunValidationApplication(
                AnomalyQueryServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ContractServiceGrpc.newStub(channel),
                ExpenseReportValidationServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(ValidatePayRunRequest.getDefaultInstance());
        var response = application.validatePayRun(request);
        var sent0 = channel.request("acme.payroll.v1.PayStubCalculationService/EstimatePayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "billing_address", "lines"),
                ProbeSupport.value(sent0, "pay_stub", "billing_address", "lines"));
        var sent1 = channel.request("acme.expenses.v1.ExpenseReportValidationService/CheckExpenseReportEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "billing_address", "postal_code"),
                ProbeSupport.value(sent1, "expense_report", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "audit", "updated_at"),
                ProbeSupport.value(sent1, "expense_report", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "bank", "iban"),
                ProbeSupport.value(sent1, "expense_report", "bank", "iban"));
        var sent2 = channel.request("acme.payroll.v1.PayStubCalculationService/SimulatePayStub");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "lines", "tax", "exemptions"),
                ProbeSupport.value(sent2, "pay_stub", "lines", "tax", "exemptions"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "audit", "created_at"),
                ProbeSupport.value(response, "pay_run", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayStubCalculationService/EstimatePayStub"), "pay_stub", "due_at"),
                ProbeSupport.value(response, "pay_run", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/CheckExpenseReportEligibility"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.expenses.v1.ExpenseReportValidationService/CheckExpenseReportEligibility"), "expense_report", "audit", "updated_at"),
                ProbeSupport.value(response, "pay_run", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run_id"),
                ProbeSupport.value(response, "pay_run", "pay_run_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "pay_run", "issued_at"),
                ProbeSupport.value(response, "pay_run", "issued_at"));
    }
}
