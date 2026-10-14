package com.acme.finance.domains.payroll;

import com.acme.assets.v1.BatchCreateFixedAssetsRequest;
import com.acme.assets.v1.BatchCreateFixedAssetsResponse;
import com.acme.assets.v1.FixedAssetServiceGrpc;
import com.acme.audit.v1.AuditTrailNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.RetentionPolicyValidationServiceGrpc;
import com.acme.audit.v1.ScoreRetentionPolicyRequest;
import com.acme.audit.v1.ScoreRetentionPolicyResponse;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditTrailUpdatesResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseServiceGrpc;
import com.acme.collections.v1.UpdateCollectionCaseRequest;
import com.acme.collections.v1.UpdateCollectionCaseResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseComment;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.compliance.v1.SimulateComplianceCaseRequest;
import com.acme.compliance.v1.SimulateComplianceCaseResponse;
import com.acme.expenses.v1.ExpensePolicy;
import com.acme.expenses.v1.ExpensePolicyCondition;
import com.acme.expenses.v1.ExpensePolicyValidationServiceGrpc;
import com.acme.expenses.v1.NormalizeExpensePolicyRequest;
import com.acme.expenses.v1.NormalizeExpensePolicyResponse;
import com.acme.expenses.v1.PreviewExpensePolicyRequest;
import com.acme.expenses.v1.PreviewExpensePolicyResponse;
import com.acme.expenses.v1.ScoreExpensePolicyRequest;
import com.acme.expenses.v1.ScoreExpensePolicyResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LintRepaymentsRequest;
import com.acme.lending.v1.LintRepaymentsResponse;
import com.acme.lending.v1.Repayment;
import com.acme.lending.v1.RepaymentParticipant;
import com.acme.lending.v1.RepaymentValidationServiceGrpc;
import com.acme.payroll.v1.CheckDeductionEligibilityRequest;
import com.acme.payroll.v1.CheckDeductionEligibilityResponse;
import com.acme.payroll.v1.ComparePayStubOptionsRequest;
import com.acme.payroll.v1.ComparePayStubOptionsResponse;
import com.acme.payroll.v1.Deduction;
import com.acme.payroll.v1.DeductionCalculationServiceGrpc;
import com.acme.payroll.v1.DeductionCondition;
import com.acme.payroll.v1.EstimatePayStubRequest;
import com.acme.payroll.v1.EstimatePayStubResponse;
import com.acme.payroll.v1.EstimateTaxWithholdingRequest;
import com.acme.payroll.v1.EstimateTaxWithholdingResponse;
import com.acme.payroll.v1.ExplainDeductionCalculationRequest;
import com.acme.payroll.v1.ExplainDeductionCalculationResponse;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.GetDeductionRulesRequest;
import com.acme.payroll.v1.GetDeductionRulesResponse;
import com.acme.payroll.v1.LintDeductionsRequest;
import com.acme.payroll.v1.LintDeductionsResponse;
import com.acme.payroll.v1.NormalizeDeductionRequest;
import com.acme.payroll.v1.NormalizeDeductionResponse;
import com.acme.payroll.v1.NormalizePayStubRequest;
import com.acme.payroll.v1.NormalizePayStubResponse;
import com.acme.payroll.v1.PayRun;
import com.acme.payroll.v1.PayRunCalculationServiceGrpc;
import com.acme.payroll.v1.PayRunLine;
import com.acme.payroll.v1.PayRunLineTax;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.payroll.v1.PayStubLine;
import com.acme.payroll.v1.PayStubLineTax;
import com.acme.payroll.v1.PayStubValidationServiceGrpc;
import com.acme.payroll.v1.PreviewDeductionRequest;
import com.acme.payroll.v1.PreviewDeductionResponse;
import com.acme.payroll.v1.PreviewPayStubRequest;
import com.acme.payroll.v1.PreviewPayStubResponse;
import com.acme.payroll.v1.QuotePayRunRequest;
import com.acme.payroll.v1.QuotePayRunResponse;
import com.acme.payroll.v1.ScoreDeductionRequest;
import com.acme.payroll.v1.ScoreDeductionResponse;
import com.acme.payroll.v1.SimulateTaxWithholdingRequest;
import com.acme.payroll.v1.SimulateTaxWithholdingResponse;
import com.acme.payroll.v1.TaxWithholding;
import com.acme.payroll.v1.TaxWithholdingCalculationServiceGrpc;
import com.acme.payroll.v1.TaxWithholdingCondition;
import com.acme.payroll.v1.TaxWithholdingDiscountAction;
import com.acme.payroll.v1.ValidateDeductionRequest;
import com.acme.payroll.v1.ValidateDeductionResponse;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class DeductionValidationApplicationTest {
    @Test
    void scoreDeductionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new PayrollRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveDeduction(String tenant, Deduction value) {}
            public void savePayRun(String tenant, PayRun value) {}
            public void savePayStub(String tenant, PayStub value) {}
            public void saveTaxWithholding(String tenant, TaxWithholding value) {}
        };
        var application = new DeductionValidationApplication(
                FixedAssetServiceGrpc.newStub(channel),
                AuditTrailNotificationServiceGrpc.newBlockingStub(channel),
                RetentionPolicySyncServiceGrpc.newStub(channel),
                RetentionPolicyValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CollectionCaseServiceGrpc.newBlockingStub(channel),
                ComplianceCaseCalculationServiceGrpc.newBlockingStub(channel),
                ExpensePolicyValidationServiceGrpc.newBlockingStub(channel),
                RepaymentValidationServiceGrpc.newBlockingStub(channel),
                new DeductionValidationRequestMapperImpl(),
                DeductionCalculationServiceGrpc.newBlockingStub(channel),
                PayRunCalculationServiceGrpc.newBlockingStub(channel),
                PayStubCalculationServiceGrpc.newBlockingStub(channel),
                PayStubValidationServiceGrpc.newBlockingStub(channel),
                TaxWithholdingCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ScoreDeductionRequest.getDefaultInstance());
        var response = application.scoreDeduction(request);
        var sent0 = channel.request("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "owner", "user_id"),
                ProbeSupport.value(sent0, "retention_policy", "owner", "user_id"));
        var sent1 = channel.request("acme.payroll.v1.DeductionCalculationService/ExplainDeductionCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "operator"),
                ProbeSupport.value(sent1, "deduction", "conditions", "operator"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "negate"),
                ProbeSupport.value(sent1, "deduction", "conditions", "negate"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "value"),
                ProbeSupport.value(sent1, "deduction", "conditions", "value"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "attribute"),
                ProbeSupport.value(sent1, "deduction", "conditions", "attribute"));
        var sent2 = channel.request("acme.expenses.v1.ExpensePolicyValidationService/NormalizeExpensePolicy");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "operator"),
                ProbeSupport.value(sent2, "expense_policy", "conditions", "operator"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "conditions", "attribute"),
                ProbeSupport.value(sent2, "expense_policy", "conditions", "attribute"));
        var sent3 = channel.request("acme.payroll.v1.PayRunCalculationService/QuotePayRun");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.DeductionCalculationService/ExplainDeductionCalculation"), "deduction", "discount", "amount"),
                ProbeSupport.value(sent3, "pay_run", "lines", "tax", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "audit", "updated_at"),
                ProbeSupport.value(sent3, "pay_run", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "audit", "updated_by"),
                ProbeSupport.value(sent3, "pay_run", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "owner"),
                ProbeSupport.value(response, "deduction", "owner"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.RetentionPolicyValidationService/ScoreRetentionPolicy"), "retention_policy", "parameters"),
                ProbeSupport.value(response, "deduction", "parameters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.payroll.v1.PayRunCalculationService/QuotePayRun"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction_id"),
                ProbeSupport.value(response, "deduction", "deduction_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "deduction", "priority"),
                ProbeSupport.value(response, "deduction", "priority"));
    }
}
