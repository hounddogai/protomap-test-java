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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class DeductionValidationRequestMapperImpl implements DeductionValidationRequestMapper {
    @Override
    public ComparePayStubOptionsRequest previewDeductionStage2(PreviewDeductionRequest request, PreviewExpensePolicyResponse reply0) {
        return ComparePayStubOptionsRequest.newBuilder()
                .setContext(request.getContext())
                .setPayStub(PayStub.newBuilder()
                        .setAudit(AuditInfo.newBuilder()
                                .setEtag(reply0.getExpensePolicy().getAudit().getEtag())
                                .build())
                        .addLines(PayStubLine.newBuilder()
                                .setTax(PayStubLineTax.newBuilder()
                                        .setAmount(request.getDeduction().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
    }

    @Override
    public SimulateComplianceCaseRequest checkDeductionEligibilityStage3(CheckDeductionEligibilityRequest request, EstimatePayStubResponse reply1) {
        return SimulateComplianceCaseRequest.newBuilder()
                .setComplianceCase(ComplianceCase.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDeduction().getOwner().getUserId())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedBy(reply1.getPayStub().getAudit().getUpdatedBy())
                                .build())
                        .addComments(ComplianceCaseComment.newBuilder()
                                .setCreatedAt(reply1.getPayStub().getAudit().getCreatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
