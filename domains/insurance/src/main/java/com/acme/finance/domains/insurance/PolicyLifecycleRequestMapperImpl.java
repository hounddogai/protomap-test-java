package com.acme.finance.domains.insurance;

import com.acme.accounting.v1.ChartOfAccounts;
import com.acme.accounting.v1.ChartOfAccountsValidationServiceGrpc;
import com.acme.accounting.v1.ValidateChartOfAccountsRequest;
import com.acme.accounting.v1.ValidateChartOfAccountsResponse;
import com.acme.analytics.v1.AnalyticsEvent;
import com.acme.analytics.v1.AnalyticsEventServiceGrpc;
import com.acme.analytics.v1.CreateAnalyticsEventRequest;
import com.acme.analytics.v1.CreateAnalyticsEventResponse;
import com.acme.analytics.v1.FunnelRef;
import com.acme.assets.v1.CheckFixedAssetEligibilityRequest;
import com.acme.assets.v1.CheckFixedAssetEligibilityResponse;
import com.acme.assets.v1.FixedAsset;
import com.acme.assets.v1.FixedAssetValidationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCaseServiceGrpc;
import com.acme.compliance.v1.ListComplianceCasesRequest;
import com.acme.compliance.v1.ListComplianceCasesResponse;
import com.acme.customers.v1.CalculateHouseholdRequest;
import com.acme.customers.v1.CalculateHouseholdResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentDiscountAction;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.EstimateHouseholdRequest;
import com.acme.customers.v1.EstimateHouseholdResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.customers.v1.RecalculateHouseholdsRequest;
import com.acme.customers.v1.RecalculateHouseholdsResponse;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.ValidateCustomerSegmentRequest;
import com.acme.customers.v1.ValidateCustomerSegmentResponse;
import com.acme.documents.v1.Document;
import com.acme.documents.v1.DocumentValidationServiceGrpc;
import com.acme.documents.v1.LintDocumentsRequest;
import com.acme.documents.v1.LintDocumentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.giftcards.v1.CreateGiftCardRequest;
import com.acme.giftcards.v1.CreateGiftCardResponse;
import com.acme.giftcards.v1.GiftCard;
import com.acme.giftcards.v1.GiftCardServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ActivatePolicyRequest;
import com.acme.insurance.v1.ActivatePolicyResponse;
import com.acme.insurance.v1.CalculatePolicyRequest;
import com.acme.insurance.v1.CalculatePolicyResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimCalculationServiceGrpc;
import com.acme.insurance.v1.ClaimRef;
import com.acme.insurance.v1.Coverage;
import com.acme.insurance.v1.CoverageCalculationServiceGrpc;
import com.acme.insurance.v1.CoverageRef;
import com.acme.insurance.v1.CoverageRouteAction;
import com.acme.insurance.v1.CoverageValidationServiceGrpc;
import com.acme.insurance.v1.ExpirePolicyRequest;
import com.acme.insurance.v1.ExpirePolicyResponse;
import com.acme.insurance.v1.ExplainClaimCalculationRequest;
import com.acme.insurance.v1.ExplainClaimCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageCalculationRequest;
import com.acme.insurance.v1.ExplainCoverageCalculationResponse;
import com.acme.insurance.v1.ExplainCoverageDecisionRequest;
import com.acme.insurance.v1.ExplainCoverageDecisionResponse;
import com.acme.insurance.v1.ExplainUnderwritingDecisionRequest;
import com.acme.insurance.v1.ExplainUnderwritingDecisionResponse;
import com.acme.insurance.v1.GetCoverageRulesRequest;
import com.acme.insurance.v1.GetCoverageRulesResponse;
import com.acme.insurance.v1.GetPolicyLifecycleRequest;
import com.acme.insurance.v1.GetPolicyLifecycleResponse;
import com.acme.insurance.v1.NormalizeCoverageRequest;
import com.acme.insurance.v1.NormalizeCoverageResponse;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCalculationServiceGrpc;
import com.acme.insurance.v1.PolicyLimit;
import com.acme.insurance.v1.QuoteCoverageRequest;
import com.acme.insurance.v1.QuoteCoverageResponse;
import com.acme.insurance.v1.ReinstatePolicyRequest;
import com.acme.insurance.v1.ReinstatePolicyResponse;
import com.acme.insurance.v1.RenewPolicyRequest;
import com.acme.insurance.v1.RenewPolicyResponse;
import com.acme.insurance.v1.SchedulePolicyExpiryRequest;
import com.acme.insurance.v1.SchedulePolicyExpiryResponse;
import com.acme.insurance.v1.SuspendPolicyRequest;
import com.acme.insurance.v1.SuspendPolicyResponse;
import com.acme.insurance.v1.TerminatePolicyRequest;
import com.acme.insurance.v1.TerminatePolicyResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingValidationServiceGrpc;
import com.acme.ledger.v1.PostingBatch;
import com.acme.ledger.v1.PostingBatchLine;
import com.acme.ledger.v1.PostingBatchLineTax;
import com.acme.ledger.v1.PostingBatchValidationServiceGrpc;
import com.acme.ledger.v1.ValidatePostingBatchRequest;
import com.acme.ledger.v1.ValidatePostingBatchResponse;
import com.acme.payroll.v1.DeductionRef;
import com.acme.payroll.v1.ExplainPayStubCalculationRequest;
import com.acme.payroll.v1.ExplainPayStubCalculationResponse;
import com.acme.payroll.v1.PayStub;
import com.acme.payroll.v1.PayStubCalculationServiceGrpc;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.treasury.v1.BatchCreateCashPositionsRequest;
import com.acme.treasury.v1.BatchCreateCashPositionsResponse;
import com.acme.treasury.v1.CashPositionServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PolicyLifecycleRequestMapperImpl implements PolicyLifecycleRequestMapper {
    @Override
    public ExplainUnderwritingDecisionRequest activatePolicyStage0(ActivatePolicyRequest request) {
        return ExplainUnderwritingDecisionRequest.newBuilder()
                .setContext(request.getContext())
                .setDetails(ExplainUnderwritingDecisionRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getDetails().getNotifyContact().getEmail())
                                .build())
                        .build())
                .setUnderwriting(Underwriting.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setUserId(request.getDetails().getNewOwner().getUserId())
                                .build())
                        .setCustomer(ClaimRef.newBuilder()
                                .setDisplayName(request.getDetails().getNewOwner().getDisplayName())
                                .build())
                        .setRequester(ContactInfo.newBuilder()
                                .setName(request.getDetails().getNotifyContact().getName())
                                .build())
                        .build())
                .build();
    }

    @Override
    public ValidatePostingBatchRequest schedulePolicyExpiryStage2(SchedulePolicyExpiryRequest request, SimulateCustomerSegmentResponse reply1) {
        return ValidatePostingBatchRequest.newBuilder()
                .setContext(request.getContext())
                .setPostingBatch(PostingBatch.newBuilder()
                        .addLines(PostingBatchLine.newBuilder()
                                .setTax(PostingBatchLineTax.newBuilder()
                                        .setAmount(reply1.getCustomerSegment().getDiscount().getAmount())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
