package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationQueryServiceGrpc;
import com.acme.assets.v1.FindDepreciationsRequest;
import com.acme.assets.v1.FindDepreciationsResponse;
import com.acme.budgeting.v1.Budget;
import com.acme.budgeting.v1.BudgetCardFunding;
import com.acme.budgeting.v1.BudgetValidationServiceGrpc;
import com.acme.budgeting.v1.ScoreBudgetRequest;
import com.acme.budgeting.v1.ScoreBudgetResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CompareSanctionOptionsRequest;
import com.acme.compliance.v1.CompareSanctionOptionsResponse;
import com.acme.compliance.v1.Sanction;
import com.acme.compliance.v1.SanctionCalculationServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.CheckClaimEligibilityRequest;
import com.acme.insurance.v1.CheckClaimEligibilityResponse;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.ClaimValidationServiceGrpc;
import com.acme.insurance.v1.Policy;
import com.acme.insurance.v1.PolicyCardFunding;
import com.acme.insurance.v1.PolicyServiceGrpc;
import com.acme.insurance.v1.UpdatePolicyRequest;
import com.acme.insurance.v1.UpdatePolicyResponse;
import com.acme.ledger.v1.JournalEntryNotificationServiceGrpc;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesRequest;
import com.acme.ledger.v1.SubscribeJournalEntryUpdatesResponse;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanValidationServiceGrpc;
import com.acme.lending.v1.ScoreLoanRequest;
import com.acme.lending.v1.ScoreLoanResponse;
import com.acme.licensing.v1.FindSoftwareLicensesRequest;
import com.acme.licensing.v1.FindSoftwareLicensesResponse;
import com.acme.licensing.v1.SoftwareLicenseQueryServiceGrpc;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseOrderEligibilityResponse;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseOrderDecisionResponse;
import com.acme.procurement.v1.GetPurchaseOrderRulesRequest;
import com.acme.procurement.v1.GetPurchaseOrderRulesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRatesResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionRulesResponse;
import com.acme.procurement.v1.LintPurchaseOrdersRequest;
import com.acme.procurement.v1.LintPurchaseOrdersResponse;
import com.acme.procurement.v1.LintPurchaseRequisitionsRequest;
import com.acme.procurement.v1.LintPurchaseRequisitionsResponse;
import com.acme.procurement.v1.NormalizePurchaseOrderRequest;
import com.acme.procurement.v1.NormalizePurchaseOrderResponse;
import com.acme.procurement.v1.NormalizePurchaseRequisitionRequest;
import com.acme.procurement.v1.NormalizePurchaseRequisitionResponse;
import com.acme.procurement.v1.PreviewPurchaseOrderRequest;
import com.acme.procurement.v1.PreviewPurchaseOrderResponse;
import com.acme.procurement.v1.PreviewPurchaseRequisitionRequest;
import com.acme.procurement.v1.PreviewPurchaseRequisitionResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionAttachment;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionCardSettlement;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionLineTax;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsRequest;
import com.acme.procurement.v1.RecalculatePurchaseRequisitionsResponse;
import com.acme.procurement.v1.ScorePurchaseOrderRequest;
import com.acme.procurement.v1.ScorePurchaseOrderResponse;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.ValidatePurchaseOrderRequest;
import com.acme.procurement.v1.ValidatePurchaseOrderResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import com.acme.search.v1.BrowseSearchIndexesRequest;
import com.acme.search.v1.BrowseSearchIndexesResponse;
import com.acme.search.v1.SearchIndexQueryServiceGrpc;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class PurchaseOrderValidationRequestMapperImpl implements PurchaseOrderValidationRequestMapper {
    @Override
    public CheckClaimEligibilityRequest checkPurchaseOrderEligibilityStage0(CheckPurchaseOrderEligibilityRequest request) {
        return CheckClaimEligibilityRequest.newBuilder()
                .setClaim(Claim.newBuilder()
                        .setAssignee(UserRef.newBuilder()
                                .setDisplayName(request.getPurchaseOrder().getCounterparty().getDisplayName())
                                .build())
                        .setAudit(AuditInfo.newBuilder()
                                .setUpdatedAt(request.getPurchaseOrder().getAudit().getUpdatedAt())
                                .build())
                        .build())
                .setContext(request.getContext())
                .build();
    }
}
