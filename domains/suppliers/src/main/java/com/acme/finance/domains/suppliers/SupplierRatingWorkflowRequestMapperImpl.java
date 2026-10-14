package com.acme.finance.domains.suppliers;

import com.acme.audiences.v1.AudienceQueryServiceGrpc;
import com.acme.audiences.v1.CountAudiencesRequest;
import com.acme.audiences.v1.CountAudiencesResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineBankSettlement;
import com.acme.budgeting.v1.BudgetLineLifecycleServiceGrpc;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryRequest;
import com.acme.budgeting.v1.ScheduleBudgetLineExpiryResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.Contract;
import com.acme.contracts.v1.ContractApprovalServiceGrpc;
import com.acme.contracts.v1.ContractCardSettlement;
import com.acme.contracts.v1.EscalateContractRequest;
import com.acme.contracts.v1.EscalateContractResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.LintCustomersRequest;
import com.acme.customers.v1.LintCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceDiscountAction;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.ValidateConsentRecordRequest;
import com.acme.customers.v1.ValidateConsentRecordResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.fleet.v1.ListVehiclesRequest;
import com.acme.fleet.v1.ListVehiclesResponse;
import com.acme.fleet.v1.VehicleServiceGrpc;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntry;
import com.acme.ledger.v1.JournalEntryLine;
import com.acme.ledger.v1.JournalEntryValidationServiceGrpc;
import com.acme.ledger.v1.NormalizeJournalEntryRequest;
import com.acme.ledger.v1.NormalizeJournalEntryResponse;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionRequest;
import com.acme.suppliers.v1.ExplainSupplierRatingDecisionResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierRatingWorkflowsResponse;
import com.acme.suppliers.v1.ListSuppliersRequest;
import com.acme.suppliers.v1.ListSuppliersResponse;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierRatingWorkflowResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class SupplierRatingWorkflowRequestMapperImpl implements SupplierRatingWorkflowRequestMapper {
    @Override
    public ListSuppliersRequest retrySupplierRatingWorkflowStage1(RetrySupplierRatingWorkflowRequest request, EscalateContractResponse reply0) {
        return ListSuppliersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(ListSuppliersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getSupplierRating().getAudit().getCreatedAt())
                        .setOwnerId(reply0.getResult().getCorrelationId())
                        .setRegion(request.getSupplierRating().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
