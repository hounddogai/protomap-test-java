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
import com.acme.finance.testing.ProbeSupport;
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
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingCardSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRisk;
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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

class SupplierRatingWorkflowApplicationTest {
    @Test
    void retrySupplierRatingWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierRatingWorkflowApplication(
                AudienceQueryServiceGrpc.newBlockingStub(channel),
                BudgetLineLifecycleServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                ContractApprovalServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                VehicleServiceGrpc.newBlockingStub(channel),
                JournalEntryValidationServiceGrpc.newBlockingStub(channel),
                new SupplierRatingWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel),
                SupplierWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetrySupplierRatingWorkflowRequest.getDefaultInstance());
        var response = application.retrySupplierRatingWorkflow(request);
        var sent0 = channel.request("acme.contracts.v1.ContractApprovalService/EscalateContract");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "card", "last4"),
                ProbeSupport.value(sent0, "contract", "card", "last4"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "details", "approver", "display_name"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierService/ListSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/EscalateContract"), "result", "correlation_id"),
                ProbeSupport.value(sent1, "filter", "owner_id"));
        var sent2 = channel.request("acme.customers.v1.PreferenceCalculationService/QuotePreference");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "lines", "tax", "amount"),
                ProbeSupport.value(sent2, "preference", "discount", "amount"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "lines", "description"),
                ProbeSupport.value(sent2, "preference", "description"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "audit", "updated_by"),
                ProbeSupport.value(sent2, "preference", "audit", "updated_by"));
        var sent3 = channel.request("acme.budgeting.v1.BudgetLineLifecycleService/ScheduleBudgetLineExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/EscalateContract"), "contract", "bank", "account_holder"),
                ProbeSupport.value(sent3, "budget_line", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "counterparty", "email"),
                ProbeSupport.value(sent3, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.PreferenceCalculationService/QuotePreference"), "preference", "audit", "updated_at"),
                ProbeSupport.value(sent3, "budget_line", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/EscalateContract"), "contract", "audit", "updated_by"),
                ProbeSupport.value(sent3, "budget_line", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ContractApprovalService/EscalateContract"), "contract", "billing_address", "recipient"),
                ProbeSupport.value(response, "supplier_rating", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/ListSuppliers"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/ListSuppliers"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetLineLifecycleService/ScheduleBudgetLineExpiry"), "budget_line", "total"),
                ProbeSupport.value(response, "supplier_rating", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating_id"),
                ProbeSupport.value(response, "supplier_rating", "supplier_rating_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_rating", "number"),
                ProbeSupport.value(response, "supplier_rating", "number"));
    }
}
