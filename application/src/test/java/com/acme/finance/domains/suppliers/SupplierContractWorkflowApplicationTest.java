package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.audit.v1.ValidateAuditTrailRequest;
import com.acme.audit.v1.ValidateAuditTrailResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineLine;
import com.acme.budgeting.v1.BudgetLineLineTax;
import com.acme.budgeting.v1.BudgetLineWorkflowServiceGrpc;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowRequest;
import com.acme.budgeting.v1.ResumeBudgetLineWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CalculatePreferenceRequest;
import com.acme.customers.v1.CalculatePreferenceResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.ListCustomersRequest;
import com.acme.customers.v1.ListCustomersResponse;
import com.acme.customers.v1.Preference;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.QuotePreferenceRequest;
import com.acme.customers.v1.QuotePreferenceResponse;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.ResolveHouseholdsRequest;
import com.acme.customers.v1.ResolveHouseholdsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.ledger.v1.ResolveJournalEntriesRequest;
import com.acme.ledger.v1.ResolveJournalEntriesResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.lending.v1.StreamRepaymentMetricsRequest;
import com.acme.lending.v1.StreamRepaymentMetricsResponse;
import com.acme.lineage.v1.LineageNode;
import com.acme.lineage.v1.LineageNodeCalculationServiceGrpc;
import com.acme.lineage.v1.SimulateLineageNodeRequest;
import com.acme.lineage.v1.SimulateLineageNodeResponse;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRiskEligibilityResponse;
import com.acme.suppliers.v1.CreateSupplierRequest;
import com.acme.suppliers.v1.CreateSupplierResponse;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalRequest;
import com.acme.suppliers.v1.DelegateSupplierRatingApprovalResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierContractWorkflowsResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.NormalizeSupplierRiskRequest;
import com.acme.suppliers.v1.NormalizeSupplierRiskResponse;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierContractWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierContractWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierContractWorkflowResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierRatingBankSettlement;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.SupplierWorkflowServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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

class SupplierContractWorkflowApplicationTest {
    @Test
    void advanceSupplierContractWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierContractWorkflowApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                BudgetLineWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerServiceGrpc.newBlockingStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                PreferenceCalculationServiceGrpc.newBlockingStub(channel),
                JournalEntryQueryServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                LineageNodeCalculationServiceGrpc.newBlockingStub(channel),
                new SupplierContractWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingApprovalServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel),
                SupplierWorkflowServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(AdvanceSupplierContractWorkflowRequest.getDefaultInstance());
        var response = application.advanceSupplierContractWorkflow(request);
        var sent0 = channel.request("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "counterparty", "display_name"),
                ProbeSupport.value(sent0, "lineage_node", "children", "children", "manager", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "billing_address", "region"),
                ProbeSupport.value(sent0, "lineage_node", "children", "children", "location", "region"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierService/CreateSupplier");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode"), "lineage_node", "manager", "email"),
                ProbeSupport.value(sent1, "supplier", "relationship_manager", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode"), "lineage_node", "audit", "updated_by"),
                ProbeSupport.value(sent1, "supplier", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "billing_address", "lines"),
                ProbeSupport.value(sent1, "supplier", "primary_address", "lines"));
        var sent2 = channel.request("acme.audit.v1.AuditEventService/SearchAuditEvents");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode"), "lineage_node", "location", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "lines", "attributes"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "lines", "line_id"),
                ProbeSupport.value(sent2, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode"), "lineage_node", "audit", "created_at"),
                ProbeSupport.value(sent2, "filter", "created_after"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/CreateSupplier"), "supplier", "contact", "email"),
                ProbeSupport.value(sent3, "supplier_risk", "assignee", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.lineage.v1.LineageNodeCalculationService/SimulateLineageNode"), "lineage_node", "location", "lines"),
                ProbeSupport.value(response, "supplier_contract", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/CreateSupplier"), "supplier", "audit", "updated_at"),
                ProbeSupport.value(response, "supplier_contract", "audit", "updated_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierService/CreateSupplier"), "supplier", "audit", "updated_by"),
                ProbeSupport.value(response, "supplier_contract", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRiskValidationService/NormalizeSupplierRisk"), "supplier_risk", "assignee", "email"),
                ProbeSupport.value(response, "supplier_contract", "counterparty", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract_id"),
                ProbeSupport.value(response, "supplier_contract", "supplier_contract_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_contract", "number"),
                ProbeSupport.value(response, "supplier_contract", "number"));
    }
}
