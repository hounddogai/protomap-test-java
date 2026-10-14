package com.acme.finance.domains.suppliers;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.corporateevents.v1.BatchCreateCorporateEventsRequest;
import com.acme.corporateevents.v1.BatchCreateCorporateEventsResponse;
import com.acme.corporateevents.v1.CorporateEvent;
import com.acme.corporateevents.v1.CorporateEventServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryRequest;
import com.acme.insurance.v1.ScheduleUnderwritingExpiryResponse;
import com.acme.insurance.v1.Underwriting;
import com.acme.insurance.v1.UnderwritingLifecycleServiceGrpc;
import com.acme.ledger.v1.LedgerAccountServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountsRequest;
import com.acme.ledger.v1.ListLedgerAccountsResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexServiceGrpc;
import com.acme.search.v1.UpdateSearchIndexRequest;
import com.acme.search.v1.UpdateSearchIndexResponse;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowRequest;
import com.acme.suppliers.v1.AdvanceSupplierWorkflowResponse;
import com.acme.suppliers.v1.BrowseSuppliersRequest;
import com.acme.suppliers.v1.BrowseSuppliersResponse;
import com.acme.suppliers.v1.CancelSupplierWorkflowRequest;
import com.acme.suppliers.v1.CancelSupplierWorkflowResponse;
import com.acme.suppliers.v1.EscalateSupplierContractRequest;
import com.acme.suppliers.v1.EscalateSupplierContractResponse;
import com.acme.suppliers.v1.InspectSupplierWorkflowRequest;
import com.acme.suppliers.v1.InspectSupplierWorkflowResponse;
import com.acme.suppliers.v1.ListPendingSupplierContractsRequest;
import com.acme.suppliers.v1.ListPendingSupplierContractsResponse;
import com.acme.suppliers.v1.ListSupplierWorkflowsRequest;
import com.acme.suppliers.v1.ListSupplierWorkflowsResponse;
import com.acme.suppliers.v1.LookupSupplierRequest;
import com.acme.suppliers.v1.LookupSupplierResponse;
import com.acme.suppliers.v1.PauseSupplierWorkflowRequest;
import com.acme.suppliers.v1.PauseSupplierWorkflowResponse;
import com.acme.suppliers.v1.PeekSupplierRequest;
import com.acme.suppliers.v1.PeekSupplierResponse;
import com.acme.suppliers.v1.PreviewSupplierRiskRequest;
import com.acme.suppliers.v1.PreviewSupplierRiskResponse;
import com.acme.suppliers.v1.RecallSupplierContractRequest;
import com.acme.suppliers.v1.RecallSupplierContractResponse;
import com.acme.suppliers.v1.ResumeSupplierWorkflowRequest;
import com.acme.suppliers.v1.ResumeSupplierWorkflowResponse;
import com.acme.suppliers.v1.RetrySupplierWorkflowRequest;
import com.acme.suppliers.v1.RetrySupplierWorkflowResponse;
import com.acme.suppliers.v1.SignalSupplierWorkflowRequest;
import com.acme.suppliers.v1.SignalSupplierWorkflowResponse;
import com.acme.suppliers.v1.StartSupplierWorkflowRequest;
import com.acme.suppliers.v1.StartSupplierWorkflowResponse;
import com.acme.suppliers.v1.SuggestSuppliersRequest;
import com.acme.suppliers.v1.SuggestSuppliersResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractApprovalServiceGrpc;
import com.acme.suppliers.v1.SupplierContractQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierPreferences;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierServiceGrpc;
import com.acme.suppliers.v1.WatchSuppliersRequest;
import com.acme.suppliers.v1.WatchSuppliersResponse;
import com.acme.timekeeping.v1.CreateTimesheetRequest;
import com.acme.timekeeping.v1.CreateTimesheetResponse;
import com.acme.timekeeping.v1.Timesheet;
import com.acme.timekeeping.v1.TimesheetServiceGrpc;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
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

class SupplierWorkflowApplicationTest {
    @Test
    void retrySupplierWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var application = new SupplierWorkflowApplication(
                ChannelRegistry.fixed(channel),
                CorporateEventServiceGrpc.newStub(channel),
                UnderwritingLifecycleServiceGrpc.newBlockingStub(channel),
                LedgerAccountServiceGrpc.newBlockingStub(channel),
                new SupplierWorkflowRequestMapperImpl(),
                repository,
                Runnable::run,
                SearchIndexServiceGrpc.newBlockingStub(channel),
                SupplierContractApprovalServiceGrpc.newBlockingStub(channel),
                SupplierContractQueryServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRiskValidationServiceGrpc.newBlockingStub(channel),
                SupplierServiceGrpc.newBlockingStub(channel),
                TimesheetServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RetrySupplierWorkflowRequest.getDefaultInstance());
        var response = application.retrySupplierWorkflow(request);
        var sent0 = channel.request("acme.venues.v1.VenueValidationService/LintVenues");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "contact", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "audit", "created_at"),
                ProbeSupport.value(sent0, "venue", "audit", "created_at"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierQueryService/PeekSupplier");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "supplier_id"),
                ProbeSupport.value(sent1, "supplier_id"));
        var sent2 = channel.request("acme.suppliers.v1.SupplierQueryService/LookupSupplier");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier_id"),
                ProbeSupport.value(sent2, "supplier_id"));
        var sent3 = channel.request("acme.suppliers.v1.SupplierContractApprovalService/ListPendingSupplierContracts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/LookupSupplier"), "supplier", "primary_address", "region"),
                ProbeSupport.value(sent3, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/PeekSupplier"), "supplier", "audit", "created_at"),
                ProbeSupport.value(sent3, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/LintVenues"), "venue", "components", "component_id"),
                ProbeSupport.value(sent3, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.venues.v1.VenueValidationService/LintVenues"), "venue", "audit", "created_at"),
                ProbeSupport.value(response, "supplier", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/PeekSupplier"), "supplier", "supplier_id"),
                ProbeSupport.value(response, "supplier", "supplier_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/LookupSupplier"), "supplier", "preferences", "language"),
                ProbeSupport.value(response, "supplier", "preferences", "language"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/LookupSupplier"), "supplier", "primary_address", "country_code"),
                ProbeSupport.value(response, "supplier", "primary_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierContractApprovalService/ListPendingSupplierContracts"), "result", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "name"),
                ProbeSupport.value(response, "supplier", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "supplier", "name"),
                ProbeSupport.value(response, "supplier", "name"));
    }
}
