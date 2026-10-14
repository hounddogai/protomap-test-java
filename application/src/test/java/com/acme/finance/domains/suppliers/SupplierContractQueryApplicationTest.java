package com.acme.finance.domains.suppliers;

import com.acme.audit.v1.AuditEventNotificationServiceGrpc;
import com.acme.audit.v1.RetentionPolicy;
import com.acme.audit.v1.RetentionPolicyBlockAction;
import com.acme.audit.v1.RetentionPolicySyncServiceGrpc;
import com.acme.audit.v1.StreamRetentionPolicyChangesRequest;
import com.acme.audit.v1.StreamRetentionPolicyChangesResponse;
import com.acme.audit.v1.SubscribeAuditEventUpdatesRequest;
import com.acme.audit.v1.SubscribeAuditEventUpdatesResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.LoanApplication;
import com.acme.lending.v1.LoanApplicationCalculationServiceGrpc;
import com.acme.lending.v1.LoanApplicationLine;
import com.acme.lending.v1.RecalculateLoanApplicationsRequest;
import com.acme.lending.v1.RecalculateLoanApplicationsResponse;
import com.acme.lending.v1.SimulateLoanApplicationRequest;
import com.acme.lending.v1.SimulateLoanApplicationResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.acme.risk.v1.WatchRiskRegistersRequest;
import com.acme.risk.v1.WatchRiskRegistersResponse;
import com.acme.suppliers.v1.BrowseSupplierContractsRequest;
import com.acme.suppliers.v1.BrowseSupplierContractsResponse;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityRequest;
import com.acme.suppliers.v1.CheckSupplierRatingEligibilityResponse;
import com.acme.suppliers.v1.CountSupplierContractsRequest;
import com.acme.suppliers.v1.CountSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSupplierContractsRequest;
import com.acme.suppliers.v1.ExportSupplierContractsResponse;
import com.acme.suppliers.v1.ExportSuppliersRequest;
import com.acme.suppliers.v1.ExportSuppliersResponse;
import com.acme.suppliers.v1.FindSupplierContractsRequest;
import com.acme.suppliers.v1.FindSupplierContractsResponse;
import com.acme.suppliers.v1.FindSuppliersRequest;
import com.acme.suppliers.v1.FindSuppliersResponse;
import com.acme.suppliers.v1.LookupSupplierContractRequest;
import com.acme.suppliers.v1.LookupSupplierContractResponse;
import com.acme.suppliers.v1.NormalizeSupplierRatingRequest;
import com.acme.suppliers.v1.NormalizeSupplierRatingResponse;
import com.acme.suppliers.v1.PeekSupplierContractRequest;
import com.acme.suppliers.v1.PeekSupplierContractResponse;
import com.acme.suppliers.v1.ResolveSupplierContractsRequest;
import com.acme.suppliers.v1.ResolveSupplierContractsResponse;
import com.acme.suppliers.v1.SuggestSupplierContractsRequest;
import com.acme.suppliers.v1.SuggestSupplierContractsResponse;
import com.acme.suppliers.v1.SummarizeSupplierContractsRequest;
import com.acme.suppliers.v1.SummarizeSupplierContractsResponse;
import com.acme.suppliers.v1.Supplier;
import com.acme.suppliers.v1.SupplierContract;
import com.acme.suppliers.v1.SupplierContractLine;
import com.acme.suppliers.v1.SupplierQueryServiceGrpc;
import com.acme.suppliers.v1.SupplierRating;
import com.acme.suppliers.v1.SupplierRatingValidationServiceGrpc;
import com.acme.suppliers.v1.SupplierRef;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.ValidateSupplierRatingRequest;
import com.acme.suppliers.v1.ValidateSupplierRatingResponse;
import com.acme.venues.v1.CheckVenueEligibilityRequest;
import com.acme.venues.v1.CheckVenueEligibilityResponse;
import com.acme.venues.v1.LintVenuesRequest;
import com.acme.venues.v1.LintVenuesResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class SupplierContractQueryApplicationTest {
    @Test
    void exportSupplierContractsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new SuppliersRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveSupplier(String tenant, Supplier value) {}
            public void saveSupplierContract(String tenant, SupplierContract value) {}
            public void saveSupplierRating(String tenant, SupplierRating value) {}
            public void saveSupplierRisk(String tenant, SupplierRisk value) {}
        };
        var listener = new SupplierContractQueryApplicationEvents(AuditEventNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof SupplierContractQueryApplication.SuggestSupplierContractsStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new SupplierContractQueryApplication(
                RetentionPolicySyncServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                events,
                LoanApplicationCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                RiskRegisterServiceGrpc.newStub(channel),
                RiskRegisterServiceGrpc.newBlockingStub(channel),
                SupplierQueryServiceGrpc.newBlockingStub(channel),
                SupplierRatingValidationServiceGrpc.newBlockingStub(channel),
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(ExportSupplierContractsRequest.getDefaultInstance());
        var response = application.exportSupplierContracts(request);
        var sent0 = channel.request("acme.suppliers.v1.SupplierQueryService/FindSuppliers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "owner_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        var sent1 = channel.request("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent1, "supplier_rating", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "created_after"),
                ProbeSupport.value(sent1, "supplier_rating", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent1, "details", "locale"));
        var sent2 = channel.request("acme.risk.v1.RiskRegisterService/WatchRiskRegisters");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "labels"),
                ProbeSupport.value(sent2, "filter", "labels"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/FindSuppliers"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierQueryService/FindSuppliers"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.suppliers.v1.SupplierRatingValidationService/NormalizeSupplierRating"), "supplier_rating", "billing_address", "country_code"),
                ProbeSupport.value(response, "supplier_contract", "billing_address", "country_code"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(response, "supplier_contract", "billing_address", "region"));
        var endpoint = new SupplierContractQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportSupplierContractsResponse> observer = new StreamObserver<>() {
            public void onNext(ExportSupplierContractsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportSupplierContracts(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new SupplierContractQueryApplicationEvents(AuditEventNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<SubscribeAuditEventUpdatesResponse>();
        listener.on(new SupplierContractQueryApplication.SuggestSupplierContractsStage0Requested(ProbeSupport.sample(SubscribeAuditEventUpdatesRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
