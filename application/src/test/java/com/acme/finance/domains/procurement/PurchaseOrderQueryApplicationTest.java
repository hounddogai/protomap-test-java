package com.acme.finance.domains.procurement;

import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.ComplianceCase;
import com.acme.compliance.v1.ComplianceCaseCalculationServiceGrpc;
import com.acme.compliance.v1.ComplianceCaseSla;
import com.acme.compliance.v1.EstimateComplianceCaseRequest;
import com.acme.compliance.v1.EstimateComplianceCaseResponse;
import com.acme.compliance.v1.QuoteComplianceCaseRequest;
import com.acme.compliance.v1.QuoteComplianceCaseResponse;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.LedgerAccountQueryServiceGrpc;
import com.acme.ledger.v1.ResolveLedgerAccountsRequest;
import com.acme.ledger.v1.ResolveLedgerAccountsResponse;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesRequest;
import com.acme.ledger.v1.UnsubscribeLedgerAccountUpdatesResponse;
import com.acme.procurement.v1.BrowsePurchaseOrdersRequest;
import com.acme.procurement.v1.BrowsePurchaseOrdersResponse;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityRequest;
import com.acme.procurement.v1.CheckPurchaseRequisitionEligibilityResponse;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsRequest;
import com.acme.procurement.v1.ComparePurchaseRequisitionOptionsResponse;
import com.acme.procurement.v1.CountPurchaseOrdersRequest;
import com.acme.procurement.v1.CountPurchaseOrdersResponse;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionRequest;
import com.acme.procurement.v1.ExplainPurchaseRequisitionDecisionResponse;
import com.acme.procurement.v1.ExportPurchaseOrdersRequest;
import com.acme.procurement.v1.ExportPurchaseOrdersResponse;
import com.acme.procurement.v1.FindPurchaseOrdersRequest;
import com.acme.procurement.v1.FindPurchaseOrdersResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.LookupPurchaseOrderRequest;
import com.acme.procurement.v1.LookupPurchaseOrderResponse;
import com.acme.procurement.v1.PeekPurchaseOrderRequest;
import com.acme.procurement.v1.PeekPurchaseOrderResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionBankSettlement;
import com.acme.procurement.v1.PurchaseRequisitionCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.ResolvePurchaseOrdersRequest;
import com.acme.procurement.v1.ResolvePurchaseOrdersResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.SimulateRfqRequest;
import com.acme.procurement.v1.SimulateRfqResponse;
import com.acme.procurement.v1.SuggestPurchaseOrdersRequest;
import com.acme.procurement.v1.SuggestPurchaseOrdersResponse;
import com.acme.procurement.v1.SummarizePurchaseOrdersRequest;
import com.acme.procurement.v1.SummarizePurchaseOrdersResponse;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PurchaseOrderQueryApplicationTest {
    @Test
    void exportPurchaseOrdersPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new PurchaseOrderQueryApplicationEvents(ComplianceCaseCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PurchaseOrderQueryApplication.CountPurchaseOrdersStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PurchaseOrderQueryApplication(
                ChannelRegistry.fixed(channel),
                events,
                LedgerAccountNotificationServiceGrpc.newBlockingStub(channel),
                LedgerAccountQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(ExportPurchaseOrdersRequest.getDefaultInstance());
        var response = application.exportPurchaseOrders(request);
        var sent0 = channel.request("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "purchase_requisition", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent0, "details", "locale"));
        var sent1 = channel.request("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "audit", "created_at"),
                ProbeSupport.value(sent1, "purchase_requisition", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "billing_address", "region"),
                ProbeSupport.value(sent1, "details", "destination", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "counterparty", "number"),
                ProbeSupport.value(sent1, "purchase_requisition", "counterparty", "number"));
        var sent2 = channel.request("acme.procurement.v1.PurchaseRequisitionValidationService/CheckPurchaseRequisitionEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "bank", "account_holder"),
                ProbeSupport.value(sent2, "purchase_requisition", "bank", "account_holder"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "purchase_requisition", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "audit", "updated_by"),
                ProbeSupport.value(sent2, "purchase_requisition", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "bank", "bic"),
                ProbeSupport.value(sent2, "purchase_requisition", "bank", "bic"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "billing_address", "region"),
                ProbeSupport.value(response, "purchase_order", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/ExplainPurchaseRequisitionDecision"), "purchase_requisition", "billing_address", "postal_code"),
                ProbeSupport.value(response, "purchase_order", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionCalculationService/ComparePurchaseRequisitionOptions"), "purchase_requisition", "billing_address", "locality"),
                ProbeSupport.value(response, "purchase_order", "billing_address", "locality"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.PurchaseRequisitionValidationService/CheckPurchaseRequisitionEligibility"), "purchase_requisition", "total"),
                ProbeSupport.value(response, "purchase_order", "total"));
        var endpoint = new PurchaseOrderQueryEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<ExportPurchaseOrdersResponse> observer = new StreamObserver<>() {
            public void onNext(ExportPurchaseOrdersResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.exportPurchaseOrders(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PurchaseOrderQueryApplicationEvents(ComplianceCaseCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<QuoteComplianceCaseResponse>();
        listener.on(new PurchaseOrderQueryApplication.CountPurchaseOrdersStage0Requested(ProbeSupport.sample(QuoteComplianceCaseRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
