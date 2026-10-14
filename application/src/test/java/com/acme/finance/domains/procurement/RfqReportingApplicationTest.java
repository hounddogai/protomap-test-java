package com.acme.finance.domains.procurement;

import com.acme.assets.v1.DepreciationReportingServiceGrpc;
import com.acme.assets.v1.ListDepreciationReportsRequest;
import com.acme.assets.v1.ListDepreciationReportsResponse;
import com.acme.collections.v1.EscalateWriteOffRequest;
import com.acme.collections.v1.EscalateWriteOffResponse;
import com.acme.collections.v1.RecallWriteOffRequest;
import com.acme.collections.v1.RecallWriteOffResponse;
import com.acme.collections.v1.WriteOff;
import com.acme.collections.v1.WriteOffApprovalServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.BrowseHouseholdsRequest;
import com.acme.customers.v1.BrowseHouseholdsResponse;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.ExportCustomersRequest;
import com.acme.customers.v1.ExportCustomersResponse;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.FindJournalEntriesRequest;
import com.acme.ledger.v1.FindJournalEntriesResponse;
import com.acme.ledger.v1.JournalEntryQueryServiceGrpc;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DeleteRfqReportRequest;
import com.acme.procurement.v1.DeleteRfqReportResponse;
import com.acme.procurement.v1.ExplainRfqCalculationRequest;
import com.acme.procurement.v1.ExplainRfqCalculationResponse;
import com.acme.procurement.v1.FetchRfqReportRequest;
import com.acme.procurement.v1.FetchRfqReportResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptValidationServiceGrpc;
import com.acme.procurement.v1.ListRfqReportsRequest;
import com.acme.procurement.v1.ListRfqReportsResponse;
import com.acme.procurement.v1.NormalizeGoodsReceiptRequest;
import com.acme.procurement.v1.NormalizeGoodsReceiptResponse;
import com.acme.procurement.v1.PreviewGoodsReceiptRequest;
import com.acme.procurement.v1.PreviewGoodsReceiptResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqCalculationServiceGrpc;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScheduleRfqReportRequest;
import com.acme.procurement.v1.ScheduleRfqReportResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.acme.procurement.v1.ValidatePurchaseRequisitionRequest;
import com.acme.procurement.v1.ValidatePurchaseRequisitionResponse;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RfqReportingApplicationTest {
    @Test
    void generateRfqReportPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new RfqReportingApplicationEvents(HouseholdQueryServiceGrpc.newStub(channel), RfqCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RfqReportingApplication.GenerateRfqReportStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof RfqReportingApplication.GenerateRfqReportStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RfqReportingApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                events,
                new RfqReportingRequestMapperImpl(),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newFutureStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(GenerateRfqReportRequest.getDefaultInstance());
        var response = application.generateRfqReport(request);
        var sent0 = channel.request("acme.customers.v1.HouseholdQueryService/BrowseHouseholds");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "deadline"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq_id"),
                ProbeSupport.value(sent0, "filter", "owner_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent0, "filter", "labels"));
        var sent1 = channel.request("acme.procurement.v1.GoodsReceiptValidationService/NormalizeGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent1, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent1, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "actor", "email"),
                ProbeSupport.value(sent1, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "context", "locale"),
                ProbeSupport.value(sent1, "details", "locale"));
        var sent2 = channel.request("acme.procurement.v1.RfqCalculationService/ExplainRfqCalculation");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/NormalizeGoodsReceipt"), "goods_receipt", "audit", "created_at"),
                ProbeSupport.value(sent2, "rfq", "comments", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/NormalizeGoodsReceipt"), "goods_receipt", "due_at"),
                ProbeSupport.value(sent2, "rfq", "sla", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/NormalizeGoodsReceipt"), "goods_receipt", "audit", "created_by"),
                ProbeSupport.value(sent2, "rfq", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/BrowseHouseholds"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.HouseholdQueryService/BrowseHouseholds"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/NormalizeGoodsReceipt"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqCalculationService/ExplainRfqCalculation"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "period"),
                ProbeSupport.value(response, "period"));
    }

    @Test
    void streamRfqMetricsPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new RfqReportingApplicationEvents(HouseholdQueryServiceGrpc.newStub(channel), RfqCalculationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RfqReportingApplication.GenerateRfqReportStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof RfqReportingApplication.GenerateRfqReportStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RfqReportingApplication(
                DepreciationReportingServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                WriteOffApprovalServiceGrpc.newBlockingStub(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                events,
                new RfqReportingRequestMapperImpl(),
                GoodsReceiptValidationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptValidationServiceGrpc.newFutureStub(channel),
                RfqCalculationServiceGrpc.newBlockingStub(channel),
                repository);
        var request = ProbeSupport.sample(StreamRfqMetricsRequest.getDefaultInstance());
        var response = application.streamRfqMetrics(request);
        var sent0 = channel.request("acme.procurement.v1.GoodsReceiptValidationService/PreviewGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "filter", "region"),
                ProbeSupport.value(sent0, "goods_receipt", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(sent0, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(sent0, "details", "contact", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "name"),
                ProbeSupport.value(sent0, "details", "contact", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptValidationService/PreviewGoodsReceipt"), "goods_receipt", "audit", "created_at"),
                ProbeSupport.value(response, "rfq", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "email"),
                ProbeSupport.value(response, "rfq", "requester", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "subscriber", "preferred_channel"),
                ProbeSupport.value(response, "rfq", "requester", "preferred_channel"));
        var endpoint = new RfqReportingEndpoint(application);
        var signals = new ArrayList<String>();
        StreamObserver<StreamRfqMetricsResponse> observer = new StreamObserver<>() {
            public void onNext(StreamRfqMetricsResponse value) { signals.add("value"); }
            public void onError(Throwable failure) { throw new AssertionError(failure); }
            public void onCompleted() { signals.add("completed"); }
        };
        endpoint.streamRfqMetrics(request, observer);
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of("value", "completed"), signals);
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RfqReportingApplicationEvents(HouseholdQueryServiceGrpc.newStub(channel), RfqCalculationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<BrowseHouseholdsResponse>();
        listener.on(new RfqReportingApplication.GenerateRfqReportStage0Requested(ProbeSupport.sample(BrowseHouseholdsRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ExplainRfqCalculationResponse>();
        listener.on(new RfqReportingApplication.GenerateRfqReportStage2Requested(ProbeSupport.sample(ExplainRfqCalculationRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
