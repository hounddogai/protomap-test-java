package com.acme.finance.domains.procurement;

import com.acme.budgeting.v1.CancelForecastWorkflowRequest;
import com.acme.budgeting.v1.CancelForecastWorkflowResponse;
import com.acme.budgeting.v1.Forecast;
import com.acme.budgeting.v1.ForecastWorkflowServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.Attestation;
import com.acme.compliance.v1.AttestationWorkflowServiceGrpc;
import com.acme.compliance.v1.CancelAttestationWorkflowRequest;
import com.acme.compliance.v1.CancelAttestationWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlApprovalServiceGrpc;
import com.acme.compliance.v1.SubmitControlRequest;
import com.acme.compliance.v1.SubmitControlResponse;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityRequest;
import com.acme.customers.v1.CheckCustomerSegmentEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationRequest;
import com.acme.customers.v1.ExplainCustomerSegmentCalculationResponse;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.RecalculateConsentRecordsRequest;
import com.acme.customers.v1.RecalculateConsentRecordsResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.devices.v1.ManagedDevice;
import com.acme.devices.v1.ManagedDeviceCalculationServiceGrpc;
import com.acme.devices.v1.SimulateManagedDeviceRequest;
import com.acme.devices.v1.SimulateManagedDeviceResponse;
import com.acme.documents.v1.ListDocumentsRequest;
import com.acme.documents.v1.ListDocumentsResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.forecasting.v1.DemandForecastQueryServiceGrpc;
import com.acme.forecasting.v1.ResolveDemandForecastsRequest;
import com.acme.forecasting.v1.ResolveDemandForecastsResponse;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccountNotificationServiceGrpc;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsRequest;
import com.acme.ledger.v1.ListLedgerAccountSubscriptionsResponse;
import com.acme.lending.v1.GenerateRepaymentReportRequest;
import com.acme.lending.v1.GenerateRepaymentReportResponse;
import com.acme.lending.v1.RepaymentReportingServiceGrpc;
import com.acme.metrics.v1.BrowseMetricSeriesesRequest;
import com.acme.metrics.v1.BrowseMetricSeriesesResponse;
import com.acme.metrics.v1.MetricSeriesQueryServiceGrpc;
import com.acme.notifications.v1.ListNotificationsRequest;
import com.acme.notifications.v1.ListNotificationsResponse;
import com.acme.notifications.v1.NotificationServiceGrpc;
import com.acme.procurement.v1.ApproveRfqRequest;
import com.acme.procurement.v1.ApproveRfqResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.CommentRfqApprovalRequest;
import com.acme.procurement.v1.CommentRfqApprovalResponse;
import com.acme.procurement.v1.DelegateRfqApprovalRequest;
import com.acme.procurement.v1.DelegateRfqApprovalResponse;
import com.acme.procurement.v1.EscalateGoodsReceiptRequest;
import com.acme.procurement.v1.EscalateGoodsReceiptResponse;
import com.acme.procurement.v1.EscalateRfqRequest;
import com.acme.procurement.v1.EscalateRfqResponse;
import com.acme.procurement.v1.GetRfqApprovalStatusRequest;
import com.acme.procurement.v1.GetRfqApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.ListPendingRfqsRequest;
import com.acme.procurement.v1.ListPendingRfqsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderCalculationServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseOrderLineTax;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.PurchaseRequisitionValidationServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionWorkflowServiceGrpc;
import com.acme.procurement.v1.QuotePurchaseOrderRequest;
import com.acme.procurement.v1.QuotePurchaseOrderResponse;
import com.acme.procurement.v1.RecallRfqRequest;
import com.acme.procurement.v1.RecallRfqResponse;
import com.acme.procurement.v1.RejectRfqRequest;
import com.acme.procurement.v1.RejectRfqResponse;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.ScorePurchaseRequisitionRequest;
import com.acme.procurement.v1.ScorePurchaseRequisitionResponse;
import com.acme.procurement.v1.SubmitRfqRequest;
import com.acme.procurement.v1.SubmitRfqResponse;
import com.acme.suppliers.v1.SubmitSupplierRiskRequest;
import com.acme.suppliers.v1.SubmitSupplierRiskResponse;
import com.acme.suppliers.v1.SupplierRisk;
import com.acme.suppliers.v1.SupplierRiskApprovalServiceGrpc;
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
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class RfqApprovalApplicationTest {
    @Test
    void rejectRfqPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new RfqApprovalApplicationEvents(ForecastWorkflowServiceGrpc.newStub(channel), LedgerAccountNotificationServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof RfqApprovalApplication.ApproveRfqStage0Requested command) {
                listener.on(command);
            }
            else if (event instanceof RfqApprovalApplication.RejectRfqStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new RfqApprovalApplication(
                ForecastWorkflowServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                AttestationWorkflowServiceGrpc.newBlockingStub(channel),
                ControlApprovalServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                ManagedDeviceCalculationServiceGrpc.newBlockingStub(channel),
                events,
                DemandForecastQueryServiceGrpc.newBlockingStub(channel),
                RepaymentReportingServiceGrpc.newBlockingStub(channel),
                new RfqApprovalRequestMapperImpl(),
                MetricSeriesQueryServiceGrpc.newBlockingStub(channel),
                NotificationServiceGrpc.newBlockingStub(channel),
                GoodsReceiptApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseOrderApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseOrderCalculationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionValidationServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionWorkflowServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SupplierRiskApprovalServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectRfqRequest.getDefaultInstance());
        var response = application.rejectRfq(request);
        var sent0 = channel.request("acme.notifications.v1.NotificationService/ListNotifications");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "sla", "due_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.compliance.v1.AttestationWorkflowService/CancelAttestationWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "comments", "created_at"),
                ProbeSupport.value(sent1, "attestation", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "comments", "author_email"),
                ProbeSupport.value(sent1, "details", "notify", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "requester", "preferred_channel"),
                ProbeSupport.value(sent1, "details", "notify", "preferred_channel"));
        var sent2 = channel.request("acme.ledger.v1.LedgerAccountNotificationService/ListLedgerAccountSubscriptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationWorkflowService/CancelAttestationWorkflow"), "attestation", "billing_address", "region"),
                ProbeSupport.value(sent2, "filter", "region"));
        var sent3 = channel.request("acme.budgeting.v1.ForecastWorkflowService/CancelForecastWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationWorkflowService/CancelAttestationWorkflow"), "attestation", "billing_address", "region"),
                ProbeSupport.value(sent3, "forecast", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.compliance.v1.AttestationWorkflowService/CancelAttestationWorkflow"), "attestation", "billing_address", "recipient"),
                ProbeSupport.value(sent3, "forecast", "billing_address", "recipient"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "requester", "name"),
                ProbeSupport.value(sent3, "details", "notify", "name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "audit", "created_at"),
                ProbeSupport.value(sent3, "forecast", "audit", "created_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.notifications.v1.NotificationService/ListNotifications"), "result", "correlation_id"),
                ProbeSupport.value(response, "result", "correlation_id"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountNotificationService/ListLedgerAccountSubscriptions"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountNotificationService/ListLedgerAccountSubscriptions"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastWorkflowService/CancelForecastWorkflow"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.ForecastWorkflowService/CancelForecastWorkflow"), "outcome", "message"),
                ProbeSupport.value(response, "outcome", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq_id"),
                ProbeSupport.value(response, "rfq", "rfq_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "rfq", "custom_fields"),
                ProbeSupport.value(response, "rfq", "custom_fields"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new RfqApprovalApplicationEvents(ForecastWorkflowServiceGrpc.newStub(channel), LedgerAccountNotificationServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CancelForecastWorkflowResponse>();
        listener.on(new RfqApprovalApplication.ApproveRfqStage0Requested(ProbeSupport.sample(CancelForecastWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        var completion1 = new CompletableFuture<ListLedgerAccountSubscriptionsResponse>();
        listener.on(new RfqApprovalApplication.RejectRfqStage2Requested(ProbeSupport.sample(ListLedgerAccountSubscriptionsRequest.getDefaultInstance()), completion1, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion1.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
        org.junit.jupiter.api.Assertions.assertNotNull(completion1.getNow(null));
    }
}
