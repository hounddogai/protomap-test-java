package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.AppointmentQueryServiceGrpc;
import com.acme.appointments.v1.ResolveAppointmentsRequest;
import com.acme.appointments.v1.ResolveAppointmentsResponse;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailBankSettlement;
import com.acme.audit.v1.AuditTrailHistoryServiceGrpc;
import com.acme.audit.v1.CompareAuditTrailVersionsRequest;
import com.acme.audit.v1.CompareAuditTrailVersionsResponse;
import com.acme.collections.v1.CollectionCase;
import com.acme.collections.v1.CollectionCaseWorkflowServiceGrpc;
import com.acme.collections.v1.PaymentPromiseRef;
import com.acme.collections.v1.RetryCollectionCaseWorkflowRequest;
import com.acme.collections.v1.RetryCollectionCaseWorkflowResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.PreviewCustomerRequest;
import com.acme.customers.v1.PreviewCustomerResponse;
import com.acme.customers.v1.QuoteCustomerSegmentRequest;
import com.acme.customers.v1.QuoteCustomerSegmentResponse;
import com.acme.customers.v1.RecalculateCustomersRequest;
import com.acme.customers.v1.RecalculateCustomersResponse;
import com.acme.documents.v1.BatchGetDocumentsRequest;
import com.acme.documents.v1.BatchGetDocumentsResponse;
import com.acme.documents.v1.DocumentServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.ledger.v1.LedgerAccount;
import com.acme.ledger.v1.LedgerAccountValidationServiceGrpc;
import com.acme.ledger.v1.LintLedgerAccountsRequest;
import com.acme.ledger.v1.LintLedgerAccountsResponse;
import com.acme.ledger.v1.NormalizeLedgerAccountRequest;
import com.acme.ledger.v1.NormalizeLedgerAccountResponse;
import com.acme.notifications.v1.Notification;
import com.acme.notifications.v1.NotificationTemplateRef;
import com.acme.notifications.v1.NotificationValidationServiceGrpc;
import com.acme.notifications.v1.PreviewNotificationRequest;
import com.acme.notifications.v1.PreviewNotificationResponse;
import com.acme.procurement.v1.ApprovePurchaseRequisitionRequest;
import com.acme.procurement.v1.ApprovePurchaseRequisitionResponse;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.CountRfqTotalsRequest;
import com.acme.procurement.v1.CountRfqTotalsResponse;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.DelegatePurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.EscalatePurchaseRequisitionRequest;
import com.acme.procurement.v1.EscalatePurchaseRequisitionResponse;
import com.acme.procurement.v1.GenerateRfqReportRequest;
import com.acme.procurement.v1.GenerateRfqReportResponse;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusRequest;
import com.acme.procurement.v1.GetPurchaseRequisitionApprovalStatusResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsRequest;
import com.acme.procurement.v1.ListPendingPurchaseRequisitionsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.RecallPurchaseRequisitionRequest;
import com.acme.procurement.v1.RecallPurchaseRequisitionResponse;
import com.acme.procurement.v1.RejectPurchaseRequisitionRequest;
import com.acme.procurement.v1.RejectPurchaseRequisitionResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqComment;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SubmitPurchaseRequisitionRequest;
import com.acme.procurement.v1.SubmitPurchaseRequisitionResponse;
import com.acme.sellers.v1.Seller;
import com.acme.sellers.v1.SellerServiceGrpc;
import com.acme.sellers.v1.UpdateSellerRequest;
import com.acme.sellers.v1.UpdateSellerResponse;
import com.google.protobuf.Any;
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

class PurchaseRequisitionApprovalApplicationTest {
    @Test
    void rejectPurchaseRequisitionPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionApprovalApplication(
                AppointmentQueryServiceGrpc.newBlockingStub(channel),
                AuditTrailHistoryServiceGrpc.newBlockingStub(channel),
                CollectionCaseWorkflowServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newFutureStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                DocumentServiceGrpc.newBlockingStub(channel),
                LedgerAccountValidationServiceGrpc.newBlockingStub(channel),
                NotificationValidationServiceGrpc.newBlockingStub(channel),
                RfqLifecycleServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                SellerServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(RejectPurchaseRequisitionRequest.getDefaultInstance());
        var response = application.rejectPurchaseRequisition(request);
        var sent0 = channel.request("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "created_by"),
                ProbeSupport.value(sent0, "ledger_account", "audit", "created_by"));
        var sent1 = channel.request("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts"), "ledger_account", "holder", "display_name"),
                ProbeSupport.value(sent1, "collection_case", "customer", "display_name"));
        var sent2 = channel.request("acme.procurement.v1.RfqReportingService/GenerateRfqReport");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "annotations"),
                ProbeSupport.value(sent2, "annotations"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "reason"),
                ProbeSupport.value(sent2, "reason"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts"), "ledger_account", "labels"),
                ProbeSupport.value(sent2, "details", "filters"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.ledger.v1.LedgerAccountValidationService/LintLedgerAccounts"), "result", "reference_ids"),
                ProbeSupport.value(response, "result", "reference_ids"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.collections.v1.CollectionCaseWorkflowService/RetryCollectionCaseWorkflow"), "collection_case", "customer", "display_name"),
                ProbeSupport.value(response, "purchase_requisition", "counterparty", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqReportingService/GenerateRfqReport"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition_id"),
                ProbeSupport.value(response, "purchase_requisition", "purchase_requisition_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address"),
                ProbeSupport.value(response, "purchase_requisition", "billing_address"));
    }
}
