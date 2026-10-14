package com.acme.finance.domains.procurement;

import com.acme.assets.v1.Depreciation;
import com.acme.assets.v1.DepreciationParticipant;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.PauseDepreciationWorkflowRequest;
import com.acme.assets.v1.PauseDepreciationWorkflowResponse;
import com.acme.banking.v1.BankAccountServiceGrpc;
import com.acme.banking.v1.BatchCreateBankAccountsRequest;
import com.acme.banking.v1.BatchCreateBankAccountsResponse;
import com.acme.budgeting.v1.StreamVarianceChangesRequest;
import com.acme.budgeting.v1.StreamVarianceChangesResponse;
import com.acme.budgeting.v1.Variance;
import com.acme.budgeting.v1.VarianceSyncServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.contracts.v1.CommentObligationApprovalRequest;
import com.acme.contracts.v1.CommentObligationApprovalResponse;
import com.acme.contracts.v1.Obligation;
import com.acme.contracts.v1.ObligationApprovalServiceGrpc;
import com.acme.customers.v1.CalculateConsentRecordRequest;
import com.acme.customers.v1.CalculateConsentRecordResponse;
import com.acme.customers.v1.CalculateCustomerRequest;
import com.acme.customers.v1.CalculateCustomerResponse;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.ConsentRecord;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdRef;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreHouseholdRequest;
import com.acme.customers.v1.ScoreHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.lending.v1.Loan;
import com.acme.lending.v1.LoanCardFunding;
import com.acme.lending.v1.LoanWorkflowServiceGrpc;
import com.acme.lending.v1.PauseLoanWorkflowRequest;
import com.acme.lending.v1.PauseLoanWorkflowResponse;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseOrderApprovalResponse;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalRequest;
import com.acme.procurement.v1.CommentPurchaseRequisitionApprovalResponse;
import com.acme.procurement.v1.ExplainRfqDecisionRequest;
import com.acme.procurement.v1.ExplainRfqDecisionResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.GoodsReceiptApprovalServiceGrpc;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseOrderWorkflowsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderAttachment;
import com.acme.procurement.v1.PurchaseOrderLine;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseRequisitionRef;
import com.acme.procurement.v1.RecallGoodsReceiptRequest;
import com.acme.procurement.v1.RecallGoodsReceiptResponse;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqReportingServiceGrpc;
import com.acme.procurement.v1.RfqValidationServiceGrpc;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseOrderWorkflowResponse;
import com.acme.procurement.v1.StreamRfqMetricsRequest;
import com.acme.procurement.v1.StreamRfqMetricsResponse;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class PurchaseOrderWorkflowApplicationTest {
    @Test
    void advancePurchaseOrderWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var listener = new PurchaseOrderWorkflowApplicationEvents(DepreciationWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof PurchaseOrderWorkflowApplication.StartPurchaseOrderWorkflowStage0Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new PurchaseOrderWorkflowApplication(
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                BankAccountServiceGrpc.newStub(channel),
                VarianceSyncServiceGrpc.newStub(channel),
                ObligationApprovalServiceGrpc.newBlockingStub(channel),
                ConsentRecordCalculationServiceGrpc.newBlockingStub(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                events,
                LoanWorkflowServiceGrpc.newBlockingStub(channel),
                GoodsReceiptApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseOrderApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionApprovalServiceGrpc.newBlockingStub(channel),
                RfqReportingServiceGrpc.newBlockingStub(channel),
                RfqValidationServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run);
        var request = ProbeSupport.sample(AdvancePurchaseOrderWorkflowRequest.getDefaultInstance());
        var response = application.advancePurchaseOrderWorkflow(request);
        var sent0 = channel.request("acme.contracts.v1.ObligationApprovalService/CommentObligationApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "preferred_channel"),
                ProbeSupport.value(sent0, "obligation", "requester", "preferred_channel"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "obligation", "requester", "phone"));
        var sent1 = channel.request("acme.customers.v1.CustomerCalculationService/CalculateCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "billing_address", "region"),
                ProbeSupport.value(sent1, "customer", "primary_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent1, "customer", "contact", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/CommentObligationApproval"), "obligation", "customer", "email"),
                ProbeSupport.value(sent1, "customer", "relationship_manager", "email"));
        var sent2 = channel.request("acme.procurement.v1.GoodsReceiptApprovalService/RecallGoodsReceipt");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/CalculateCustomer"), "customer", "contact", "email"),
                ProbeSupport.value(sent2, "details", "approver", "email"));
        var sent3 = channel.request("acme.procurement.v1.PurchaseOrderApprovalService/CommentPurchaseOrderApproval");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent3, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "attachments", "checksum"),
                ProbeSupport.value(sent3, "purchase_order", "attachments", "checksum"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "attachments", "size_bytes"),
                ProbeSupport.value(sent3, "purchase_order", "attachments", "size_bytes"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "attachments", "content_type"),
                ProbeSupport.value(sent3, "purchase_order", "attachments", "content_type"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/CommentObligationApproval"), "outcome", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.contracts.v1.ObligationApprovalService/CommentObligationApproval"), "outcome", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerCalculationService/CalculateCustomer"), "customer", "national_id", "number"),
                ProbeSupport.value(response, "purchase_order", "counterparty", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptApprovalService/RecallGoodsReceipt"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.GoodsReceiptApprovalService/RecallGoodsReceipt"), "goods_receipt", "due_at"),
                ProbeSupport.value(response, "purchase_order", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order_id"),
                ProbeSupport.value(response, "purchase_order", "purchase_order_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_order", "metadata"),
                ProbeSupport.value(response, "purchase_order", "metadata"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new PurchaseOrderWorkflowApplicationEvents(DepreciationWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<PauseDepreciationWorkflowResponse>();
        listener.on(new PurchaseOrderWorkflowApplication.StartPurchaseOrderWorkflowStage0Requested(ProbeSupport.sample(PauseDepreciationWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
