package com.acme.finance.domains.procurement;

import com.acme.appointments.v1.Appointment;
import com.acme.appointments.v1.AppointmentParticipant;
import com.acme.appointments.v1.AppointmentServiceGrpc;
import com.acme.appointments.v1.CreateAppointmentRequest;
import com.acme.appointments.v1.CreateAppointmentResponse;
import com.acme.assets.v1.ApproveAssetTransferRequest;
import com.acme.assets.v1.ApproveAssetTransferResponse;
import com.acme.assets.v1.AssetTransferApprovalServiceGrpc;
import com.acme.assets.v1.DepreciationWorkflowServiceGrpc;
import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.ListDepreciationWorkflowsRequest;
import com.acme.assets.v1.ListDepreciationWorkflowsResponse;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.banking.v1.BankAccount;
import com.acme.banking.v1.BankAccountWorkflowServiceGrpc;
import com.acme.banking.v1.CancelBankAccountWorkflowRequest;
import com.acme.banking.v1.CancelBankAccountWorkflowResponse;
import com.acme.budgeting.v1.BudgetWorkflowServiceGrpc;
import com.acme.budgeting.v1.ImportVariancesRequest;
import com.acme.budgeting.v1.ImportVariancesResponse;
import com.acme.budgeting.v1.ListBudgetWorkflowsRequest;
import com.acme.budgeting.v1.ListBudgetWorkflowsResponse;
import com.acme.budgeting.v1.VarianceImportServiceGrpc;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CompareCustomerSegmentOptionsRequest;
import com.acme.customers.v1.CompareCustomerSegmentOptionsResponse;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.ScoreCustomerRequest;
import com.acme.customers.v1.ScoreCustomerResponse;
import com.acme.customers.v1.ValidateHouseholdRequest;
import com.acme.customers.v1.ValidateHouseholdResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcPort;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.AdvancePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.CancelPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.GoodsReceipt;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.InspectPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.ListPendingPurchaseOrdersRequest;
import com.acme.procurement.v1.ListPendingPurchaseOrdersResponse;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsRequest;
import com.acme.procurement.v1.ListPurchaseRequisitionWorkflowsResponse;
import com.acme.procurement.v1.PurchaseOrder;
import com.acme.procurement.v1.PurchaseOrderApprovalServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderQueryServiceGrpc;
import com.acme.procurement.v1.PurchaseOrderRef;
import com.acme.procurement.v1.PurchaseRequisition;
import com.acme.procurement.v1.PurchaseRequisitionLine;
import com.acme.procurement.v1.PurchaseRequisitionServiceGrpc;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.ResumePurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.RetryPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.Rfq;
import com.acme.procurement.v1.RfqLifecycleServiceGrpc;
import com.acme.procurement.v1.RfqSla;
import com.acme.procurement.v1.ScheduleRfqExpiryRequest;
import com.acme.procurement.v1.ScheduleRfqExpiryResponse;
import com.acme.procurement.v1.SearchPurchaseRequisitionsRequest;
import com.acme.procurement.v1.SearchPurchaseRequisitionsResponse;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.SignalPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowRequest;
import com.acme.procurement.v1.StartPurchaseRequisitionWorkflowResponse;
import com.acme.procurement.v1.SuggestPurchaseOrdersRequest;
import com.acme.procurement.v1.SuggestPurchaseOrdersResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueComponent;
import com.acme.venues.v1.VenueValidationServiceGrpc;
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
import org.springframework.stereotype.Service;

class PurchaseRequisitionWorkflowApplicationTest {
    @Test
    void startPurchaseRequisitionWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new ProcurementRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveGoodsReceipt(String tenant, GoodsReceipt value) {}
            public void savePurchaseOrder(String tenant, PurchaseOrder value) {}
            public void savePurchaseRequisition(String tenant, PurchaseRequisition value) {}
            public void saveRfq(String tenant, Rfq value) {}
        };
        var application = new PurchaseRequisitionWorkflowApplication(
                AssetTransferApprovalServiceGrpc.newBlockingStub(channel),
                DepreciationWorkflowServiceGrpc.newBlockingStub(channel),
                DisposalSyncServiceGrpc.newStub(channel),
                BankAccountWorkflowServiceGrpc.newBlockingStub(channel),
                BudgetWorkflowServiceGrpc.newBlockingStub(channel),
                VarianceImportServiceGrpc.newStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                CustomerValidationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                new PurchaseRequisitionWorkflowRequestMapperImpl(),
                PurchaseOrderApprovalServiceGrpc.newBlockingStub(channel),
                PurchaseOrderQueryServiceGrpc.newBlockingStub(channel),
                PurchaseRequisitionServiceGrpc.newBlockingStub(channel),
                RfqLifecycleServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartPurchaseRequisitionWorkflowRequest.getDefaultInstance());
        var response = application.startPurchaseRequisitionWorkflow(request);
        var sent0 = channel.request("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "phone"),
                ProbeSupport.value(sent0, "rfq", "requester", "phone"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "etag"),
                ProbeSupport.value(sent0, "rfq", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "updated_by"),
                ProbeSupport.value(sent0, "rfq", "audit", "updated_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "due_at"),
                ProbeSupport.value(sent0, "rfq", "sla", "due_at"));
        var sent1 = channel.request("acme.budgeting.v1.BudgetWorkflowService/ListBudgetWorkflows");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry"), "rfq", "audit", "created_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry"), "rfq", "custom_fields"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry"), "rfq", "customer", "display_name"),
                ProbeSupport.value(sent2, "customer_segment", "owner", "display_name"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "audit", "etag"),
                ProbeSupport.value(sent2, "customer_segment", "audit", "etag"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry"), "outcome", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.procurement.v1.RfqLifecycleService/ScheduleRfqExpiry"), "outcome", "effective_at"),
                ProbeSupport.value(response, "outcome", "effective_at"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.budgeting.v1.BudgetWorkflowService/ListBudgetWorkflows"), "result", "details"),
                ProbeSupport.value(response, "result", "details"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions"), "breakdown", "total"),
                ProbeSupport.value(response, "purchase_requisition", "total"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerSegmentCalculationService/CompareCustomerSegmentOptions"), "customer_segment", "audit", "created_by"),
                ProbeSupport.value(response, "purchase_requisition", "audit", "created_by"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition_id"),
                ProbeSupport.value(response, "purchase_requisition", "purchase_requisition_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "purchase_requisition", "number"),
                ProbeSupport.value(response, "purchase_requisition", "counterparty", "number"));
    }
}
