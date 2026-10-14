package com.acme.finance.domains.invoicing;

import com.acme.assets.v1.Disposal;
import com.acme.assets.v1.DisposalComment;
import com.acme.assets.v1.DisposalSyncServiceGrpc;
import com.acme.assets.v1.StreamDisposalChangesRequest;
import com.acme.assets.v1.StreamDisposalChangesResponse;
import com.acme.benefits.v1.BenefitPlanQueryServiceGrpc;
import com.acme.benefits.v1.ResolveBenefitPlansRequest;
import com.acme.benefits.v1.ResolveBenefitPlansResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.compliance.v1.CancelControlWorkflowRequest;
import com.acme.compliance.v1.CancelControlWorkflowResponse;
import com.acme.compliance.v1.Control;
import com.acme.compliance.v1.ControlWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentRouteAction;
import com.acme.customers.v1.EstimateCustomerSegmentRequest;
import com.acme.customers.v1.EstimateCustomerSegmentResponse;
import com.acme.customers.v1.ExplainHouseholdDecisionRequest;
import com.acme.customers.v1.ExplainHouseholdDecisionResponse;
import com.acme.customers.v1.FindCustomersRequest;
import com.acme.customers.v1.FindCustomersResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.featureflags.v1.FeatureFlag;
import com.acme.featureflags.v1.FeatureFlagValidationServiceGrpc;
import com.acme.featureflags.v1.ValidateFeatureFlagRequest;
import com.acme.featureflags.v1.ValidateFeatureFlagResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.platform.StreamCollector;
import com.acme.finance.testing.ProbeSupport;
import com.acme.identity.v1.UserRef;
import com.acme.invoicing.v1.ActivateEInvoiceRequest;
import com.acme.invoicing.v1.ActivateEInvoiceResponse;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesRequest;
import com.acme.invoicing.v1.BatchCreateInvoiceBatchesResponse;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceBatchEligibilityResponse;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsRequest;
import com.acme.invoicing.v1.CompareInvoiceTemplateOptionsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.ExpireEInvoiceRequest;
import com.acme.invoicing.v1.ExpireEInvoiceResponse;
import com.acme.invoicing.v1.GetEInvoiceLifecycleRequest;
import com.acme.invoicing.v1.GetEInvoiceLifecycleResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchRef;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.QuoteInvoiceTemplateRequest;
import com.acme.invoicing.v1.QuoteInvoiceTemplateResponse;
import com.acme.invoicing.v1.RecalculateEInvoicesRequest;
import com.acme.invoicing.v1.RecalculateEInvoicesResponse;
import com.acme.invoicing.v1.ReinstateEInvoiceRequest;
import com.acme.invoicing.v1.ReinstateEInvoiceResponse;
import com.acme.invoicing.v1.RenewEInvoiceRequest;
import com.acme.invoicing.v1.RenewEInvoiceResponse;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryRequest;
import com.acme.invoicing.v1.ScheduleEInvoiceExpiryResponse;
import com.acme.invoicing.v1.SuspendEInvoiceRequest;
import com.acme.invoicing.v1.SuspendEInvoiceResponse;
import com.acme.invoicing.v1.TerminateEInvoiceRequest;
import com.acme.invoicing.v1.TerminateEInvoiceResponse;
import com.acme.invoicing.v1.WatchInvoiceBatchesRequest;
import com.acme.invoicing.v1.WatchInvoiceBatchesResponse;
import com.acme.risk.v1.BatchCreateRiskRegistersRequest;
import com.acme.risk.v1.BatchCreateRiskRegistersResponse;
import com.acme.risk.v1.ExposureApprovalServiceGrpc;
import com.acme.risk.v1.ListPendingExposuresRequest;
import com.acme.risk.v1.ListPendingExposuresResponse;
import com.acme.risk.v1.RiskRegisterServiceGrpc;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

class EInvoiceLifecycleApplicationTest {
    @Test
    void scheduleEInvoiceExpiryPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var listener = new EInvoiceLifecycleApplicationEvents(ControlWorkflowServiceGrpc.newStub(channel));
        org.springframework.context.ApplicationEventPublisher events = event -> {
            if (event instanceof EInvoiceLifecycleApplication.ReinstateEInvoiceStage2Requested command) {
                listener.on(command);
            }
            else { throw new IllegalArgumentException(event.getClass().getName()); }
        };
        var application = new EInvoiceLifecycleApplication(
                DisposalSyncServiceGrpc.newStub(channel),
                BenefitPlanQueryServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerQueryServiceGrpc.newBlockingStub(channel),
                CustomerSegmentCalculationServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                events,
                FeatureFlagValidationServiceGrpc.newFutureStub(channel),
                InvoiceBatchServiceGrpc.newStub(channel),
                InvoiceBatchServiceGrpc.newBlockingStub(channel),
                InvoiceBatchValidationServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channel),
                new EInvoiceLifecycleRequestMapperImpl(),
                repository,
                ExposureApprovalServiceGrpc.newBlockingStub(channel),
                RiskRegisterServiceGrpc.newStub(channel));
        var request = ProbeSupport.sample(ScheduleEInvoiceExpiryRequest.getDefaultInstance());
        var response = application.scheduleEInvoiceExpiry(request);
        var sent0 = channel.request("acme.customers.v1.CustomerQueryService/FindCustomers");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent0, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_at"),
                ProbeSupport.value(sent0, "filter", "created_after"));
        var sent1 = channel.request("acme.risk.v1.ExposureApprovalService/ListPendingExposures");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent1, "filter", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "audit", "updated_at"),
                ProbeSupport.value(sent1, "filter", "created_after"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "parameters"),
                ProbeSupport.value(sent1, "filter", "labels"));
        var sent2 = channel.request("acme.invoicing.v1.InvoiceBatchValidationService/CheckInvoiceBatchEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address", "region"),
                ProbeSupport.value(sent2, "invoice_batch", "billing_address", "region"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "details", "notify", "email"),
                ProbeSupport.value(sent2, "details", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerQueryService/FindCustomers"), "result", "status"),
                ProbeSupport.value(response, "result", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.risk.v1.ExposureApprovalService/ListPendingExposures"), "result", "message"),
                ProbeSupport.value(response, "result", "message"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.invoicing.v1.InvoiceBatchValidationService/CheckInvoiceBatchEligibility"), "result", "status"),
                ProbeSupport.value(response, "outcome", "status"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice_id"),
                ProbeSupport.value(response, "e_invoice", "e_invoice_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "e_invoice", "billing_address"),
                ProbeSupport.value(response, "e_invoice", "billing_address"));
    }

    @Test
    void eventListenersReturnBeforeTheirCallsComplete() {
        var channel = new ProbeSupport.RecordingChannel();
        var listener = new EInvoiceLifecycleApplicationEvents(ControlWorkflowServiceGrpc.newStub(channel));
        channel.hold();
        var completion0 = new CompletableFuture<CancelControlWorkflowResponse>();
        listener.on(new EInvoiceLifecycleApplication.ReinstateEInvoiceStage2Requested(ProbeSupport.sample(CancelControlWorkflowRequest.getDefaultInstance()), completion0, io.grpc.Context.current()));
        org.junit.jupiter.api.Assertions.assertFalse(completion0.isDone());
        channel.release();
        org.junit.jupiter.api.Assertions.assertNotNull(completion0.getNow(null));
    }
}
