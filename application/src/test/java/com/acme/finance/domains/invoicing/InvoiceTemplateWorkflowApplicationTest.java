package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditEvent;
import com.acme.audit.v1.AuditEventMeasurement;
import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.AuditEventValidationServiceGrpc;
import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailCardSettlement;
import com.acme.audit.v1.AuditTrailRef;
import com.acme.audit.v1.AuditTrailValidationServiceGrpc;
import com.acme.audit.v1.CloneAuditEventRequest;
import com.acme.audit.v1.CloneAuditEventResponse;
import com.acme.audit.v1.NormalizeAuditEventRequest;
import com.acme.audit.v1.NormalizeAuditEventResponse;
import com.acme.audit.v1.PreviewAuditTrailRequest;
import com.acme.audit.v1.PreviewAuditTrailResponse;
import com.acme.audit.v1.SearchAuditEventsRequest;
import com.acme.audit.v1.SearchAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CheckHouseholdEligibilityRequest;
import com.acme.customers.v1.CheckHouseholdEligibilityResponse;
import com.acme.customers.v1.CloneCustomerRequest;
import com.acme.customers.v1.CloneCustomerResponse;
import com.acme.customers.v1.Customer;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerPassport;
import com.acme.customers.v1.CustomerRef;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.FindHouseholdsRequest;
import com.acme.customers.v1.FindHouseholdsResponse;
import com.acme.customers.v1.GetCustomerRatesRequest;
import com.acme.customers.v1.GetCustomerRatesResponse;
import com.acme.customers.v1.Household;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.LintCustomerSegmentsRequest;
import com.acme.customers.v1.LintCustomerSegmentsResponse;
import com.acme.finance.platform.BeanCopies;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.finance.testing.ProbeSupport;
import com.acme.insurance.v1.CountPremiumTotalsRequest;
import com.acme.insurance.v1.CountPremiumTotalsResponse;
import com.acme.insurance.v1.PremiumReportingServiceGrpc;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.AdvanceInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesRequest;
import com.acme.invoicing.v1.BrowseInvoiceTemplatesResponse;
import com.acme.invoicing.v1.CountEInvoiceTotalsRequest;
import com.acme.invoicing.v1.CountEInvoiceTotalsResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.ExportInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ExportInvoiceTemplatesResponse;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.InspectInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsRequest;
import com.acme.invoicing.v1.ListInvoiceTemplateWorkflowsResponse;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.PauseInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesRequest;
import com.acme.invoicing.v1.ResolveInvoiceTemplatesResponse;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.ResumeInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.RetryInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.SignalInvoiceTemplateWorkflowResponse;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowRequest;
import com.acme.invoicing.v1.StartInvoiceTemplateWorkflowResponse;
import com.acme.venues.v1.ValidateVenueRequest;
import com.acme.venues.v1.ValidateVenueResponse;
import com.acme.venues.v1.Venue;
import com.acme.venues.v1.VenueValidationServiceGrpc;
import com.google.protobuf.Any;
import java.util.ArrayList;
import java.util.List;
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

class InvoiceTemplateWorkflowApplicationTest {
    @Test
    void startInvoiceTemplateWorkflowPreservesPlannedFields() {
        var channel = new ProbeSupport.RecordingChannel();
        var repository = new InvoicingRepository() {
            public void recordAction(String tenant, String kind, String id, String action) {}
            public void recordDetail(String tenant, String operation, String detail) {}
            public void saveEInvoice(String tenant, EInvoice value) {}
            public void saveInvoiceBatch(String tenant, InvoiceBatch value) {}
            public void saveInvoiceTemplate(String tenant, InvoiceTemplate value) {}
        };
        var application = new InvoiceTemplateWorkflowApplication(
                AuditEventServiceGrpc.newBlockingStub(channel),
                AuditTrailValidationServiceGrpc.newBlockingStub(channel),
                ChannelRegistry.fixed(channel),
                CustomerCalculationServiceGrpc.newBlockingStub(channel),
                CustomerSegmentValidationServiceGrpc.newBlockingStub(channel),
                CustomerServiceGrpc.newFutureStub(channel),
                HouseholdQueryServiceGrpc.newBlockingStub(channel),
                HouseholdValidationServiceGrpc.newBlockingStub(channel),
                PremiumReportingServiceGrpc.newBlockingStub(channel),
                EInvoiceReportingServiceGrpc.newBlockingStub(channel),
                InvoiceTemplateQueryServiceGrpc.newBlockingStub(channel),
                repository,
                Runnable::run,
                VenueValidationServiceGrpc.newBlockingStub(channel));
        var request = ProbeSupport.sample(StartInvoiceTemplateWorkflowRequest.getDefaultInstance());
        var response = application.startInvoiceTemplateWorkflow(request);
        var sent0 = channel.request("acme.customers.v1.CustomerService/CloneCustomer");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent0, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "counterparty", "email"),
                ProbeSupport.value(sent0, "customer", "contact", "email"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "number"),
                ProbeSupport.value(sent0, "customer", "passport", "number"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "audit", "updated_at"),
                ProbeSupport.value(sent0, "customer", "audit", "updated_at"));
        var sent1 = channel.request("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent1, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "card", "network"),
                ProbeSupport.value(sent1, "audit_trail", "card", "network"));
        var sent2 = channel.request("acme.customers.v1.HouseholdValidationService/CheckHouseholdEligibility");
        ProbeSupport.assertValue(ProbeSupport.value(request, "context"),
                ProbeSupport.value(sent2, "context"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "lines", "attributes"),
                ProbeSupport.value(sent2, "household", "children", "children", "children", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail"), "audit_trail", "billing_address", "postal_code"),
                ProbeSupport.value(sent2, "household", "children", "children", "location", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/CloneCustomer"), "customer", "primary_address", "lines"),
                ProbeSupport.value(response, "invoice_template", "billing_address", "lines"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.customers.v1.CustomerService/CloneCustomer"), "customer", "attributes"),
                ProbeSupport.value(response, "invoice_template", "lines", "attributes"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail"), "audit_trail", "billing_address", "postal_code"),
                ProbeSupport.value(response, "invoice_template", "billing_address", "postal_code"));
        ProbeSupport.assertValue(ProbeSupport.value(channel.response("acme.audit.v1.AuditTrailValidationService/PreviewAuditTrail"), "audit_trail", "due_at"),
                ProbeSupport.value(response, "invoice_template", "due_at"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template_id"),
                ProbeSupport.value(response, "invoice_template", "invoice_template_id"));
        ProbeSupport.assertValue(ProbeSupport.value(request, "invoice_template", "metadata"),
                ProbeSupport.value(response, "invoice_template", "metadata"));
    }
}
