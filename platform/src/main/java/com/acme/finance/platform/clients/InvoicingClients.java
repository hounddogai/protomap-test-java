package com.acme.finance.platform.clients;

import com.acme.finance.platform.ChannelRegistry;
import com.acme.invoicing.v1.EInvoiceApprovalServiceGrpc;
import com.acme.invoicing.v1.EInvoiceCalculationServiceGrpc;
import com.acme.invoicing.v1.EInvoiceLifecycleServiceGrpc;
import com.acme.invoicing.v1.EInvoiceReportingServiceGrpc;
import com.acme.invoicing.v1.EInvoiceValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchServiceGrpc;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateApprovalServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateLifecycleServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateQueryServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateWorkflowServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the invoicing contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class InvoicingClients {
    @Bean
    public EInvoiceApprovalServiceGrpc.EInvoiceApprovalServiceBlockingStub invoicingEInvoiceApprovalServiceBlocking(ChannelRegistry channels) {
        return EInvoiceApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.EInvoiceApprovalService"));
    }

    @Bean
    public EInvoiceCalculationServiceGrpc.EInvoiceCalculationServiceBlockingStub invoicingEInvoiceCalculationServiceBlocking(ChannelRegistry channels) {
        return EInvoiceCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.EInvoiceCalculationService"));
    }

    @Bean
    public EInvoiceLifecycleServiceGrpc.EInvoiceLifecycleServiceBlockingStub invoicingEInvoiceLifecycleServiceBlocking(ChannelRegistry channels) {
        return EInvoiceLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.EInvoiceLifecycleService"));
    }

    @Bean
    public EInvoiceReportingServiceGrpc.EInvoiceReportingServiceBlockingStub invoicingEInvoiceReportingServiceBlocking(ChannelRegistry channels) {
        return EInvoiceReportingServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.EInvoiceReportingService"));
    }

    @Bean
    public EInvoiceReportingServiceGrpc.EInvoiceReportingServiceFutureStub invoicingEInvoiceReportingServiceFuture(ChannelRegistry channels) {
        return EInvoiceReportingServiceGrpc.newFutureStub(channels.lookup("acme.invoicing.v1.EInvoiceReportingService"));
    }

    @Bean
    public EInvoiceValidationServiceGrpc.EInvoiceValidationServiceStub invoicingEInvoiceValidationServiceAsync(ChannelRegistry channels) {
        return EInvoiceValidationServiceGrpc.newStub(channels.lookup("acme.invoicing.v1.EInvoiceValidationService"));
    }

    @Bean
    public EInvoiceValidationServiceGrpc.EInvoiceValidationServiceBlockingStub invoicingEInvoiceValidationServiceBlocking(ChannelRegistry channels) {
        return EInvoiceValidationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.EInvoiceValidationService"));
    }

    @Bean
    public InvoiceBatchApprovalServiceGrpc.InvoiceBatchApprovalServiceBlockingStub invoicingInvoiceBatchApprovalServiceBlocking(ChannelRegistry channels) {
        return InvoiceBatchApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceBatchApprovalService"));
    }

    @Bean
    public InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceStub invoicingInvoiceBatchCalculationServiceAsync(ChannelRegistry channels) {
        return InvoiceBatchCalculationServiceGrpc.newStub(channels.lookup("acme.invoicing.v1.InvoiceBatchCalculationService"));
    }

    @Bean
    public InvoiceBatchCalculationServiceGrpc.InvoiceBatchCalculationServiceBlockingStub invoicingInvoiceBatchCalculationServiceBlocking(ChannelRegistry channels) {
        return InvoiceBatchCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceBatchCalculationService"));
    }

    @Bean
    public InvoiceBatchQueryServiceGrpc.InvoiceBatchQueryServiceBlockingStub invoicingInvoiceBatchQueryServiceBlocking(ChannelRegistry channels) {
        return InvoiceBatchQueryServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceBatchQueryService"));
    }

    @Bean
    public InvoiceBatchServiceGrpc.InvoiceBatchServiceStub invoicingInvoiceBatchServiceAsync(ChannelRegistry channels) {
        return InvoiceBatchServiceGrpc.newStub(channels.lookup("acme.invoicing.v1.InvoiceBatchService"));
    }

    @Bean
    public InvoiceBatchServiceGrpc.InvoiceBatchServiceBlockingStub invoicingInvoiceBatchServiceBlocking(ChannelRegistry channels) {
        return InvoiceBatchServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceBatchService"));
    }

    @Bean
    public InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceStub invoicingInvoiceBatchValidationServiceAsync(ChannelRegistry channels) {
        return InvoiceBatchValidationServiceGrpc.newStub(channels.lookup("acme.invoicing.v1.InvoiceBatchValidationService"));
    }

    @Bean
    public InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceBlockingStub invoicingInvoiceBatchValidationServiceBlocking(ChannelRegistry channels) {
        return InvoiceBatchValidationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceBatchValidationService"));
    }

    @Bean
    public InvoiceBatchValidationServiceGrpc.InvoiceBatchValidationServiceFutureStub invoicingInvoiceBatchValidationServiceFuture(ChannelRegistry channels) {
        return InvoiceBatchValidationServiceGrpc.newFutureStub(channels.lookup("acme.invoicing.v1.InvoiceBatchValidationService"));
    }

    @Bean
    public InvoiceTemplateApprovalServiceGrpc.InvoiceTemplateApprovalServiceBlockingStub invoicingInvoiceTemplateApprovalServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateApprovalService"));
    }

    @Bean
    public InvoiceTemplateCalculationServiceGrpc.InvoiceTemplateCalculationServiceBlockingStub invoicingInvoiceTemplateCalculationServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateCalculationService"));
    }

    @Bean
    public InvoiceTemplateLifecycleServiceGrpc.InvoiceTemplateLifecycleServiceBlockingStub invoicingInvoiceTemplateLifecycleServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateLifecycleService"));
    }

    @Bean
    public InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceStub invoicingInvoiceTemplateQueryServiceAsync(ChannelRegistry channels) {
        return InvoiceTemplateQueryServiceGrpc.newStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateQueryService"));
    }

    @Bean
    public InvoiceTemplateQueryServiceGrpc.InvoiceTemplateQueryServiceBlockingStub invoicingInvoiceTemplateQueryServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateQueryServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateQueryService"));
    }

    @Bean
    public InvoiceTemplateValidationServiceGrpc.InvoiceTemplateValidationServiceBlockingStub invoicingInvoiceTemplateValidationServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateValidationServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateValidationService"));
    }

    @Bean
    public InvoiceTemplateWorkflowServiceGrpc.InvoiceTemplateWorkflowServiceBlockingStub invoicingInvoiceTemplateWorkflowServiceBlocking(ChannelRegistry channels) {
        return InvoiceTemplateWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.invoicing.v1.InvoiceTemplateWorkflowService"));
    }
}
