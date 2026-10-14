package com.acme.finance.platform.clients;

import com.acme.customers.v1.ConsentRecordApprovalServiceGrpc;
import com.acme.customers.v1.ConsentRecordCalculationServiceGrpc;
import com.acme.customers.v1.ConsentRecordSyncServiceGrpc;
import com.acme.customers.v1.ConsentRecordValidationServiceGrpc;
import com.acme.customers.v1.ConsentRecordWorkflowServiceGrpc;
import com.acme.customers.v1.CustomerApprovalServiceGrpc;
import com.acme.customers.v1.CustomerCalculationServiceGrpc;
import com.acme.customers.v1.CustomerLifecycleServiceGrpc;
import com.acme.customers.v1.CustomerQueryServiceGrpc;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.CustomerSegmentSyncServiceGrpc;
import com.acme.customers.v1.CustomerSegmentValidationServiceGrpc;
import com.acme.customers.v1.CustomerServiceGrpc;
import com.acme.customers.v1.CustomerValidationServiceGrpc;
import com.acme.customers.v1.CustomerWorkflowServiceGrpc;
import com.acme.customers.v1.HouseholdApprovalServiceGrpc;
import com.acme.customers.v1.HouseholdCalculationServiceGrpc;
import com.acme.customers.v1.HouseholdQueryServiceGrpc;
import com.acme.customers.v1.HouseholdReportingServiceGrpc;
import com.acme.customers.v1.HouseholdValidationServiceGrpc;
import com.acme.customers.v1.HouseholdWorkflowServiceGrpc;
import com.acme.customers.v1.PreferenceCalculationServiceGrpc;
import com.acme.customers.v1.PreferenceImportServiceGrpc;
import com.acme.customers.v1.PreferenceReportingServiceGrpc;
import com.acme.customers.v1.PreferenceSyncServiceGrpc;
import com.acme.customers.v1.PreferenceValidationServiceGrpc;
import com.acme.finance.platform.ChannelRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constructor-injected client stubs for the customers contracts. Deadlines are set per call.
 */
@Configuration(proxyBeanMethods = false)
public class CustomersClients {
    @Bean
    public ConsentRecordApprovalServiceGrpc.ConsentRecordApprovalServiceBlockingStub customersConsentRecordApprovalServiceBlocking(ChannelRegistry channels) {
        return ConsentRecordApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.ConsentRecordApprovalService"));
    }

    @Bean
    public ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceStub customersConsentRecordCalculationServiceAsync(ChannelRegistry channels) {
        return ConsentRecordCalculationServiceGrpc.newStub(channels.lookup("acme.customers.v1.ConsentRecordCalculationService"));
    }

    @Bean
    public ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceBlockingStub customersConsentRecordCalculationServiceBlocking(ChannelRegistry channels) {
        return ConsentRecordCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.ConsentRecordCalculationService"));
    }

    @Bean
    public ConsentRecordCalculationServiceGrpc.ConsentRecordCalculationServiceFutureStub customersConsentRecordCalculationServiceFuture(ChannelRegistry channels) {
        return ConsentRecordCalculationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.ConsentRecordCalculationService"));
    }

    @Bean
    public ConsentRecordSyncServiceGrpc.ConsentRecordSyncServiceBlockingStub customersConsentRecordSyncServiceBlocking(ChannelRegistry channels) {
        return ConsentRecordSyncServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.ConsentRecordSyncService"));
    }

    @Bean
    public ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceBlockingStub customersConsentRecordValidationServiceBlocking(ChannelRegistry channels) {
        return ConsentRecordValidationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.ConsentRecordValidationService"));
    }

    @Bean
    public ConsentRecordValidationServiceGrpc.ConsentRecordValidationServiceFutureStub customersConsentRecordValidationServiceFuture(ChannelRegistry channels) {
        return ConsentRecordValidationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.ConsentRecordValidationService"));
    }

    @Bean
    public ConsentRecordWorkflowServiceGrpc.ConsentRecordWorkflowServiceBlockingStub customersConsentRecordWorkflowServiceBlocking(ChannelRegistry channels) {
        return ConsentRecordWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.ConsentRecordWorkflowService"));
    }

    @Bean
    public CustomerApprovalServiceGrpc.CustomerApprovalServiceBlockingStub customersCustomerApprovalServiceBlocking(ChannelRegistry channels) {
        return CustomerApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerApprovalService"));
    }

    @Bean
    public CustomerCalculationServiceGrpc.CustomerCalculationServiceBlockingStub customersCustomerCalculationServiceBlocking(ChannelRegistry channels) {
        return CustomerCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerCalculationService"));
    }

    @Bean
    public CustomerCalculationServiceGrpc.CustomerCalculationServiceFutureStub customersCustomerCalculationServiceFuture(ChannelRegistry channels) {
        return CustomerCalculationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerCalculationService"));
    }

    @Bean
    public CustomerLifecycleServiceGrpc.CustomerLifecycleServiceBlockingStub customersCustomerLifecycleServiceBlocking(ChannelRegistry channels) {
        return CustomerLifecycleServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerLifecycleService"));
    }

    @Bean
    public CustomerQueryServiceGrpc.CustomerQueryServiceStub customersCustomerQueryServiceAsync(ChannelRegistry channels) {
        return CustomerQueryServiceGrpc.newStub(channels.lookup("acme.customers.v1.CustomerQueryService"));
    }

    @Bean
    public CustomerQueryServiceGrpc.CustomerQueryServiceBlockingStub customersCustomerQueryServiceBlocking(ChannelRegistry channels) {
        return CustomerQueryServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerQueryService"));
    }

    @Bean
    public CustomerQueryServiceGrpc.CustomerQueryServiceFutureStub customersCustomerQueryServiceFuture(ChannelRegistry channels) {
        return CustomerQueryServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerQueryService"));
    }

    @Bean
    public CustomerSegmentApprovalServiceGrpc.CustomerSegmentApprovalServiceBlockingStub customersCustomerSegmentApprovalServiceBlocking(ChannelRegistry channels) {
        return CustomerSegmentApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerSegmentApprovalService"));
    }

    @Bean
    public CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceBlockingStub customersCustomerSegmentCalculationServiceBlocking(ChannelRegistry channels) {
        return CustomerSegmentCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerSegmentCalculationService"));
    }

    @Bean
    public CustomerSegmentCalculationServiceGrpc.CustomerSegmentCalculationServiceFutureStub customersCustomerSegmentCalculationServiceFuture(ChannelRegistry channels) {
        return CustomerSegmentCalculationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerSegmentCalculationService"));
    }

    @Bean
    public CustomerSegmentSyncServiceGrpc.CustomerSegmentSyncServiceBlockingStub customersCustomerSegmentSyncServiceBlocking(ChannelRegistry channels) {
        return CustomerSegmentSyncServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerSegmentSyncService"));
    }

    @Bean
    public CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceStub customersCustomerSegmentValidationServiceAsync(ChannelRegistry channels) {
        return CustomerSegmentValidationServiceGrpc.newStub(channels.lookup("acme.customers.v1.CustomerSegmentValidationService"));
    }

    @Bean
    public CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceBlockingStub customersCustomerSegmentValidationServiceBlocking(ChannelRegistry channels) {
        return CustomerSegmentValidationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerSegmentValidationService"));
    }

    @Bean
    public CustomerSegmentValidationServiceGrpc.CustomerSegmentValidationServiceFutureStub customersCustomerSegmentValidationServiceFuture(ChannelRegistry channels) {
        return CustomerSegmentValidationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerSegmentValidationService"));
    }

    @Bean
    public CustomerServiceGrpc.CustomerServiceStub customersCustomerServiceAsync(ChannelRegistry channels) {
        return CustomerServiceGrpc.newStub(channels.lookup("acme.customers.v1.CustomerService"));
    }

    @Bean
    public CustomerServiceGrpc.CustomerServiceBlockingStub customersCustomerServiceBlocking(ChannelRegistry channels) {
        return CustomerServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerService"));
    }

    @Bean
    public CustomerServiceGrpc.CustomerServiceFutureStub customersCustomerServiceFuture(ChannelRegistry channels) {
        return CustomerServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerService"));
    }

    @Bean
    public CustomerValidationServiceGrpc.CustomerValidationServiceBlockingStub customersCustomerValidationServiceBlocking(ChannelRegistry channels) {
        return CustomerValidationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerValidationService"));
    }

    @Bean
    public CustomerValidationServiceGrpc.CustomerValidationServiceFutureStub customersCustomerValidationServiceFuture(ChannelRegistry channels) {
        return CustomerValidationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.CustomerValidationService"));
    }

    @Bean
    public CustomerWorkflowServiceGrpc.CustomerWorkflowServiceBlockingStub customersCustomerWorkflowServiceBlocking(ChannelRegistry channels) {
        return CustomerWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.CustomerWorkflowService"));
    }

    @Bean
    public HouseholdApprovalServiceGrpc.HouseholdApprovalServiceBlockingStub customersHouseholdApprovalServiceBlocking(ChannelRegistry channels) {
        return HouseholdApprovalServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdApprovalService"));
    }

    @Bean
    public HouseholdCalculationServiceGrpc.HouseholdCalculationServiceStub customersHouseholdCalculationServiceAsync(ChannelRegistry channels) {
        return HouseholdCalculationServiceGrpc.newStub(channels.lookup("acme.customers.v1.HouseholdCalculationService"));
    }

    @Bean
    public HouseholdCalculationServiceGrpc.HouseholdCalculationServiceBlockingStub customersHouseholdCalculationServiceBlocking(ChannelRegistry channels) {
        return HouseholdCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdCalculationService"));
    }

    @Bean
    public HouseholdQueryServiceGrpc.HouseholdQueryServiceStub customersHouseholdQueryServiceAsync(ChannelRegistry channels) {
        return HouseholdQueryServiceGrpc.newStub(channels.lookup("acme.customers.v1.HouseholdQueryService"));
    }

    @Bean
    public HouseholdQueryServiceGrpc.HouseholdQueryServiceBlockingStub customersHouseholdQueryServiceBlocking(ChannelRegistry channels) {
        return HouseholdQueryServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdQueryService"));
    }

    @Bean
    public HouseholdQueryServiceGrpc.HouseholdQueryServiceFutureStub customersHouseholdQueryServiceFuture(ChannelRegistry channels) {
        return HouseholdQueryServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.HouseholdQueryService"));
    }

    @Bean
    public HouseholdReportingServiceGrpc.HouseholdReportingServiceBlockingStub customersHouseholdReportingServiceBlocking(ChannelRegistry channels) {
        return HouseholdReportingServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdReportingService"));
    }

    @Bean
    public HouseholdValidationServiceGrpc.HouseholdValidationServiceBlockingStub customersHouseholdValidationServiceBlocking(ChannelRegistry channels) {
        return HouseholdValidationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdValidationService"));
    }

    @Bean
    public HouseholdValidationServiceGrpc.HouseholdValidationServiceFutureStub customersHouseholdValidationServiceFuture(ChannelRegistry channels) {
        return HouseholdValidationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.HouseholdValidationService"));
    }

    @Bean
    public HouseholdWorkflowServiceGrpc.HouseholdWorkflowServiceBlockingStub customersHouseholdWorkflowServiceBlocking(ChannelRegistry channels) {
        return HouseholdWorkflowServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.HouseholdWorkflowService"));
    }

    @Bean
    public PreferenceCalculationServiceGrpc.PreferenceCalculationServiceStub customersPreferenceCalculationServiceAsync(ChannelRegistry channels) {
        return PreferenceCalculationServiceGrpc.newStub(channels.lookup("acme.customers.v1.PreferenceCalculationService"));
    }

    @Bean
    public PreferenceCalculationServiceGrpc.PreferenceCalculationServiceBlockingStub customersPreferenceCalculationServiceBlocking(ChannelRegistry channels) {
        return PreferenceCalculationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.PreferenceCalculationService"));
    }

    @Bean
    public PreferenceImportServiceGrpc.PreferenceImportServiceBlockingStub customersPreferenceImportServiceBlocking(ChannelRegistry channels) {
        return PreferenceImportServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.PreferenceImportService"));
    }

    @Bean
    public PreferenceReportingServiceGrpc.PreferenceReportingServiceBlockingStub customersPreferenceReportingServiceBlocking(ChannelRegistry channels) {
        return PreferenceReportingServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.PreferenceReportingService"));
    }

    @Bean
    public PreferenceSyncServiceGrpc.PreferenceSyncServiceBlockingStub customersPreferenceSyncServiceBlocking(ChannelRegistry channels) {
        return PreferenceSyncServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.PreferenceSyncService"));
    }

    @Bean
    public PreferenceValidationServiceGrpc.PreferenceValidationServiceBlockingStub customersPreferenceValidationServiceBlocking(ChannelRegistry channels) {
        return PreferenceValidationServiceGrpc.newBlockingStub(channels.lookup("acme.customers.v1.PreferenceValidationService"));
    }

    @Bean
    public PreferenceValidationServiceGrpc.PreferenceValidationServiceFutureStub customersPreferenceValidationServiceFuture(ChannelRegistry channels) {
        return PreferenceValidationServiceGrpc.newFutureStub(channels.lookup("acme.customers.v1.PreferenceValidationService"));
    }
}
