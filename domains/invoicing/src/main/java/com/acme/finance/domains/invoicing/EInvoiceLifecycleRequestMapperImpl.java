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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class EInvoiceLifecycleRequestMapperImpl implements EInvoiceLifecycleRequestMapper {
    @Override
    public FindCustomersRequest scheduleEInvoiceExpiryStage0(ScheduleEInvoiceExpiryRequest request) {
        return FindCustomersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(FindCustomersRequest.Filter.newBuilder()
                        .setCreatedAfter(request.getEInvoice().getAudit().getUpdatedAt())
                        .setRegion(request.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
