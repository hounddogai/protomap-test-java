package com.acme.finance.domains.invoicing;

import com.acme.audit.v1.AuditTrail;
import com.acme.audit.v1.AuditTrailExportServiceGrpc;
import com.acme.audit.v1.RetryAuditTrailExportRequest;
import com.acme.audit.v1.RetryAuditTrailExportResponse;
import com.acme.budgeting.v1.BudgetLine;
import com.acme.budgeting.v1.BudgetLineValidationServiceGrpc;
import com.acme.budgeting.v1.NormalizeBudgetLineRequest;
import com.acme.budgeting.v1.NormalizeBudgetLineResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.ContactInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerRef;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.DispatchCommand;
import com.acme.finance.platform.RpcSupport;
import com.acme.identity.v1.UserRef;
import com.acme.insurance.v1.Claim;
import com.acme.insurance.v1.QuoteClaimRequest;
import com.acme.insurance.v1.QuoteClaimResponse;
import com.acme.invoicing.v1.BrowseInvoiceBatchesRequest;
import com.acme.invoicing.v1.BrowseInvoiceBatchesResponse;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityRequest;
import com.acme.invoicing.v1.CheckInvoiceTemplateEligibilityResponse;
import com.acme.invoicing.v1.CountInvoiceBatchesRequest;
import com.acme.invoicing.v1.CountInvoiceBatchesResponse;
import com.acme.invoicing.v1.EInvoice;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionRequest;
import com.acme.invoicing.v1.ExplainInvoiceTemplateDecisionResponse;
import com.acme.invoicing.v1.ExportInvoiceBatchesRequest;
import com.acme.invoicing.v1.ExportInvoiceBatchesResponse;
import com.acme.invoicing.v1.FindInvoiceBatchesRequest;
import com.acme.invoicing.v1.FindInvoiceBatchesResponse;
import com.acme.invoicing.v1.InvoiceBatch;
import com.acme.invoicing.v1.InvoiceBatchValidationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplate;
import com.acme.invoicing.v1.InvoiceTemplateCalculationServiceGrpc;
import com.acme.invoicing.v1.InvoiceTemplateLine;
import com.acme.invoicing.v1.InvoiceTemplateValidationServiceGrpc;
import com.acme.invoicing.v1.LookupInvoiceBatchRequest;
import com.acme.invoicing.v1.LookupInvoiceBatchResponse;
import com.acme.invoicing.v1.NormalizeInvoiceBatchRequest;
import com.acme.invoicing.v1.NormalizeInvoiceBatchResponse;
import com.acme.invoicing.v1.PeekInvoiceBatchRequest;
import com.acme.invoicing.v1.PeekInvoiceBatchResponse;
import com.acme.invoicing.v1.ResolveInvoiceBatchesRequest;
import com.acme.invoicing.v1.ResolveInvoiceBatchesResponse;
import com.acme.invoicing.v1.SimulateInvoiceTemplateRequest;
import com.acme.invoicing.v1.SimulateInvoiceTemplateResponse;
import com.acme.invoicing.v1.SuggestInvoiceBatchesRequest;
import com.acme.invoicing.v1.SuggestInvoiceBatchesResponse;
import com.acme.invoicing.v1.ValidateEInvoiceRequest;
import com.acme.invoicing.v1.ValidateEInvoiceResponse;
import com.acme.risk.v1.RiskRegisterQueryServiceGrpc;
import com.acme.risk.v1.SuggestRiskRegistersRequest;
import com.acme.risk.v1.SuggestRiskRegistersResponse;
import com.acme.search.v1.SearchIndex;
import com.acme.search.v1.SearchIndexValidationServiceGrpc;
import com.acme.search.v1.ValidateSearchIndexRequest;
import com.acme.search.v1.ValidateSearchIndexResponse;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public final class InvoiceBatchQueryRequestMapperImpl implements InvoiceBatchQueryRequestMapper {
    @Override
    public NormalizeBudgetLineRequest browseInvoiceBatchesStage0(BrowseInvoiceBatchesRequest request) {
        return NormalizeBudgetLineRequest.newBuilder()
                .setBudgetLine(BudgetLine.newBuilder()
                        .setBillingAddress(PostalAddress.newBuilder()
                                .setRegion(request.getFilter().getRegion())
                                .build())
                        .build())
                .setContext(request.getContext())
                .setDetails(NormalizeBudgetLineRequest.Details.newBuilder()
                        .setContact(ContactInfo.newBuilder()
                                .setEmail(request.getContext().getActor().getEmail())
                                .build())
                        .setLocale(request.getContext().getLocale())
                        .build())
                .build();
    }

    @Override
    public SuggestRiskRegistersRequest exportInvoiceBatchesStage1(ExportInvoiceBatchesRequest request, ValidateEInvoiceResponse reply0) {
        return SuggestRiskRegistersRequest.newBuilder()
                .setContext(request.getContext())
                .setFilter(SuggestRiskRegistersRequest.Filter.newBuilder()
                        .setOwnerId(request.getFilter().getOwnerId())
                        .setRegion(reply0.getEInvoice().getBillingAddress().getRegion())
                        .build())
                .build();
    }
}
