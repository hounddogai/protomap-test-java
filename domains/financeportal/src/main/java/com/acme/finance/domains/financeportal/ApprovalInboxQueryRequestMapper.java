package com.acme.finance.domains.financeportal;

import com.acme.audit.v1.AuditEventServiceGrpc;
import com.acme.audit.v1.ListAuditEventsRequest;
import com.acme.audit.v1.ListAuditEventsResponse;
import com.acme.common.v1.AuditInfo;
import com.acme.common.v1.OperationResult;
import com.acme.common.v1.PageInfo;
import com.acme.common.v1.PageRequest;
import com.acme.common.v1.PostalAddress;
import com.acme.customers.v1.CustomerSegment;
import com.acme.customers.v1.CustomerSegmentApprovalServiceGrpc;
import com.acme.customers.v1.CustomerSegmentBlockAction;
import com.acme.customers.v1.CustomerSegmentCalculationServiceGrpc;
import com.acme.customers.v1.SimulateCustomerSegmentRequest;
import com.acme.customers.v1.SimulateCustomerSegmentResponse;
import com.acme.customers.v1.SubmitCustomerSegmentRequest;
import com.acme.customers.v1.SubmitCustomerSegmentResponse;
import com.acme.finance.platform.ChannelRegistry;
import com.acme.finance.platform.JsonCodec;
import com.acme.finance.platform.RpcSupport;
import com.acme.financeportal.v1.ApprovalInbox;
import com.acme.financeportal.v1.ApprovalInboxLine;
import com.acme.financeportal.v1.ApprovalInboxLineTax;
import com.acme.financeportal.v1.BrowseApprovalInboxesRequest;
import com.acme.financeportal.v1.BrowseApprovalInboxesResponse;
import com.acme.financeportal.v1.CloseChecklist;
import com.acme.financeportal.v1.CloseChecklistLine;
import com.acme.financeportal.v1.CloseChecklistWorkflowServiceGrpc;
import com.acme.financeportal.v1.CountApprovalInboxesRequest;
import com.acme.financeportal.v1.CountApprovalInboxesResponse;
import com.acme.financeportal.v1.ExportApprovalInboxesRequest;
import com.acme.financeportal.v1.ExportApprovalInboxesResponse;
import com.acme.financeportal.v1.FinanceWorkspaceQueryServiceGrpc;
import com.acme.financeportal.v1.FindApprovalInboxesRequest;
import com.acme.financeportal.v1.FindApprovalInboxesResponse;
import com.acme.financeportal.v1.LookupApprovalInboxRequest;
import com.acme.financeportal.v1.LookupApprovalInboxResponse;
import com.acme.financeportal.v1.PeekApprovalInboxRequest;
import com.acme.financeportal.v1.PeekApprovalInboxResponse;
import com.acme.financeportal.v1.ResolveApprovalInboxesRequest;
import com.acme.financeportal.v1.ResolveApprovalInboxesResponse;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowRequest;
import com.acme.financeportal.v1.ResumeCloseChecklistWorkflowResponse;
import com.acme.financeportal.v1.SuggestApprovalInboxesRequest;
import com.acme.financeportal.v1.SuggestApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeApprovalInboxesRequest;
import com.acme.financeportal.v1.SummarizeApprovalInboxesResponse;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesRequest;
import com.acme.financeportal.v1.SummarizeFinanceWorkspacesResponse;
import com.acme.identity.v1.UserRef;
import com.acme.payroll.v1.GenerateTaxWithholdingReportRequest;
import com.acme.payroll.v1.GenerateTaxWithholdingReportResponse;
import com.acme.venues.v1.SummarizeVenuesRequest;
import com.acme.venues.v1.SummarizeVenuesResponse;
import com.acme.venues.v1.VenueQueryServiceGrpc;
import com.acme.webbff.v1.PageCompositionQueryServiceGrpc;
import com.acme.webbff.v1.ResolvePageCompositionsRequest;
import com.acme.webbff.v1.ResolvePageCompositionsResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

/**
 * Maps workflow inputs to downstream contracts. Implementations keep wire construction explicit.
 */
public interface ApprovalInboxQueryRequestMapper {
    ResumeCloseChecklistWorkflowRequest exportApprovalInboxesStage1(ExportApprovalInboxesRequest request, SubmitCustomerSegmentResponse reply0);
}
