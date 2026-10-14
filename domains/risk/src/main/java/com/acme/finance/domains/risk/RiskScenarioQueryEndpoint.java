package com.acme.finance.domains.risk;

import com.acme.finance.platform.RpcSupport;
import com.acme.risk.v1.BrowseRiskScenariosRequest;
import com.acme.risk.v1.BrowseRiskScenariosResponse;
import com.acme.risk.v1.CountRiskScenariosRequest;
import com.acme.risk.v1.CountRiskScenariosResponse;
import com.acme.risk.v1.LookupRiskScenarioRequest;
import com.acme.risk.v1.LookupRiskScenarioResponse;
import com.acme.risk.v1.PeekRiskScenarioRequest;
import com.acme.risk.v1.PeekRiskScenarioResponse;
import com.acme.risk.v1.ResolveRiskScenariosRequest;
import com.acme.risk.v1.ResolveRiskScenariosResponse;
import com.acme.risk.v1.RiskScenarioQueryServiceGrpc;
import com.acme.risk.v1.SummarizeRiskScenariosRequest;
import com.acme.risk.v1.SummarizeRiskScenariosResponse;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

/**
 * Answers read-only questions about risk scenarios, for screens and reports.
 */
@GrpcService
public class RiskScenarioQueryEndpoint extends RiskScenarioQueryServiceGrpc.RiskScenarioQueryServiceImplBase {
    protected final RiskScenarioQueryApplication application;

    public RiskScenarioQueryEndpoint(RiskScenarioQueryApplication application) {
        this.application = application;
    }

    /**
     * Returns a risk scenario.
     */
    @Override
    public void lookupRiskScenario(LookupRiskScenarioRequest request, StreamObserver<LookupRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.lookupRiskScenario(request));
    }

    /**
     * Lists risk scenarios a page at a time.
     */
    @Override
    public void browseRiskScenarios(BrowseRiskScenariosRequest request, StreamObserver<BrowseRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.browseRiskScenarios(request));
    }

    /**
     * Counts risk scenarios by status.
     */
    @Override
    public void countRiskScenarios(CountRiskScenariosRequest request, StreamObserver<CountRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.countRiskScenarios(request));
    }

    /**
     * Returns several risk scenarios at once.
     */
    @Override
    public void resolveRiskScenarios(ResolveRiskScenariosRequest request, StreamObserver<ResolveRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.resolveRiskScenarios(request));
    }

    /**
     * Summarizes a risk scenario.
     */
    @Override
    public void summarizeRiskScenarios(SummarizeRiskScenariosRequest request, StreamObserver<SummarizeRiskScenariosResponse> observer) {
        RpcSupport.unary(observer, () -> application.summarizeRiskScenarios(request));
    }

    /**
     * Returns a risk scenario.
     */
    @Override
    public void peekRiskScenario(PeekRiskScenarioRequest request, StreamObserver<PeekRiskScenarioResponse> observer) {
        RpcSupport.unary(observer, () -> application.peekRiskScenario(request));
    }
}
