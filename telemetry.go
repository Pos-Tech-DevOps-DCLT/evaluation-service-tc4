package main

// Instrumentação OpenTelemetry padronizada do ToggleMaster (Fase 4).
//
// Arquivo IDÊNTICO no auth-service e no evaluation-service — se alterar
// aqui, replique no outro.
//
// Envia traces, métricas e logs via OTLP/HTTP para o OTel Collector do
// cluster, que roteia para APM (traces), Prometheus (métricas) e Loki (logs).
// Configuração 100% pelas variáveis de ambiente padrão do OTel (ConfigMap e
// Deployment do chart, em infra-tc4): OTEL_SERVICE_NAME,
// OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_RESOURCE_ATTRIBUTES...
//
// Sem OTEL_EXPORTER_OTLP_ENDPOINT (dev local, testes) nada é exportado e o
// serviço roda exatamente como antes.
//
// Versões fixadas em otel v1.28 / contrib v0.53: as últimas compatíveis com
// Go 1.21, a versão usada no template de CI.

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// setupTelemetry inicializa os providers globais de trace, métrica e log.
// A função retornada faz flush/encerramento e deve ser chamada no shutdown.
func setupTelemetry(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }

	// Propagação W3C (traceparent) sempre ativa: mesmo sem exporter, o
	// serviço repassa o contexto recebido para os serviços seguintes.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	if os.Getenv("OTEL_SDK_DISABLED") == "true" || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		log.Println("OpenTelemetry desabilitado (OTEL_EXPORTER_OTLP_ENDPOINT não definida)")
		return noop, nil
	}

	instanceID := os.Getenv("K8S_POD_NAME")
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}
	// WithFromEnv lê OTEL_SERVICE_NAME e OTEL_RESOURCE_ATTRIBUTES.
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attribute.String("service.instance.id", instanceID)),
	)
	if err != nil {
		return noop, err
	}

	// Exporters leem endpoint/protocolo das env vars OTEL_EXPORTER_OTLP_*.
	traceExp, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return noop, errors.Join(err, tp.Shutdown(ctx))
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	// Métricas do runtime Go (goroutines, GC, memória).
	if err := runtime.Start(runtime.WithMeterProvider(mp)); err != nil {
		log.Printf("Aviso: métricas de runtime indisponíveis: %v", err)
	}

	logExp, err := otlploghttp.New(ctx)
	if err != nil {
		return noop, errors.Join(err, tp.Shutdown(ctx), mp.Shutdown(ctx))
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	// Tudo que o serviço escreve com o pacote `log` continua indo para o
	// stdout E passa a ir também para o Collector (-> Loki).
	log.SetOutput(io.MultiWriter(os.Stderr, &otelLogWriter{logger: lp.Logger("std-log")}))

	log.Println("OpenTelemetry inicializado")
	return func(ctx context.Context) error {
		log.SetOutput(os.Stderr)
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx), lp.Shutdown(ctx))
	}, nil
}

// instrumentHandler envolve o mux com spans e métricas HTTP de servidor
// (http.server.duration -> http_server_duration_milliseconds no Prometheus).
func instrumentHandler(h http.Handler, service string) http.Handler {
	return otelhttp.NewHandler(h, service,
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
		// /health é chamado pelos probes do k8s a cada 10-15s: só gera ruído.
		otelhttp.WithFilter(func(r *http.Request) bool {
			return r.URL.Path != "/health"
		}),
	)
}

// otelLogWriter converte cada linha do pacote `log` num LogRecord OTel.
type otelLogWriter struct {
	logger otellog.Logger
}

func (w *otelLogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	severity, text := logSeverity(msg)

	var rec otellog.Record
	now := time.Now()
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(severity)
	rec.SetSeverityText(text)
	rec.SetBody(otellog.StringValue(msg))
	w.logger.Emit(context.Background(), rec)
	return len(p), nil
}

// logSeverity infere o nível pelo texto: os serviços usam log.Printf sem
// nível explícito, e as mensagens de erro seguem o padrão "Erro ..."/"Falha ...".
func logSeverity(msg string) (otellog.Severity, string) {
	lower := strings.ToLower(msg)
	switch {
	case strings.Contains(lower, "erro") || strings.Contains(lower, "falha") ||
		strings.Contains(lower, "não foi possível"):
		return otellog.SeverityError, "ERROR"
	case strings.Contains(lower, "aviso") || strings.Contains(lower, "atenção"):
		return otellog.SeverityWarn, "WARN"
	default:
		return otellog.SeverityInfo, "INFO"
	}
}
