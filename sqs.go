package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("evaluation-service")

// sqsAttributeCarrier adapta os MessageAttributes da SQS à interface de
// propagação do OTel: o traceparent viaja junto com a mensagem e o
// analytics-service continua o mesmo trace ao consumi-la.
type sqsAttributeCarrier map[string]*sqs.MessageAttributeValue

func (c sqsAttributeCarrier) Get(key string) string {
	if v, ok := c[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}

func (c sqsAttributeCarrier) Set(key, value string) {
	c[key] = &sqs.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(value)}
}

func (c sqsAttributeCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// Evento que será enviado para a fila
type EvaluationEvent struct {
	UserID    string    `json:"user_id"`
	FlagName  string    `json:"flag_name"`
	Result    bool      `json:"result"`
	Timestamp time.Time `json:"timestamp"`
}

// sendEvaluationEvent envia um evento para a fila SQS
func (a *App) sendEvaluationEvent(ctx context.Context, userID, flagName string, result bool) {
	// Se a URL da fila não foi configurada, apenas loga localmente e sai.
	if a.SqsSvc == nil || a.SqsQueueURL == "" {
		log.Printf("[SQS_DISABLED] Evento: User '%s', Flag '%s', Result '%t'", userID, flagName, result)
		return
	}

	event := EvaluationEvent{
		UserID:    userID,
		FlagName:  flagName,
		Result:    result,
		Timestamp: time.Now().UTC(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Erro ao serializar evento SQS: %v", err)
		return
	}

	// Span PRODUCER da publicação (o aws-sdk-go v1 não tem instrumentação
	// OTel oficial, por isso o span e a injeção de contexto são manuais).
	queueName := a.SqsQueueURL[strings.LastIndex(a.SqsQueueURL, "/")+1:]
	ctx, span := tracer.Start(ctx, queueName+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sqs"),
			attribute.String("messaging.operation", "publish"),
			attribute.String("messaging.destination.name", queueName),
			attribute.String("flag.name", flagName),
		),
	)
	defer span.End()

	attrs := sqsAttributeCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, attrs)

	// Envia a mensagem
	out, err := a.SqsSvc.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		MessageBody:       aws.String(string(body)),
		QueueUrl:          aws.String(a.SqsQueueURL),
		MessageAttributes: attrs,
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "falha ao publicar na SQS")
		log.Printf("Erro ao enviar mensagem para SQS: %v", err)
	} else {
		span.SetAttributes(attribute.String("messaging.message.id", aws.StringValue(out.MessageId)))
		log.Printf("Evento de avaliação enviado para SQS (Flag: %s)", flagName)
	}
}