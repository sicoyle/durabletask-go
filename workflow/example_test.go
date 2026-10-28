package workflow_test

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/workflow"
)

// GreetingWorkflow calls the SayHello activity once per city, in sequence,
// and returns the greetings.
func GreetingWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var greetings []string
	for _, city := range []string{"Tokyo", "London", "Seattle"} {
		var greeting string
		if err := ctx.CallActivity(SayHello, workflow.WithActivityInput(city)).Await(&greeting); err != nil {
			return nil, err
		}
		greetings = append(greetings, greeting)
	}
	return greetings, nil
}

// SayHello is an activity that returns a greeting for its input.
func SayHello(ctx workflow.ActivityContext) (any, error) {
	var name string
	if err := ctx.GetInput(&name); err != nil {
		return nil, err
	}
	return fmt.Sprintf("Hello, %s!", name), nil
}

// This example registers a workflow and an activity, connects to a Durable
// Task gRPC endpoint, runs the workflow and waits for it to complete. In a
// Dapr application the endpoint is the Dapr sidecar; here an in-memory engine
// is started in the same process.
func Example() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, stop := startEngine(ctx)
	defer stop()

	r := workflow.NewRegistry()
	if err := r.AddWorkflow(GreetingWorkflow); err != nil {
		log.Fatal(err)
	}
	if err := r.AddActivity(SayHello); err != nil {
		log.Fatal(err)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := workflow.NewClient(conn)
	if err := client.StartWorker(ctx, r); err != nil {
		log.Fatal(err)
	}

	id, err := client.ScheduleWorkflow(ctx, "GreetingWorkflow")
	if err != nil {
		log.Fatal(err)
	}
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(metadata.Output.GetValue())
	// Output: ["Hello, Tokyo!","Hello, London!","Hello, Seattle!"]
}

// startEngine runs the Durable Task engine with an in-memory sqlite backend
// behind a gRPC server on a free local port, and returns its address.
func startEngine(ctx context.Context) (string, func()) {
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)

	server := grpc.NewServer()
	executor, register := backend.NewGrpcExecutor(be, logger)
	register(server)

	worker := backend.NewTaskHubWorker(
		be,
		backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{Backend: be, Executor: executor, Logger: logger}),
		backend.NewActivityTaskWorker(be, executor, logger),
		logger,
	)
	if err := worker.Start(ctx); err != nil {
		log.Fatal(err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve(lis)

	return lis.Addr().String(), func() {
		server.Stop()
		worker.Shutdown(context.Background())
	}
}
