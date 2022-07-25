package cli

import (
	"context"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:  "kafka-offset",
	Long: `kafka-offset tool can be used to manipulate kafka consumer group offsets`,
}

func Execute(ctx context.Context) {
	rootCmd.AddCommand(
		cmdCopyConsumer(ctx),
		cmdResetConsumer(ctx),
	)

	_ = rootCmd.Execute()
}
