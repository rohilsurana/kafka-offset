package cli

import (
	"context"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use: "kafka-offset",
	Long: `Entropy is a framework to safely and predictably create, change, 
and improve modern cloud applications and infrastructure using 
familiar languages, tools, and engineering practices.`,
}

func Execute(ctx context.Context) {
	rootCmd.AddCommand(
		cmdCopyConsumer(ctx),
		cmdResetConsumer(ctx),
	)

	_ = rootCmd.Execute()
}
