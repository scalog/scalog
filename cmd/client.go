// nolint
package cmd

import (
	"github.com/scalog/scalog/client"

	"github.com/spf13/cobra"
)

// clientCmd represents the client command
var clientCmd = &cobra.Command{
	Use:   "client",
	Short: "The Scalog client",
	Long:  "The Scalog client",
	Run: func(cmd *cobra.Command, args []string) {
		client.Start()
	},
}

func init() {
	RootCmd.AddCommand(clientCmd)
}
