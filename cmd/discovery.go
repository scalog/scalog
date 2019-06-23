// nolint
package cmd

import (
	"github.com/scalog/scalog/discovery"

	"github.com/spf13/cobra"
)

// discoveryCmd represents the discovery command
var discoveryCmd = &cobra.Command{
	Use:   "discovery",
	Short: "The discovery service",
	Long:  `The discovery service`,
	Run: func(cmd *cobra.Command, args []string) {
		discovery.Start()
	},
}

func init() {
	RootCmd.AddCommand(discoveryCmd)
}
