// nolint
package cmd

import (
	"github.com/scalog/scalog/discovery"

	"github.com/spf13/cobra"
)

// k8sdiscoveryCmd represents the k8sdiscovery command
var k8sdiscoveryCmd = &cobra.Command{
	Use:   "k8sdiscovery",
	Short: "The k8s discovery service",
	Long:  `The k8s discovery service`,
	Run: func(cmd *cobra.Command, args []string) {
		discovery.StartK8s()
	},
}

func init() {
	RootCmd.AddCommand(k8sdiscoveryCmd)
}
