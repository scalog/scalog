// nolint
package cmd

import (
	"github.com/scalog/scalog/client"

	"github.com/spf13/cobra"
)

// k8sclientCmd represents the k8sclient command
var k8sclientCmd = &cobra.Command{
	Use:   "k8sclient",
	Short: "The Scalog k8s client",
	Long:  "The Scalog k8s client",
	Run: func(cmd *cobra.Command, args []string) {
		client.StartK8s()
	},
}

func init() {
	RootCmd.AddCommand(k8sclientCmd)
}
