// nolint
package cmd

import (
	"github.com/scalog/scalog/order"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// k8sorderCmd represents the k8s order command
var k8sorderCmd = &cobra.Command{
	Use:   "k8sorder",
	Short: "The k8s ordering layer",
	Long:  `The k8s ordering layer`,
	Run: func(cmd *cobra.Command, args []string) {
		order.StartK8s()
	},
}

func init() {
	RootCmd.AddCommand(k8sorderCmd)
	viper.BindEnv("name")
}
