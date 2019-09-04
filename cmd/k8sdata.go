// nolint
package cmd

import (
	"github.com/scalog/scalog/data"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// k8sdataCmd represents the k8sdata command
var k8sdataCmd = &cobra.Command{
	Use:   "k8sdata",
	Short: "The k8s data dissemination layer",
	Long:  `The k8s data dissemination layer`,
	Run: func(cmd *cobra.Command, args []string) {
		data.StartK8s()
	},
}

func init() {
	RootCmd.AddCommand(k8sdataCmd)
	viper.BindEnv("name")
}
