// nolint
package cmd

import (
	"github.com/scalog/scalog/data"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// dataCmd represents the data command
var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "The data dissemination layer",
	Long:  `The data dissemination layer`,
	Run: func(cmd *cobra.Command, args []string) {
		data.Start()
	},
}

func init() {
	RootCmd.AddCommand(dataCmd)
	dataCmd.PersistentFlags().IntP("sid", "s", 0, "Shard index")
	dataCmd.PersistentFlags().IntP("rid", "r", 0, "Replica index in the shard")
	viper.BindPFlag("sid", dataCmd.PersistentFlags().Lookup("sid"))
	viper.BindPFlag("rid", dataCmd.PersistentFlags().Lookup("rid"))
}
