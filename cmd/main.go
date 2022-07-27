// Copyright 2022 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	"github.com/kubewharf/kubebrain/cmd/option"
	"github.com/kubewharf/kubebrain/cmd/version"
)

func main() {
	command := NewKubeBrainCommand()

	logs.InitLogs()
	defer logs.FlushLogs()

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := command.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return
}

// NewKubeBrainCommand creates a *cobra.Command object with default parameters
func NewKubeBrainCommand() *cobra.Command {
	o := option.NewOptions()
	cmd := &cobra.Command{
		Use:  "kube-brain",
		Long: `KubeBrain is a new metadata storage backend for Kubernetes better than etcd`,
		RunE: func(cmd *cobra.Command, args []string) error {
			printFlags(cmd.Flags())
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(cmd.Context())
		},
	}
	// parse flags
	o.AddFlags(cmd.Flags())
	// global flags including klog
	globalflag.AddGlobalFlags(cmd.Flags(), cmd.Name())

	cmd.AddCommand(version.VersionCmd)
	return cmd
}

func printFlags(flags *pflag.FlagSet) {
	flags.VisitAll(func(flag *pflag.Flag) {
		klog.V(1).Infof("FLAG: --%s=%q", flag.Name, flag.Value)
	})
}
