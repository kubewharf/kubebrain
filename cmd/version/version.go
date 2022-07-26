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

package version

import (
	"fmt"

	"github.com/spf13/cobra"
)

// values will be injected while building
var (
	// Version is version of binary
	Version string

	// Storage is the storage engine used by this binary
	Storage string

	// GitSHA is the SHA of commit
	GitSHA string

	// GoVersion is the version of go compiler
	GoVersion string

	// GoOsArch is the os and arch of the binary
	GoOsArch string

	// Date is the time when binary is build
	Date string
)

// VersionCmd is the cobra.Command for print version info
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "show version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("KubeBrain")
		fmt.Println("Version:   \t", Version)
		fmt.Println("Storage:   \t", Storage)
		fmt.Println("Git SHA:   \t", GitSHA)
		fmt.Println("Go Version:\t", GoVersion)
		fmt.Println("Go OS/Arch:\t", GoOsArch)
		fmt.Println("BuildTime: \t", Date)
	},
}
